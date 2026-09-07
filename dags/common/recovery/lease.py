"""Fenced, bounded recovery lease contract.

The planner intentionally has no side effects.  A future executor needs one
additional boundary before it can trigger a DAG: exactly one coordinator must
own a recovery job, and a stale coordinator must not be able to publish after
its lease has expired.  This module defines that boundary against a small
compare-and-swap backend protocol.  It ships with an in-memory backend for
contract tests only; the production adapter must provide the same atomic
operations through the Airflow/Postgres control store.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from threading import RLock
from typing import Protocol

from common.recovery.planner import RecoveryAction, RecoveryJob


SCHEMA_VERSION = "weather-recovery-lease/v1"
ACTIVE_STATES = frozenset({"leased", "running"})
TERMINAL_STATES = frozenset({"succeeded", "skipped", "unrecoverable", "blocked"})
LEASE_STATES = ACTIVE_STATES | TERMINAL_STATES
_SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+-]{0,255}$")
_ALLOWED_ACTIONS = frozenset({RecoveryAction.RAW_REPLAY, RecoveryAction.RECOLLECT})


class LeaseDecision(StrEnum):
    """Outcome of an attempted job claim."""

    ACQUIRED = "acquired"
    TAKEOVER = "takeover"
    ALREADY_OWNED = "already_owned"
    HELD = "held"
    TERMINAL = "terminal"
    CONFLICT = "conflict"


class RecoveryLeaseError(ValueError):
    """Lease evidence or a state transition is invalid."""


class LeaseLostError(RecoveryLeaseError):
    """A worker attempted to update a lease it no longer owns."""


class LeaseBackend(Protocol):
    """Atomic storage required by :class:`RecoveryLeaseRegistry`.

    ``replace_if_expired`` and ``replace_if_owner`` must be one transaction at
    the backend.  A read followed by an unconditional write is not sufficient
    because two coordinators can otherwise both pass the same observation.
    """

    def read(self, job_key: str) -> Mapping[str, object] | None: ...

    def create_if_absent(self, job_key: str, document: Mapping[str, object]) -> bool: ...

    def replace_if_expired(
        self,
        job_key: str,
        expected_lease_id: str,
        now: datetime,
        document: Mapping[str, object],
    ) -> bool: ...

    def replace_if_owner(
        self,
        job_key: str,
        expected_lease_id: str,
        owner_id: str,
        now: datetime,
        document: Mapping[str, object],
    ) -> bool: ...


@dataclass(frozen=True)
class RecoveryLease:
    """A fenced lease record for one deterministic recovery job."""

    job_key: str
    lease_id: str
    owner_id: str
    plan_id: str
    action: RecoveryAction
    state: str
    acquired_at: datetime
    expires_at: datetime
    updated_at: datetime
    attempt_count: int
    evidence: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _safe_text(self.job_key, field="job_key")
        _safe_text(self.lease_id, field="lease_id")
        _safe_text(self.owner_id, field="owner_id")
        _safe_text(self.plan_id, field="plan_id")
        if self.action not in _ALLOWED_ACTIONS:
            raise RecoveryLeaseError("lease action is not claimable")
        if self.state not in LEASE_STATES:
            raise RecoveryLeaseError(f"unsupported lease state: {self.state}")
        for value, field_name in (
            (self.acquired_at, "acquired_at"),
            (self.expires_at, "expires_at"),
            (self.updated_at, "updated_at"),
        ):
            _aware_utc(value, field=field_name)
        if self.expires_at < self.acquired_at:
            raise RecoveryLeaseError("expires_at must not precede acquired_at")
        if isinstance(self.attempt_count, bool) or not isinstance(self.attempt_count, int):
            raise RecoveryLeaseError("attempt_count must be a non-negative integer")
        if self.attempt_count < 1:
            raise RecoveryLeaseError("attempt_count must be positive")
        if not isinstance(self.evidence, Mapping):
            raise RecoveryLeaseError("evidence must be an object")
        _safe_evidence(self.evidence)

    def is_active_at(self, now: datetime) -> bool:
        return self.state in ACTIVE_STATES and self.expires_at > _aware_utc(now, field="now")

    def to_dict(self) -> dict[str, object]:
        """Return a redacted, JSON-safe control document."""
        return {
            "schema_version": SCHEMA_VERSION,
            "job_key": self.job_key,
            "lease_id": self.lease_id,
            "owner_id": self.owner_id,
            "plan_id": self.plan_id,
            "action": self.action.value,
            "state": self.state,
            "acquired_at": _aware_utc(self.acquired_at, field="acquired_at").isoformat(),
            "expires_at": _aware_utc(self.expires_at, field="expires_at").isoformat(),
            "updated_at": _aware_utc(self.updated_at, field="updated_at").isoformat(),
            "attempt_count": self.attempt_count,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True)
class LeaseResult:
    decision: LeaseDecision
    lease: RecoveryLease

    def to_dict(self) -> dict[str, object]:
        return {
            "decision": self.decision.value,
            "lease": self.lease.to_dict(),
        }


class RecoveryLeaseRegistry:
    """Claim and fence recovery jobs using an atomic backend."""

    def __init__(self, backend: LeaseBackend, *, lease_ttl: timedelta = timedelta(minutes=15)) -> None:
        if not isinstance(lease_ttl, timedelta) or lease_ttl <= timedelta(0):
            raise RecoveryLeaseError("lease_ttl must be positive")
        self._backend = backend
        self._lease_ttl = lease_ttl

    def claim(
        self,
        job: RecoveryJob,
        *,
        plan_id: str,
        owner_id: str,
        now: datetime,
    ) -> LeaseResult:
        """Claim a planned action or return the existing fenced decision.

        Only replay/recollect actions are claimable.  Blocked/deferred jobs are
        planner output, not execution input, so they never create a lease.
        """
        if job.action not in _ALLOWED_ACTIONS:
            raise RecoveryLeaseError("only raw_replay and recollect jobs are claimable")
        _safe_text(plan_id, field="plan_id")
        _safe_text(owner_id, field="owner_id")
        normalized_now = _aware_utc(now, field="now")
        current = self._read(job.job_key)
        if current is None:
            proposed = self._new_lease(job, plan_id, owner_id, normalized_now, attempt_count=1)
            if self._backend.create_if_absent(job.job_key, proposed.to_dict()):
                return LeaseResult(LeaseDecision.ACQUIRED, proposed)
            # A competing coordinator won the create race.  Re-read and apply
            # the same state machine; never blindly overwrite its lease.
            current = self._read(job.job_key)
            if current is None:
                raise RecoveryLeaseError("lease create race produced no readable record")

        if current.job_key != job.job_key or current.action is not job.action:
            raise RecoveryLeaseError("lease job identity mismatch")
        if current.state in TERMINAL_STATES:
            return LeaseResult(LeaseDecision.TERMINAL, current)
        if current.is_active_at(normalized_now):
            decision = (
                LeaseDecision.ALREADY_OWNED
                if current.owner_id == owner_id
                else LeaseDecision.HELD
            )
            return LeaseResult(decision, current)

        takeover = self._new_lease(
            job,
            plan_id,
            owner_id,
            normalized_now,
            attempt_count=current.attempt_count + 1,
        )
        if self._backend.replace_if_expired(
            job.job_key,
            current.lease_id,
            normalized_now,
            takeover.to_dict(),
        ):
            return LeaseResult(LeaseDecision.TAKEOVER, takeover)
        # A concurrent takeover won the compare-and-swap.  Return its current
        # status rather than retrying in a loop and amplifying contention.
        latest = self._read(job.job_key)
        if latest is None:
            raise RecoveryLeaseError("lease takeover race produced no readable record")
        decision = (
            LeaseDecision.ALREADY_OWNED
            if latest.owner_id == owner_id and latest.is_active_at(normalized_now)
            else LeaseDecision.HELD
            if latest.is_active_at(normalized_now)
            else LeaseDecision.TERMINAL
            if latest.state in TERMINAL_STATES
            else LeaseDecision.CONFLICT
        )
        return LeaseResult(decision, latest)

    def mark_running(
        self,
        lease: RecoveryLease,
        *,
        owner_id: str,
        now: datetime,
    ) -> RecoveryLease:
        return self._transition(lease, owner_id=owner_id, now=now, state="running")

    def mark_terminal(
        self,
        lease: RecoveryLease,
        *,
        owner_id: str,
        state: str,
        now: datetime,
        evidence: Mapping[str, object] | None = None,
    ) -> RecoveryLease:
        if state not in TERMINAL_STATES:
            raise RecoveryLeaseError("terminal transition requires a terminal state")
        return self._transition(
            lease,
            owner_id=owner_id,
            now=now,
            state=state,
            evidence=evidence,
        )

    def renew(
        self,
        lease: RecoveryLease,
        *,
        owner_id: str,
        now: datetime,
    ) -> RecoveryLease:
        normalized_now = _aware_utc(now, field="now")
        if lease.state not in ACTIVE_STATES:
            raise RecoveryLeaseError("only active leases can be renewed")
        if lease.owner_id != owner_id or not lease.is_active_at(normalized_now):
            raise LeaseLostError("lease is no longer owned by this worker")
        updated = RecoveryLease(
            job_key=lease.job_key,
            lease_id=lease.lease_id,
            owner_id=lease.owner_id,
            plan_id=lease.plan_id,
            action=lease.action,
            state=lease.state,
            acquired_at=lease.acquired_at,
            expires_at=normalized_now + self._lease_ttl,
            updated_at=normalized_now,
            attempt_count=lease.attempt_count,
            evidence=lease.evidence,
        )
        if not self._backend.replace_if_owner(
            lease.job_key,
            lease.lease_id,
            owner_id,
            normalized_now,
            updated.to_dict(),
        ):
            raise LeaseLostError("lease renewal compare-and-swap failed")
        return updated

    def _transition(
        self,
        lease: RecoveryLease,
        *,
        owner_id: str,
        now: datetime,
        state: str,
        evidence: Mapping[str, object] | None = None,
    ) -> RecoveryLease:
        normalized_now = _aware_utc(now, field="now")
        if lease.state == "leased" and state not in {"running", *TERMINAL_STATES}:
            raise RecoveryLeaseError("leased transition is invalid")
        if lease.state == "running" and state not in TERMINAL_STATES:
            raise RecoveryLeaseError("running transition is invalid")
        if lease.state not in ACTIVE_STATES:
            raise RecoveryLeaseError("terminal lease cannot transition")
        if lease.owner_id != owner_id or not lease.is_active_at(normalized_now):
            raise LeaseLostError("lease is no longer owned by this worker")
        updated = RecoveryLease(
            job_key=lease.job_key,
            lease_id=lease.lease_id,
            owner_id=lease.owner_id,
            plan_id=lease.plan_id,
            action=lease.action,
            state=state,
            acquired_at=lease.acquired_at,
            expires_at=lease.expires_at,
            updated_at=normalized_now,
            attempt_count=lease.attempt_count,
            evidence=dict(evidence or lease.evidence),
        )
        if not self._backend.replace_if_owner(
            lease.job_key,
            lease.lease_id,
            owner_id,
            normalized_now,
            updated.to_dict(),
        ):
            raise LeaseLostError("lease transition compare-and-swap failed")
        return updated

    def _read(self, job_key: str) -> RecoveryLease | None:
        document = self._backend.read(job_key)
        if document is None:
            return None
        return _lease_from_document(document)

    def _new_lease(
        self,
        job: RecoveryJob,
        plan_id: str,
        owner_id: str,
        now: datetime,
        *,
        attempt_count: int,
    ) -> RecoveryLease:
        lease_material = "\x1f".join(
            (job.job_key, plan_id, owner_id, now.isoformat(), str(attempt_count))
        )
        lease_id = "lease/" + hashlib.sha256(lease_material.encode("utf-8")).hexdigest()
        return RecoveryLease(
            job_key=job.job_key,
            lease_id=lease_id,
            owner_id=owner_id,
            plan_id=plan_id,
            action=job.action,
            state="leased",
            acquired_at=now,
            expires_at=now + self._lease_ttl,
            updated_at=now,
            attempt_count=attempt_count,
            evidence={"action": job.action.value, "reason_code": job.reason_code},
        )


class InMemoryLeaseBackend:
    """Thread-safe backend used by unit tests and local contract simulations."""

    def __init__(self) -> None:
        self._documents: dict[str, dict[str, object]] = {}
        self._lock = RLock()

    def read(self, job_key: str) -> Mapping[str, object] | None:
        with self._lock:
            document = self._documents.get(job_key)
            return json.loads(json.dumps(document)) if document is not None else None

    def create_if_absent(self, job_key: str, document: Mapping[str, object]) -> bool:
        with self._lock:
            if job_key in self._documents:
                return False
            self._documents[job_key] = json.loads(json.dumps(dict(document)))
            return True

    def replace_if_expired(
        self,
        job_key: str,
        expected_lease_id: str,
        now: datetime,
        document: Mapping[str, object],
    ) -> bool:
        with self._lock:
            current = self._documents.get(job_key)
            if current is None or current.get("lease_id") != expected_lease_id:
                return False
            parsed = _lease_from_document(current)
            if parsed.is_active_at(now):
                return False
            self._documents[job_key] = json.loads(json.dumps(dict(document)))
            return True

    def replace_if_owner(
        self,
        job_key: str,
        expected_lease_id: str,
        owner_id: str,
        now: datetime,
        document: Mapping[str, object],
    ) -> bool:
        with self._lock:
            current = self._documents.get(job_key)
            if current is None:
                return False
            if current.get("lease_id") != expected_lease_id or current.get("owner_id") != owner_id:
                return False
            if not _lease_from_document(current).is_active_at(now):
                return False
            self._documents[job_key] = json.loads(json.dumps(dict(document)))
            return True


def _lease_from_document(document: Mapping[str, object]) -> RecoveryLease:
    if document.get("schema_version") != SCHEMA_VERSION:
        raise RecoveryLeaseError("unsupported lease schema")
    try:
        return RecoveryLease(
            job_key=_text(document, "job_key"),
            lease_id=_text(document, "lease_id"),
            owner_id=_text(document, "owner_id"),
            plan_id=_text(document, "plan_id"),
            action=RecoveryAction(_text(document, "action")),
            state=_text(document, "state"),
            acquired_at=_aware_utc(document.get("acquired_at"), field="acquired_at"),
            expires_at=_aware_utc(document.get("expires_at"), field="expires_at"),
            updated_at=_aware_utc(document.get("updated_at"), field="updated_at"),
            attempt_count=_int(document.get("attempt_count"), field="attempt_count"),
            evidence=document.get("evidence") or {},
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, RecoveryLeaseError):
            raise
        raise RecoveryLeaseError("lease document is invalid") from exc


def _text(document: Mapping[str, object], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str):
        raise RecoveryLeaseError(f"{field} must be a string")
    return value


def _safe_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not _SAFE_TEXT.fullmatch(value):
        raise RecoveryLeaseError(f"{field} must be a safe identifier")
    return value


_DENIED_EVIDENCE_TOKENS = (
    "secret",
    "token",
    "password",
    "credential",
    "raw_object",
    "manifest_key",
    "access_key",
)


def _safe_evidence(value: Mapping[str, object]) -> None:
    if len(value) > 32:
        raise RecoveryLeaseError("evidence contains too many fields")
    for key, item in value.items():
        if not isinstance(key, str) or not _SAFE_TEXT.fullmatch(key):
            raise RecoveryLeaseError("evidence field name is invalid")
        lowered = key.lower()
        if any(token in lowered for token in _DENIED_EVIDENCE_TOKENS):
            raise RecoveryLeaseError("sensitive evidence field is not allowed")
        if item is not None and not isinstance(item, (str, bool, int, float)):
            raise RecoveryLeaseError("evidence values must be scalar")
        if isinstance(item, str) and (len(item) > 256 or "\n" in item or "\r" in item):
            raise RecoveryLeaseError("evidence text is invalid")


def _int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise RecoveryLeaseError(f"{field} must be a positive integer")
    return value


def _aware_utc(value: object, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RecoveryLeaseError(f"{field} must be an ISO timestamp") from exc
    else:
        raise RecoveryLeaseError(f"{field} must be a timestamp")
    if parsed.tzinfo is None:
        raise RecoveryLeaseError(f"{field} must include timezone")
    return parsed.astimezone(timezone.utc)


__all__ = [
    "ACTIVE_STATES",
    "InMemoryLeaseBackend",
    "LEASE_STATES",
    "LeaseBackend",
    "LeaseDecision",
    "LeaseLostError",
    "RecoveryLease",
    "RecoveryLeaseError",
    "RecoveryLeaseRegistry",
    "LeaseResult",
    "SCHEMA_VERSION",
    "TERMINAL_STATES",
]
