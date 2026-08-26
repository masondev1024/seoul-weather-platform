"""Deterministic, bounded, side-effect-free Weather recovery planner.

This module is the first implementation slice of the recovery control plane.
It deliberately does not trigger Airflow, call KMA, or write R2/Trino/D1.  It
only classifies already observed collection evidence and emits a plan that is
safe to evaluate repeatedly.  The future executor can use ``job_key`` as its
durable idempotency key and must keep the same admission rules.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Iterable, Mapping


SCHEMA_VERSION = "weather-recovery-plan/v1"
POLICY_VERSION = "weather-recovery-policy-v1"
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:+-]{0,199}$")


class RecoveryAction(StrEnum):
    """A planner decision; no value implies that a DAG was triggered."""

    RAW_REPLAY = "raw_replay"
    RECOLLECT = "recollect"
    BLOCKED = "blocked"
    DEFERRED = "deferred"


class RecoveryPlannerError(ValueError):
    """Input or policy evidence is invalid and planning must fail closed."""


@dataclass(frozen=True)
class RecoveryPolicy:
    """Safety budget applied to one coordinator invocation."""

    max_jobs: int = 3
    max_api_jobs: int = 1
    max_recovery_age: timedelta = timedelta(hours=24)
    policy_version: str = POLICY_VERSION

    def __post_init__(self) -> None:
        _positive_int(self.max_jobs, field="max_jobs")
        _non_negative_int(self.max_api_jobs, field="max_api_jobs")
        if self.max_api_jobs > self.max_jobs:
            raise RecoveryPlannerError("max_api_jobs must not exceed max_jobs")
        if (
            not isinstance(self.max_recovery_age, timedelta)
            or self.max_recovery_age <= timedelta(0)
        ):
            raise RecoveryPlannerError("max_recovery_age must be positive")
        _required_identifier(self.policy_version, field="policy_version")


@dataclass(frozen=True)
class RecoveryCandidate:
    """One recovery unit, normally one source issue cycle.

    ``slot_ids`` contains only the still-pending grains in this recovery unit.
    ``expected_count`` and ``covered_count`` make completeness explicit so a
    partial 80-grid manifest can never be silently promoted to replay.
    """

    domain: str
    source_id: str
    slot_key: str
    slot_ids: tuple[str, ...]
    scheduled_at: datetime
    deadline_at: datetime
    recovery_boundary: datetime
    expected_count: int
    covered_count: int
    raw_manifest_verified: bool
    historical_query_allowed: bool
    normal_run_active: bool = False
    normal_run_id: str | None = None
    last_failure_code: str | None = None
    attempt_count: int = 0

    def __post_init__(self) -> None:
        _required_identifier(self.domain, field="domain")
        _required_identifier(self.source_id, field="source_id")
        _required_identifier(self.slot_key, field="slot_key")
        if not self.slot_ids or len(set(self.slot_ids)) != len(self.slot_ids):
            raise RecoveryPlannerError("slot_ids must be a non-empty unique tuple")
        for slot_id in self.slot_ids:
            _required_identifier(slot_id, field="slot_id")
        for value, field_name in (
            (self.scheduled_at, "scheduled_at"),
            (self.deadline_at, "deadline_at"),
            (self.recovery_boundary, "recovery_boundary"),
        ):
            _aware_utc(value, field=field_name)
        if self.deadline_at < self.scheduled_at:
            raise RecoveryPlannerError("deadline_at must not precede scheduled_at")
        _non_negative_int(self.expected_count, field="expected_count")
        _non_negative_int(self.covered_count, field="covered_count")
        if self.expected_count == 0:
            raise RecoveryPlannerError("expected_count must be positive")
        if self.covered_count > self.expected_count:
            raise RecoveryPlannerError("covered_count must not exceed expected_count")
        if len(self.slot_ids) != self.expected_count:
            raise RecoveryPlannerError(
                "slot_ids length must equal expected_count for a bounded recovery unit"
            )
        if not isinstance(self.raw_manifest_verified, bool):
            raise RecoveryPlannerError("raw_manifest_verified must be a bool")
        if not isinstance(self.historical_query_allowed, bool):
            raise RecoveryPlannerError("historical_query_allowed must be a bool")
        if not isinstance(self.normal_run_active, bool):
            raise RecoveryPlannerError("normal_run_active must be a bool")
        if self.normal_run_id is not None:
            _required_identifier(self.normal_run_id, field="normal_run_id")
        if self.last_failure_code is not None:
            _required_identifier(self.last_failure_code, field="last_failure_code")
        _non_negative_int(self.attempt_count, field="attempt_count")

    @property
    def normalized_scheduled_at(self) -> datetime:
        return _aware_utc(self.scheduled_at, field="scheduled_at")

    @property
    def normalized_deadline_at(self) -> datetime:
        return _aware_utc(self.deadline_at, field="deadline_at")

    @property
    def normalized_recovery_boundary(self) -> datetime:
        return _aware_utc(self.recovery_boundary, field="recovery_boundary")


@dataclass(frozen=True)
class RecoveryJob:
    """One bounded admission decision, suitable for a future executor."""

    job_key: str
    action: RecoveryAction
    domain: str
    source_id: str
    slot_key: str
    slot_ids: tuple[str, ...]
    priority: int
    api_cost: int
    reason_code: str
    scheduled_at: str
    deadline_at: str
    evidence: Mapping[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "job_key": self.job_key,
            "action": self.action.value,
            "domain": self.domain,
            "source_id": self.source_id,
            "slot_key": self.slot_key,
            "slot_ids": list(self.slot_ids),
            "priority": self.priority,
            "api_cost": self.api_cost,
            "reason_code": self.reason_code,
            "scheduled_at": self.scheduled_at,
            "deadline_at": self.deadline_at,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True)
class RecoveryPlan:
    """A deterministic planner result with no mutation instruction."""

    plan_id: str
    generated_at: str
    status: str
    jobs: tuple[RecoveryJob, ...]
    blocked: tuple[RecoveryJob, ...]
    deferred: tuple[RecoveryJob, ...]
    metrics: Mapping[str, int]
    policy_version: str = POLICY_VERSION

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "policy_version": self.policy_version,
            "plan_id": self.plan_id,
            "generated_at": self.generated_at,
            "status": self.status,
            "mutation_performed": False,
            "jobs": [job.to_dict() for job in self.jobs],
            "blocked": [job.to_dict() for job in self.blocked],
            "deferred": [job.to_dict() for job in self.deferred],
            "metrics": dict(self.metrics),
        }


_BLOCKING_FAILURE_CODES = frozenset(
    {
        "auth_failed",
        "clock_skew",
        "data_contract_invalid",
        "invalid_contract",
        "quota_exhausted",
    }
)


def plan_recovery(
    candidates: Iterable[RecoveryCandidate],
    *,
    now: datetime,
    policy: RecoveryPolicy | None = None,
) -> RecoveryPlan:
    """Classify and budget candidates without triggering or writing anything.

    The function is deterministic for a fixed candidate set, policy, and
    ``now``.  Candidate order is never trusted.  Duplicate job identities are
    rejected rather than merged, because silently merging conflicting receipt
    evidence would hide a data-contract problem.
    """

    resolved_policy = policy or RecoveryPolicy()
    normalized_now = _aware_utc(now, field="now")
    materialized = tuple(candidates)
    by_identity: dict[str, RecoveryCandidate] = {}
    duplicate_conflicts: list[RecoveryJob] = []
    for candidate in materialized:
        identity = _candidate_identity(candidate)
        previous = by_identity.get(identity)
        if previous is None:
            by_identity[identity] = candidate
        elif previous != candidate:
            duplicate_conflicts.append(
                _job_for_blocked(
                    candidate,
                    reason_code="duplicate_candidate_conflict",
                    evidence={"identity": identity},
                )
            )

    classified: list[RecoveryJob] = []
    for candidate in by_identity.values():
        classified.append(_classify(candidate, normalized_now, resolved_policy))
    classified.extend(duplicate_conflicts)

    classified.sort(
        key=lambda job: (
            _action_order(job.action),
            -job.priority,
            job.scheduled_at,
            job.slot_key,
            job.job_key,
        )
    )

    admitted: list[RecoveryJob] = []
    blocked: list[RecoveryJob] = []
    deferred: list[RecoveryJob] = []
    api_jobs = 0
    for job in classified:
        if job.action is RecoveryAction.BLOCKED:
            blocked.append(job)
            continue
        if job.action is RecoveryAction.DEFERRED:
            deferred.append(job)
            continue
        if len(admitted) >= resolved_policy.max_jobs:
            deferred.append(
                _replace_job(
                    job,
                    action=RecoveryAction.DEFERRED,
                    reason_code="coordinator_job_budget_exhausted",
                )
            )
            continue
        if job.api_cost and api_jobs + job.api_cost > resolved_policy.max_api_jobs:
            deferred.append(
                _replace_job(
                    job,
                    action=RecoveryAction.DEFERRED,
                    reason_code="coordinator_api_budget_exhausted",
                )
            )
            continue
        admitted.append(job)
        api_jobs += job.api_cost

    status = "ready" if admitted else "empty"
    if blocked and not admitted:
        status = "blocked"
    elif deferred and not admitted and not blocked:
        # A candidate can be deferred because its normal run is still active
        # or because its deadline has not elapsed.  Calling every such plan
        # "budget exhausted" hides a healthy, expected wait and would make an
        # alerting rule page the operator unnecessarily.  Reserve the more
        # specific status for a plan where every deferred action hit a
        # coordinator budget.
        budget_reasons = {
            "coordinator_job_budget_exhausted",
            "coordinator_api_budget_exhausted",
        }
        status = (
            "budget_exhausted"
            if all(job.reason_code in budget_reasons for job in deferred)
            else "deferred"
        )

    plan_id = _plan_id(
        materialized=tuple(by_identity.values()),
        policy=resolved_policy,
    )
    metrics = {
        "candidate_count": len(materialized),
        "unique_candidate_count": len(by_identity),
        "admitted_job_count": len(admitted),
        "blocked_job_count": len(blocked),
        "deferred_job_count": len(deferred),
        "api_job_count": api_jobs,
        "raw_replay_count": sum(
            job.action is RecoveryAction.RAW_REPLAY for job in admitted
        ),
        "recollect_count": sum(
            job.action is RecoveryAction.RECOLLECT for job in admitted
        ),
    }
    return RecoveryPlan(
        plan_id=plan_id,
        generated_at=normalized_now.isoformat(),
        status=status,
        jobs=tuple(admitted),
        blocked=tuple(blocked),
        deferred=tuple(deferred),
        metrics=metrics,
        policy_version=resolved_policy.policy_version,
    )


def _classify(
    candidate: RecoveryCandidate,
    now: datetime,
    policy: RecoveryPolicy,
) -> RecoveryJob:
    if candidate.normal_run_active:
        return _job_for_deferred(
            candidate,
            reason_code="normal_run_active",
            evidence={
                "normal_run_id_present": candidate.normal_run_id is not None,
            },
        )
    if candidate.normalized_deadline_at > now:
        return _job_for_deferred(candidate, reason_code="slot_not_due")
    if candidate.last_failure_code in _BLOCKING_FAILURE_CODES:
        return _job_for_blocked(
            candidate,
            reason_code="deterministic_source_failure",
            evidence={"failure_code": candidate.last_failure_code},
        )
    age = now - candidate.normalized_scheduled_at
    if age > policy.max_recovery_age:
        return _job_for_blocked(
            candidate,
            reason_code="recovery_age_exceeded",
            evidence={"age_seconds": int(age.total_seconds())},
        )
    if candidate.raw_manifest_verified and (
        candidate.covered_count != candidate.expected_count
    ):
        return _job_for_blocked(
            candidate,
            reason_code="incomplete_coverage",
            evidence={
                "expected_count": candidate.expected_count,
                "covered_count": candidate.covered_count,
            },
        )
    if candidate.raw_manifest_verified:
        return _job_for_action(
            candidate,
            action=RecoveryAction.RAW_REPLAY,
            priority=100,
            api_cost=0,
            reason_code="raw_manifest_verified",
            evidence={"manifest_verified": True},
        )
    if candidate.historical_query_allowed:
        return _job_for_action(
            candidate,
            action=RecoveryAction.RECOLLECT,
            priority=50,
            api_cost=1,
            reason_code="historical_query_allowed",
            evidence={"historical_query_allowed": True},
        )
    return _job_for_blocked(candidate, reason_code="no_recovery_evidence")


def _candidate_identity(candidate: RecoveryCandidate) -> str:
    return ":".join((candidate.domain, candidate.source_id, candidate.slot_key))


def _job_key(candidate: RecoveryCandidate, action: RecoveryAction) -> str:
    material = {
        "schema_version": SCHEMA_VERSION,
        "domain": candidate.domain,
        "source_id": candidate.source_id,
        "slot_key": candidate.slot_key,
        "action": action.value,
        "slot_ids": sorted(candidate.slot_ids),
    }
    digest = hashlib.sha256(_canonical_json(material).encode("utf-8")).hexdigest()
    return f"weather-recovery/v1/{digest}"


def _plan_id(
    *,
    materialized: tuple[RecoveryCandidate, ...],
    policy: RecoveryPolicy,
) -> str:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "policy_version": policy.policy_version,
        "candidates": [
            {
                "identity": _candidate_identity(candidate),
                "slot_ids": sorted(candidate.slot_ids),
                "scheduled_at": candidate.normalized_scheduled_at.isoformat(),
                "deadline_at": candidate.normalized_deadline_at.isoformat(),
                "recovery_boundary": candidate.normalized_recovery_boundary.isoformat(),
                "expected_count": candidate.expected_count,
                "covered_count": candidate.covered_count,
                "raw_manifest_verified": candidate.raw_manifest_verified,
                "historical_query_allowed": candidate.historical_query_allowed,
                "normal_run_active": candidate.normal_run_active,
                "last_failure_code": candidate.last_failure_code,
            }
            for candidate in sorted(materialized, key=_candidate_identity)
        ],
    }
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _job_for_action(
    candidate: RecoveryCandidate,
    *,
    action: RecoveryAction,
    priority: int,
    api_cost: int,
    reason_code: str,
    evidence: Mapping[str, object] | None = None,
) -> RecoveryJob:
    return RecoveryJob(
        job_key=_job_key(candidate, action),
        action=action,
        domain=candidate.domain,
        source_id=candidate.source_id,
        slot_key=candidate.slot_key,
        slot_ids=tuple(sorted(candidate.slot_ids)),
        priority=priority,
        api_cost=api_cost,
        reason_code=reason_code,
        scheduled_at=candidate.normalized_scheduled_at.isoformat(),
        deadline_at=candidate.normalized_deadline_at.isoformat(),
        evidence=dict(evidence or {}),
    )


def _job_for_blocked(
    candidate: RecoveryCandidate,
    *,
    reason_code: str,
    evidence: Mapping[str, object] | None = None,
) -> RecoveryJob:
    return _job_for_action(
        candidate,
        action=RecoveryAction.BLOCKED,
        priority=0,
        api_cost=0,
        reason_code=reason_code,
        evidence=evidence,
    )


def _job_for_deferred(
    candidate: RecoveryCandidate,
    *,
    reason_code: str,
    evidence: Mapping[str, object] | None = None,
) -> RecoveryJob:
    return _job_for_action(
        candidate,
        action=RecoveryAction.DEFERRED,
        priority=0,
        api_cost=0,
        reason_code=reason_code,
        evidence=evidence,
    )


def _replace_job(
    job: RecoveryJob,
    *,
    action: RecoveryAction,
    reason_code: str,
) -> RecoveryJob:
    return RecoveryJob(
        job_key=job.job_key,
        action=action,
        domain=job.domain,
        source_id=job.source_id,
        slot_key=job.slot_key,
        slot_ids=job.slot_ids,
        priority=job.priority,
        api_cost=job.api_cost,
        reason_code=reason_code,
        scheduled_at=job.scheduled_at,
        deadline_at=job.deadline_at,
        evidence=job.evidence,
    )


def _action_order(action: RecoveryAction) -> int:
    return {
        RecoveryAction.RAW_REPLAY: 0,
        RecoveryAction.RECOLLECT: 1,
        RecoveryAction.BLOCKED: 2,
        RecoveryAction.DEFERRED: 3,
    }[action]


def _required_identifier(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not _SAFE_IDENTIFIER.fullmatch(value):
        raise RecoveryPlannerError(f"{field} must be a non-empty safe identifier")
    return value


def _positive_int(value: object, *, field: str) -> int:
    _non_negative_int(value, field=field)
    if value == 0:
        raise RecoveryPlannerError(f"{field} must be positive")
    return int(value)


def _non_negative_int(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RecoveryPlannerError(f"{field} must be a non-negative integer")
    return value


def _aware_utc(value: object, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RecoveryPlannerError(f"{field} must be an ISO timestamp") from exc
    else:
        raise RecoveryPlannerError(f"{field} must be a datetime or ISO timestamp")
    if parsed.tzinfo is None:
        raise RecoveryPlannerError(f"{field} must include timezone")
    return parsed.astimezone(timezone.utc)


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


__all__ = [
    "POLICY_VERSION",
    "RecoveryAction",
    "RecoveryCandidate",
    "RecoveryJob",
    "RecoveryPlan",
    "RecoveryPlannerError",
    "RecoveryPolicy",
    "SCHEMA_VERSION",
    "plan_recovery",
]
