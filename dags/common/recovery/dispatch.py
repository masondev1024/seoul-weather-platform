"""Compile recovery plans into safe Airflow trigger requests.

This module is intentionally a pure compiler.  It validates the evidence that
would be handed to the existing Weather replay/recollect DAGs, but it never
calls Airflow's API and never performs a storage write.  A later executor can
claim a lease first and then pass ``DispatchRequest.trigger_payload()`` to a
single, audited trigger adapter.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from zoneinfo import ZoneInfo

from common.recovery.planner import RecoveryAction, RecoveryJob, RecoveryPlan


SCHEMA_VERSION = "weather-recovery-dispatch/v1"
WEATHER_SOURCE_ID = "kma_vilage_fcst"
RAW_REPLAY_DAG_ID = "weather_vilage_fcst_bronze_backfill"
RECOLLECT_DAG_ID = "weather_vilage_fcst_recollect"
KST = ZoneInfo("Asia/Seoul")
_SAFE_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._=/:+-]{0,511}$")
_LOAD_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class RecoveryDispatchError(ValueError):
    """Evidence cannot be compiled into a bounded Weather trigger request."""


class DispatchDecision(StrEnum):
    COMPILED = "compiled"
    REJECTED = "rejected"


@dataclass(frozen=True)
class RawReplayEvidence:
    """Verified raw manifest pointer and object list for one recovery job.

    The boolean is deliberately explicit: callers must set it only after
    reading the manifest and validating its run id, dataset, completeness, and
    object hashes against the expected slots.  A pointer that merely exists is
    not replay evidence.
    """

    job_key: str
    manifest_key: str
    object_keys: tuple[str, ...]
    load_date: str | None = None
    manifest_verified: bool = False

    def __post_init__(self) -> None:
        _safe_key(self.job_key, field="job_key")
        _safe_key(self.manifest_key, field="manifest_key")
        if self.manifest_verified is not True:
            raise RecoveryDispatchError("raw replay manifest must be verified")
        if not self.object_keys or len(set(self.object_keys)) != len(self.object_keys):
            raise RecoveryDispatchError("raw replay object_keys must be non-empty and unique")
        for key in self.object_keys:
            _safe_key(key, field="raw_object_key")
            if "?" in key or "#" in key:
                raise RecoveryDispatchError("raw object key must not contain query or fragment data")
        if self.load_date is not None and not _LOAD_DATE.fullmatch(self.load_date):
            raise RecoveryDispatchError("load_date must use YYYY-MM-DD")


@dataclass(frozen=True)
class DispatchRequest:
    """Validated, but not executed, request for an existing Weather DAG."""

    plan_id: str
    job_key: str
    action: RecoveryAction
    target_dag_id: str
    conf: Mapping[str, object] = field(default_factory=dict)
    api_cost: int = 0

    def trigger_payload(self) -> dict[str, object]:
        """Return only arguments a future trigger adapter may submit."""
        return {
            "trigger_dag_id": self.target_dag_id,
            "conf": dict(self.conf),
            "reset_dag_run": False,
            "wait_for_completion": False,
        }

    def to_redacted_dict(self) -> dict[str, object]:
        """Return an audit-safe summary without object key or conf contents."""
        return {
            "schema_version": SCHEMA_VERSION,
            "plan_id": self.plan_id,
            "job_key": self.job_key,
            "action": self.action.value,
            "target_dag_id": self.target_dag_id,
            "conf_keys": sorted(str(key) for key in self.conf),
            "raw_object_count": (
                len(self.conf.get("raw_object_keys", []))
                if isinstance(self.conf.get("raw_object_keys"), list)
                else 0
            ),
            "api_cost": self.api_cost,
            "mutation_performed": False,
        }


def compile_dispatch_requests(
    plan: RecoveryPlan,
    *,
    raw_evidence_by_job_key: Mapping[str, RawReplayEvidence] | None = None,
) -> tuple[DispatchRequest, ...]:
    """Compile admitted planner jobs without invoking Airflow or a source.

    Raw replay is fail-closed when a verified manifest pointer is absent.  The
    planner intentionally omits raw object names from its public JSON; the
    executor must resolve this private evidence immediately before claiming a
    lease and must not derive it from untrusted user input.
    """
    _safe_key(plan.plan_id, field="plan_id")
    evidence_by_job = raw_evidence_by_job_key or {}
    requests: list[DispatchRequest] = []
    seen: set[str] = set()
    for job in plan.jobs:
        if job.job_key in seen:
            raise RecoveryDispatchError("plan contains duplicate job_key")
        seen.add(job.job_key)
        if job.domain != "weather" or job.source_id != WEATHER_SOURCE_ID:
            raise RecoveryDispatchError("dispatch only accepts the Weather forecast source")
        if job.action is RecoveryAction.RAW_REPLAY:
            evidence = evidence_by_job.get(job.job_key)
            if evidence is None or evidence.job_key != job.job_key:
                raise RecoveryDispatchError("raw replay evidence is missing")
            if len(evidence.object_keys) < len(job.slot_ids):
                raise RecoveryDispatchError("raw replay evidence does not cover planned slots")
            conf: dict[str, object] = {
                "raw_object_keys": list(evidence.object_keys),
                "recovery_manifest_key": evidence.manifest_key,
                "recovery_job_key": job.job_key,
                "recovery_plan_id": plan.plan_id,
                "recovery_strategy": "raw_replay",
            }
            if evidence.load_date is not None:
                conf["load_date"] = evidence.load_date
            requests.append(
                DispatchRequest(
                    plan_id=plan.plan_id,
                    job_key=job.job_key,
                    action=job.action,
                    target_dag_id=RAW_REPLAY_DAG_ID,
                    conf=conf,
                    api_cost=0,
                )
            )
            continue
        if job.action is RecoveryAction.RECOLLECT:
            base_date, base_time = _kma_base_datetime(job.slot_key)
            requests.append(
                DispatchRequest(
                    plan_id=plan.plan_id,
                    job_key=job.job_key,
                    action=job.action,
                    target_dag_id=RECOLLECT_DAG_ID,
                    conf={
                        "base_date": base_date,
                        "base_time": base_time,
                        "recovery_job_key": job.job_key,
                        "recovery_plan_id": plan.plan_id,
                        "recovery_strategy": "recollect",
                    },
                    api_cost=1,
                )
            )
            continue
        raise RecoveryDispatchError("blocked or deferred jobs cannot be dispatched")
    return tuple(requests)


def _kma_base_datetime(slot_key: str) -> tuple[str, str]:
    try:
        parsed = datetime.fromisoformat(slot_key.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RecoveryDispatchError("slot_key must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise RecoveryDispatchError("slot_key must include timezone")
    issue = parsed.astimezone(KST)
    return issue.strftime("%Y%m%d"), issue.strftime("%H%M")


def _safe_key(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not _SAFE_KEY.fullmatch(value):
        raise RecoveryDispatchError(f"{field} must be a safe non-empty identifier")
    return value


__all__ = [
    "DispatchDecision",
    "DispatchRequest",
    "KST",
    "RAW_REPLAY_DAG_ID",
    "RawReplayEvidence",
    "RECOLLECT_DAG_ID",
    "RecoveryDispatchError",
    "SCHEMA_VERSION",
    "compile_dispatch_requests",
]
