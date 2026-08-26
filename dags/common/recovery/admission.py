"""Read-only admission policy for bounded Weather recovery dispatches.

The compiler creates a valid trigger payload, while this module decides whether
that payload may be handed to a future executor *now*.  It consumes snapshots
of Airflow DagRuns and pools captured by an adapter, but it never calls Airflow
or changes a DagRun.  Keeping the decision pure makes queue pressure and race
policies testable without a live scheduler.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum

from common.recovery.dispatch import (
    RAW_REPLAY_DAG_ID,
    RECOLLECT_DAG_ID,
    DispatchRequest,
)


SCHEMA_VERSION = "weather-recovery-admission/v1"
ACTIVE_RUN_STATES = frozenset({"queued", "running", "scheduled", "up_for_retry", "deferred"})
TRINO_WEATHER_HEAVY_POOL = "trino_weather_heavy"
KMA_API_POOL = "kma_api_requests"
_SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+-]{0,255}$")


class AdmissionDecision(StrEnum):
    ADMIT = "admit"
    DEFER = "defer"
    REJECT = "reject"


class RecoveryAdmissionError(ValueError):
    """An Airflow state snapshot cannot be trusted for admission."""


@dataclass(frozen=True)
class ActiveRunSnapshot:
    """Redacted DagRun state returned by a read-only Airflow adapter."""

    dag_id: str
    run_id: str
    state: str

    def __post_init__(self) -> None:
        _safe_text(self.dag_id, field="dag_id")
        _safe_text(self.run_id, field="run_id")
        _safe_text(self.state, field="state")

    @property
    def active(self) -> bool:
        return self.state in ACTIVE_RUN_STATES


@dataclass(frozen=True)
class PoolSnapshot:
    """One read-only Airflow pool snapshot at admission time."""

    pool: str
    total_slots: int
    occupied_slots: int
    queued_tasks: int = 0

    def __post_init__(self) -> None:
        _safe_text(self.pool, field="pool")
        for value, field_name in (
            (self.total_slots, "total_slots"),
            (self.occupied_slots, "occupied_slots"),
            (self.queued_tasks, "queued_tasks"),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise RecoveryAdmissionError(f"{field_name} must be a non-negative integer")
        if self.total_slots == 0:
            raise RecoveryAdmissionError("total_slots must be positive")
        if self.occupied_slots > self.total_slots:
            raise RecoveryAdmissionError("occupied_slots exceeds total_slots")

    @property
    def available_slots(self) -> int:
        return self.total_slots - self.occupied_slots


@dataclass(frozen=True)
class AdmissionPolicy:
    """Conservative admission budget for one coordinator attempt."""

    max_dispatches: int = 1
    require_empty_queue: bool = True

    def __post_init__(self) -> None:
        if isinstance(self.max_dispatches, bool) or not isinstance(self.max_dispatches, int):
            raise RecoveryAdmissionError("max_dispatches must be an integer")
        if self.max_dispatches < 1:
            raise RecoveryAdmissionError("max_dispatches must be positive")
        if not isinstance(self.require_empty_queue, bool):
            raise RecoveryAdmissionError("require_empty_queue must be a bool")


@dataclass(frozen=True)
class AdmissionResult:
    """A safe admission decision; no trigger side effect is implied."""

    request: DispatchRequest
    decision: AdmissionDecision
    reason_code: str
    active_conflicts: tuple[ActiveRunSnapshot, ...] = ()
    pool_conflicts: tuple[PoolSnapshot, ...] = ()

    def to_redacted_dict(self) -> dict[str, object]:
        """Serialize diagnostics without including trigger conf/object keys."""
        return {
            "schema_version": SCHEMA_VERSION,
            "plan_id": self.request.plan_id,
            "job_key": self.request.job_key,
            "target_dag_id": self.request.target_dag_id,
            "action": self.request.action.value,
            "decision": self.decision.value,
            "reason_code": self.reason_code,
            "active_conflict_count": len(self.active_conflicts),
            "pool_conflicts": [
                {
                    "pool": pool.pool,
                    "available_slots": pool.available_slots,
                    "queued_tasks": pool.queued_tasks,
                }
                for pool in self.pool_conflicts
            ],
            "mutation_performed": False,
        }


def admit_dispatch_requests(
    requests: Iterable[DispatchRequest],
    *,
    active_runs: Iterable[ActiveRunSnapshot] = (),
    pools: Mapping[str, PoolSnapshot],
    policy: AdmissionPolicy | None = None,
) -> tuple[AdmissionResult, ...]:
    """Admit at most the configured number of requests from a read-only snapshot.

    A normal or already-running recovery DagRun in the Weather conflict family
    causes a defer decision.  Pool pressure is also a defer decision so the
    scheduler can retry after the next normal cycle, rather than turning a
    bounded one-slot Trino lane into an unbounded queue.  Malformed snapshots
    and unknown pools reject closed.
    """
    resolved_policy = policy or AdmissionPolicy()
    materialized = tuple(requests)
    snapshots = tuple(active_runs)
    _validate_active_runs(snapshots)
    _validate_pools(pools)
    seen_jobs: set[str] = set()
    admitted = 0
    results: list[AdmissionResult] = []
    for request in materialized:
        if request.job_key in seen_jobs:
            results.append(
                AdmissionResult(request, AdmissionDecision.REJECT, "duplicate_job_key")
            )
            continue
        seen_jobs.add(request.job_key)
        if request.target_dag_id not in {RAW_REPLAY_DAG_ID, RECOLLECT_DAG_ID}:
            results.append(
                AdmissionResult(request, AdmissionDecision.REJECT, "unsupported_target_dag")
            )
            continue
        expected_action = (
            "raw_replay"
            if request.target_dag_id == RAW_REPLAY_DAG_ID
            else "recollect"
        )
        if request.action.value != expected_action:
            results.append(
                AdmissionResult(request, AdmissionDecision.REJECT, "action_target_mismatch")
            )
            continue
        conflicts = tuple(
            run
            for run in snapshots
            if run.active and run.dag_id in _conflict_dag_ids(request.target_dag_id)
        )
        if conflicts:
            results.append(
                AdmissionResult(
                    request,
                    AdmissionDecision.DEFER,
                    "active_run_conflict",
                    active_conflicts=conflicts,
                )
            )
            continue
        required_pools = _required_pools(request)
        missing = tuple(
            PoolSnapshot(pool=pool, total_slots=1, occupied_slots=1)
            for pool in required_pools
            if pool not in pools
        )
        if missing:
            results.append(
                AdmissionResult(
                    request,
                    AdmissionDecision.REJECT,
                    "pool_snapshot_missing",
                    pool_conflicts=missing,
                )
            )
            continue
        pool_conflicts = tuple(
            pools[pool]
            for pool in required_pools
            if pools[pool].available_slots < 1
            or (resolved_policy.require_empty_queue and pools[pool].queued_tasks > 0)
        )
        if pool_conflicts:
            results.append(
                AdmissionResult(
                    request,
                    AdmissionDecision.DEFER,
                    "pool_busy",
                    pool_conflicts=pool_conflicts,
                )
            )
            continue
        if admitted >= resolved_policy.max_dispatches:
            results.append(
                AdmissionResult(request, AdmissionDecision.DEFER, "dispatch_budget_exhausted")
            )
            continue
        admitted += 1
        results.append(AdmissionResult(request, AdmissionDecision.ADMIT, "admission_clear"))
    return tuple(results)


def _required_pools(request: DispatchRequest) -> tuple[str, ...]:
    if request.target_dag_id == RAW_REPLAY_DAG_ID:
        return (TRINO_WEATHER_HEAVY_POOL,)
    if request.target_dag_id == RECOLLECT_DAG_ID:
        return (KMA_API_POOL, TRINO_WEATHER_HEAVY_POOL)
    raise RecoveryAdmissionError("unsupported target DAG")


def _conflict_dag_ids(target_dag_id: str) -> frozenset[str]:
    # Both recovery targets mutate the same forecast source.  Downstream
    # transform/publication is included so a recovery cannot change its input
    # while a snapshot is being materialized.
    common = {
        "weather_vilage_fcst_bronze",
        "weather_vilage_fcst_bronze_backfill",
        "weather_vilage_fcst_recollect",
        "weather_vilage_fcst_transform",
        "weather_serving_snapshot_refresh",
        "weather_serving_export",
        "weather_serving_freshness_watchdog",
        "weather_ultra_srt_ncst_bronze",
        "weather_w2_canonical_transform",
        "weather_w1_contract_smoke",
        "weather_w2_canonical_contract_audit",
        "weather_w2_observation_recovery",
        "weather_reference_data_refresh",
        "ask_seoul_iceberg_maintenance",
    }
    if target_dag_id in {RAW_REPLAY_DAG_ID, RECOLLECT_DAG_ID}:
        return frozenset(common)
    raise RecoveryAdmissionError("unsupported target DAG")


def _validate_active_runs(runs: tuple[ActiveRunSnapshot, ...]) -> None:
    seen: set[tuple[str, str]] = set()
    for run in runs:
        identity = (run.dag_id, run.run_id)
        if identity in seen:
            raise RecoveryAdmissionError("duplicate active run snapshot")
        seen.add(identity)


def _validate_pools(pools: Mapping[str, PoolSnapshot]) -> None:
    if not isinstance(pools, Mapping):
        raise RecoveryAdmissionError("pools must be a mapping")
    for name, snapshot in pools.items():
        if not isinstance(snapshot, PoolSnapshot):
            raise RecoveryAdmissionError("pool mapping value is invalid")
        if name != snapshot.pool:
            raise RecoveryAdmissionError("pool mapping key does not match snapshot")


def _safe_text(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not _SAFE_TEXT.fullmatch(value):
        raise RecoveryAdmissionError(f"{field} must be a safe identifier")
    return value


__all__ = [
    "ACTIVE_RUN_STATES",
    "AdmissionDecision",
    "AdmissionPolicy",
    "AdmissionResult",
    "ActiveRunSnapshot",
    "KMA_API_POOL",
    "PoolSnapshot",
    "RecoveryAdmissionError",
    "SCHEMA_VERSION",
    "TRINO_WEATHER_HEAVY_POOL",
    "admit_dispatch_requests",
]
