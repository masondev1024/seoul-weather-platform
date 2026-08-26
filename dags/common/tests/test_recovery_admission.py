from __future__ import annotations

import pytest

from common.recovery.admission import (
    AdmissionDecision,
    AdmissionPolicy,
    ActiveRunSnapshot,
    KMA_API_POOL,
    PoolSnapshot,
    RecoveryAdmissionError,
    TRINO_WEATHER_HEAVY_POOL,
    admit_dispatch_requests,
)
from common.recovery.dispatch import DispatchRequest, RAW_REPLAY_DAG_ID, RECOLLECT_DAG_ID
from common.recovery.planner import RecoveryAction


def _request(
    action: RecoveryAction = RecoveryAction.RAW_REPLAY,
    *,
    job_key: str = "weather-recovery/v1/admission-001",
) -> DispatchRequest:
    if action is RecoveryAction.RAW_REPLAY:
        target = RAW_REPLAY_DAG_ID
        conf = {
            "raw_object_keys": ["raw/weather/slot-a.json"],
            "recovery_manifest_key": "manifest/weather/slot-a.json",
        }
    else:
        target = RECOLLECT_DAG_ID
        conf = {"base_date": "20260826", "base_time": "0620"}
    return DispatchRequest(
        plan_id="plan-001",
        job_key=job_key,
        action=action,
        target_dag_id=target,
        conf=conf,
        api_cost=0 if action is RecoveryAction.RAW_REPLAY else 1,
    )


def _free_pools(*, queued: int = 0) -> dict[str, PoolSnapshot]:
    return {
        TRINO_WEATHER_HEAVY_POOL: PoolSnapshot(
            pool=TRINO_WEATHER_HEAVY_POOL,
            total_slots=1,
            occupied_slots=0,
            queued_tasks=queued,
        ),
        KMA_API_POOL: PoolSnapshot(
            pool=KMA_API_POOL,
            total_slots=1,
            occupied_slots=0,
            queued_tasks=queued,
        ),
    }


def test_raw_replay_is_admitted_only_when_conflict_family_and_pool_are_clear() -> None:
    result = admit_dispatch_requests(
        [_request()],
        pools=_free_pools(),
    )

    assert result[0].decision is AdmissionDecision.ADMIT
    assert result[0].reason_code == "admission_clear"
    assert result[0].to_redacted_dict()["mutation_performed"] is False
    assert "raw/weather/slot-a.json" not in str(result[0].to_redacted_dict())


def test_active_run_defers_instead_of_triggering_a_duplicate() -> None:
    result = admit_dispatch_requests(
        [_request()],
        active_runs=[
            ActiveRunSnapshot(
                dag_id="weather_vilage_fcst_transform",
                run_id="scheduled__2026-08-26T06:00:00+00:00",
                state="running",
            )
        ],
        pools=_free_pools(),
    )

    assert result[0].decision is AdmissionDecision.DEFER
    assert result[0].reason_code == "active_run_conflict"
    assert result[0].to_redacted_dict()["active_conflict_count"] == 1


def test_pool_pressure_defers_and_recollect_requires_both_pools() -> None:
    raw = admit_dispatch_requests(
        [_request()],
        pools=_free_pools(queued=1),
    )
    recollect = admit_dispatch_requests(
        [_request(RecoveryAction.RECOLLECT)],
        pools={
            TRINO_WEATHER_HEAVY_POOL: PoolSnapshot(
                pool=TRINO_WEATHER_HEAVY_POOL,
                total_slots=1,
                occupied_slots=0,
            ),
            KMA_API_POOL: PoolSnapshot(
                pool=KMA_API_POOL,
                total_slots=1,
                occupied_slots=1,
            ),
        },
    )

    assert raw[0].decision is AdmissionDecision.DEFER
    assert raw[0].reason_code == "pool_busy"
    assert recollect[0].decision is AdmissionDecision.DEFER
    assert [pool.pool for pool in recollect[0].pool_conflicts] == [KMA_API_POOL]


def test_only_one_request_is_admitted_by_default_and_duplicates_reject() -> None:
    first = _request(job_key="weather-recovery/v1/admission-001")
    second = _request(
        RecoveryAction.RECOLLECT,
        job_key="weather-recovery/v1/admission-002",
    )
    duplicate = _request(job_key=first.job_key)
    results = admit_dispatch_requests(
        [first, second, duplicate],
        pools=_free_pools(),
    )

    assert [result.decision for result in results] == [
        AdmissionDecision.ADMIT,
        AdmissionDecision.DEFER,
        AdmissionDecision.REJECT,
    ]
    assert results[1].reason_code == "dispatch_budget_exhausted"
    assert results[2].reason_code == "duplicate_job_key"


def test_missing_pool_and_invalid_snapshot_fail_closed() -> None:
    result = admit_dispatch_requests(
        [_request(RecoveryAction.RECOLLECT)],
        pools={TRINO_WEATHER_HEAVY_POOL: _free_pools()[TRINO_WEATHER_HEAVY_POOL]},
    )
    assert result[0].decision is AdmissionDecision.REJECT
    assert result[0].reason_code == "pool_snapshot_missing"

    with pytest.raises(RecoveryAdmissionError, match="mapping key"):
        admit_dispatch_requests(
            [_request()],
            pools={
                "wrong": PoolSnapshot(
                    pool=TRINO_WEATHER_HEAVY_POOL,
                    total_slots=1,
                    occupied_slots=0,
                )
            },
        )


def test_action_and_target_mismatch_is_rejected() -> None:
    request = _request(RecoveryAction.RAW_REPLAY)
    mismatched = DispatchRequest(
        plan_id=request.plan_id,
        job_key="weather-recovery/v1/action-target-mismatch",
        action=RecoveryAction.RECOLLECT,
        target_dag_id=RAW_REPLAY_DAG_ID,
        conf=request.conf,
        api_cost=1,
    )

    result = admit_dispatch_requests([mismatched], pools=_free_pools())

    assert result[0].decision is AdmissionDecision.REJECT
    assert result[0].reason_code == "action_target_mismatch"


def test_custom_policy_can_allow_non_empty_queue_but_never_exceeds_budget() -> None:
    results = admit_dispatch_requests(
        [_request(), _request(RecoveryAction.RECOLLECT, job_key="weather-recovery/v1/admission-002")],
        pools=_free_pools(queued=2),
        policy=AdmissionPolicy(max_dispatches=1, require_empty_queue=False),
    )
    assert results[0].decision is AdmissionDecision.ADMIT
    assert results[1].decision is AdmissionDecision.DEFER
