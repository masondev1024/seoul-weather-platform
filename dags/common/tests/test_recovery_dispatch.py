from __future__ import annotations

from dataclasses import replace

import pytest

from common.recovery.dispatch import (
    RAW_REPLAY_DAG_ID,
    RECOLLECT_DAG_ID,
    RawReplayEvidence,
    RecoveryDispatchError,
    compile_dispatch_requests,
)
from common.recovery.planner import RecoveryAction, RecoveryJob, RecoveryPlan


def _job(action: RecoveryAction, *, job_key: str) -> RecoveryJob:
    return RecoveryJob(
        job_key=job_key,
        action=action,
        domain="weather",
        source_id="kma_vilage_fcst",
        slot_key="2026-08-26T04:20:00+00:00",
        slot_ids=("slot-a", "slot-b"),
        priority=100 if action is RecoveryAction.RAW_REPLAY else 50,
        api_cost=0 if action is RecoveryAction.RAW_REPLAY else 1,
        reason_code="raw_manifest_verified" if action is RecoveryAction.RAW_REPLAY else "historical_query_allowed",
        scheduled_at="2026-08-26T04:20:00+00:00",
        deadline_at="2026-08-26T05:20:00+00:00",
    )


def _plan(*jobs: RecoveryJob) -> RecoveryPlan:
    return RecoveryPlan(
        plan_id="a" * 64,
        generated_at="2026-08-26T06:00:00+00:00",
        status="ready",
        jobs=jobs,
        blocked=(),
        deferred=(),
        metrics={"admitted_job_count": len(jobs)},
    )


def test_raw_replay_compiles_to_existing_backfill_conf_without_network_io() -> None:
    job = _job(RecoveryAction.RAW_REPLAY, job_key="weather-recovery/v1/raw-001")
    request = compile_dispatch_requests(
        _plan(job),
        raw_evidence_by_job_key={
            job.job_key: RawReplayEvidence(
                job_key=job.job_key,
                manifest_key="control/weather/manifest.json",
                object_keys=(
                    "raw/weather/nx=1/ny=1/page-1.json",
                    "raw/weather/nx=2/ny=2/page-1.json",
                ),
                load_date="2026-08-26",
                manifest_verified=True,
            )
        },
    )[0]

    assert request.target_dag_id == RAW_REPLAY_DAG_ID
    assert request.api_cost == 0
    assert request.trigger_payload()["reset_dag_run"] is False
    assert request.trigger_payload()["conf"]["recovery_strategy"] == "raw_replay"
    redacted = request.to_redacted_dict()
    assert redacted["raw_object_count"] == 2
    assert "raw/weather" not in str(redacted)


def test_recollect_compiles_slot_key_to_kma_kst_conf() -> None:
    job = _job(RecoveryAction.RECOLLECT, job_key="weather-recovery/v1/recollect-001")
    request = compile_dispatch_requests(_plan(job))[0]

    assert request.target_dag_id == RECOLLECT_DAG_ID
    assert request.api_cost == 1
    assert request.conf["base_date"] == "20260826"
    assert request.conf["base_time"] == "1320"


def test_raw_replay_without_private_manifest_evidence_fails_closed() -> None:
    job = _job(RecoveryAction.RAW_REPLAY, job_key="weather-recovery/v1/raw-002")

    try:
        compile_dispatch_requests(_plan(job))
    except RecoveryDispatchError as exc:
        assert str(exc) == "raw replay evidence is missing"
    else:
        raise AssertionError("missing raw evidence must not compile")


def test_unverified_manifest_pointer_is_not_replay_evidence() -> None:
    with pytest.raises(RecoveryDispatchError, match="must be verified"):
        RawReplayEvidence(
            job_key="weather-recovery/v1/raw-unverified",
            manifest_key="control/weather/manifest.json",
            object_keys=("raw/weather/nx=1/ny=1/page-1.json",),
        )


def test_dispatch_rejects_wrong_source_and_partial_manifest() -> None:
    job = _job(RecoveryAction.RAW_REPLAY, job_key="weather-recovery/v1/raw-003")
    wrong_source = replace(job, source_id="other_source")
    with pytest.raises(RecoveryDispatchError, match="Weather forecast source"):
        compile_dispatch_requests(_plan(wrong_source))

    with pytest.raises(RecoveryDispatchError, match="does not cover planned slots"):
        compile_dispatch_requests(
            _plan(job),
            raw_evidence_by_job_key={
                job.job_key: RawReplayEvidence(
                    job_key=job.job_key,
                    manifest_key="control/weather/manifest.json",
                    object_keys=("raw/weather/nx=1/ny=1/page-1.json",),
                    manifest_verified=True,
                )
            },
        )


def test_redacted_request_never_contains_trigger_command_or_object_keys() -> None:
    job = _job(RecoveryAction.RAW_REPLAY, job_key="weather-recovery/v1/raw-004")
    request = compile_dispatch_requests(
        _plan(job),
        raw_evidence_by_job_key={
            job.job_key: RawReplayEvidence(
                job_key=job.job_key,
                manifest_key="control/weather/manifest.json",
                object_keys=(
                    "raw/weather/nx=1/ny=1/page-1.json",
                    "raw/weather/nx=2/ny=2/page-1.json",
                ),
                manifest_verified=True,
            )
        },
    )[0]
    rendered = str(request.to_redacted_dict()).lower()

    assert "airflow dags trigger" not in rendered
    assert "page-1" not in rendered
