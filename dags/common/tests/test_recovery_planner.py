from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from common.recovery.planner import (
    RecoveryAction,
    RecoveryCandidate,
    RecoveryPlannerError,
    RecoveryPolicy,
    plan_recovery,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 26, 6, 0, tzinfo=UTC)


def _candidate(
    slot_key: str,
    *,
    scheduled_at: datetime = NOW - timedelta(hours=1),
    expected_count: int = 80,
    covered_count: int = 80,
    raw_manifest_verified: bool = True,
    historical_query_allowed: bool = False,
    normal_run_active: bool = False,
    normal_run_id: str | None = None,
    last_failure_code: str | None = None,
) -> RecoveryCandidate:
    return RecoveryCandidate(
        domain="weather",
        source_id="kma_vilage_fcst",
        slot_key=slot_key,
        slot_ids=tuple(f"slot-{slot_key}-{index}" for index in range(expected_count)),
        scheduled_at=scheduled_at,
        deadline_at=scheduled_at + timedelta(minutes=60),
        recovery_boundary=NOW - timedelta(days=7),
        expected_count=expected_count,
        covered_count=covered_count,
        raw_manifest_verified=raw_manifest_verified,
        historical_query_allowed=historical_query_allowed,
        normal_run_active=normal_run_active,
        normal_run_id=normal_run_id,
        last_failure_code=last_failure_code,
    )


def test_raw_replay_is_admitted_before_api_recollect_and_budgeted() -> None:
    candidates = [
        _candidate("recollect-1", raw_manifest_verified=False, historical_query_allowed=True),
        _candidate("raw-1"),
        _candidate("raw-2"),
        _candidate("raw-3"),
    ]

    plan = plan_recovery(
        candidates,
        now=NOW,
        policy=RecoveryPolicy(max_jobs=3, max_api_jobs=1),
    )

    assert [job.action for job in plan.jobs] == [
        RecoveryAction.RAW_REPLAY,
        RecoveryAction.RAW_REPLAY,
        RecoveryAction.RAW_REPLAY,
    ]
    assert plan.deferred[0].action is RecoveryAction.DEFERRED
    assert plan.deferred[0].reason_code == "coordinator_job_budget_exhausted"
    assert plan.metrics["api_job_count"] == 0


def test_api_budget_defers_second_historical_recollect() -> None:
    candidates = [
        _candidate("h-1", raw_manifest_verified=False, historical_query_allowed=True),
        _candidate("h-2", raw_manifest_verified=False, historical_query_allowed=True),
    ]

    plan = plan_recovery(
        candidates,
        now=NOW,
        policy=RecoveryPolicy(max_jobs=3, max_api_jobs=1),
    )

    assert [job.action for job in plan.jobs] == [RecoveryAction.RECOLLECT]
    assert len(plan.deferred) == 1
    assert plan.deferred[0].reason_code == "coordinator_api_budget_exhausted"
    assert plan.metrics["api_job_count"] == 1


def test_plan_and_job_keys_are_order_independent_and_side_effect_free() -> None:
    first = _candidate("same")
    second = _candidate("other", raw_manifest_verified=False, historical_query_allowed=True)

    left = plan_recovery([first, second], now=NOW)
    right = plan_recovery([second, first], now=NOW)

    assert left.plan_id == right.plan_id
    assert [job.job_key for job in left.jobs] == [job.job_key for job in right.jobs]
    assert left.to_dict()["mutation_performed"] is False
    assert "airflow" not in str(left.to_dict()).lower()


@pytest.mark.parametrize(
    ("kwargs", "expected_action", "expected_reason"),
    [
        (
            {"normal_run_active": True, "normal_run_id": "scheduled__2026-08-26T05:20:00+00:00"},
            RecoveryAction.DEFERRED,
            "normal_run_active",
        ),
        (
            {"scheduled_at": NOW - timedelta(minutes=20)},
            RecoveryAction.DEFERRED,
            "slot_not_due",
        ),
        (
            {"scheduled_at": NOW - timedelta(days=2)},
            RecoveryAction.BLOCKED,
            "recovery_age_exceeded",
        ),
        (
            {"covered_count": 79, "raw_manifest_verified": True},
            RecoveryAction.BLOCKED,
            "incomplete_coverage",
        ),
        (
            {"raw_manifest_verified": False, "historical_query_allowed": False},
            RecoveryAction.BLOCKED,
            "no_recovery_evidence",
        ),
        (
            {"last_failure_code": "clock_skew"},
            RecoveryAction.BLOCKED,
            "deterministic_source_failure",
        ),
    ],
)
def test_candidate_classification_is_fail_closed(
    kwargs: dict[str, object],
    expected_action: RecoveryAction,
    expected_reason: str,
) -> None:
    plan = plan_recovery([_candidate("classified", **kwargs)], now=NOW)

    jobs = (*plan.jobs, *plan.blocked, *plan.deferred)
    assert len(jobs) == 1
    assert jobs[0].action is expected_action
    assert jobs[0].reason_code == expected_reason


def test_only_not_due_candidates_are_deferred_not_budget_exhausted() -> None:
    plan = plan_recovery(
        [_candidate("future", scheduled_at=NOW - timedelta(minutes=20))],
        now=NOW,
    )

    assert plan.status == "deferred"
    assert plan.jobs == ()
    assert plan.deferred[0].reason_code == "slot_not_due"


def test_conflicting_duplicate_candidate_is_blocked_instead_of_merged() -> None:
    one = _candidate("duplicate")
    conflicting = _candidate("duplicate", covered_count=79, raw_manifest_verified=True)

    plan = plan_recovery([one, conflicting], now=NOW)

    assert len(plan.jobs) == 1
    assert len(plan.blocked) == 1
    assert plan.blocked[0].reason_code == "duplicate_candidate_conflict"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"expected_count": 0},
        {"expected_count": 2, "covered_count": 3},
        {"expected_count": 2, "slot_ids": ("only-one",)},
    ],
)
def test_candidate_validation_rejects_inconsistent_coverage(kwargs: dict[str, object]) -> None:
    base = {
        "domain": "weather",
        "source_id": "kma_vilage_fcst",
        "slot_key": "invalid",
        "slot_ids": ("slot-a", "slot-b"),
        "scheduled_at": NOW - timedelta(hours=1),
        "deadline_at": NOW,
        "recovery_boundary": NOW - timedelta(days=7),
        "expected_count": 2,
        "covered_count": 2,
        "raw_manifest_verified": True,
        "historical_query_allowed": False,
    }
    base.update(kwargs)

    with pytest.raises(RecoveryPlannerError):
        RecoveryCandidate(**base)
