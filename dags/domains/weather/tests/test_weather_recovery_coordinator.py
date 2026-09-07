from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import weather_recovery_coordinator as module  # noqa: E402


def test_coordinator_is_paused_and_unscheduled_by_default() -> None:
    assert module.dag.is_paused_upon_creation is True
    assert module.dag.catchup is False
    assert module.dag.max_active_runs == 1
    assert module.dag.dagrun_timeout.total_seconds() == 5 * 60
    schedule = getattr(module.dag, "schedule", None)
    if schedule is None:
        schedule = getattr(module.dag, "schedule_interval", None)
    assert schedule is None
    assert module.dag.task_ids == [module.TASK_ID]


def test_coordinator_policy_defaults_are_bounded(monkeypatch) -> None:
    for name in (
        module._MAX_JOBS_ENV,
        module._MAX_API_JOBS_ENV,
        module._MAX_AGE_HOURS_ENV,
    ):
        monkeypatch.delenv(name, raising=False)

    policy = module.recovery_policy_from_environment()

    assert policy.max_jobs == 3
    assert policy.max_api_jobs == 1
    assert policy.max_recovery_age.total_seconds() == 24 * 60 * 60


def test_coordinator_task_only_reads_and_emits_a_plan(monkeypatch) -> None:
    monkeypatch.setattr(module, "build_weather_collection_slot_storage", lambda: object())
    monkeypatch.setattr(
        module,
        "read_weather_recovery_candidates",
        lambda _storage, *, now: (),
    )

    payload = module.plan_weather_recovery()

    assert payload["status"] == "empty"
    assert payload["mutation_performed"] is False
    assert payload["metrics"]["candidate_count"] == 0


def test_coordinator_source_has_no_pipeline_mutation_commands() -> None:
    source = Path(module.__file__).read_text(encoding="utf-8").lower()

    for forbidden in (
        "airflow dags trigger",
        "airflow dags unpause",
        "airflow dags backfill",
        "put_object(",
        "delete_object(",
    ):
        assert forbidden not in source
