from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from weather_transform_test_support import (
    FakeAsset,
    FakePythonOperator,
    load_transform_module,
    restore_airflow_modules_after_transform_import,  # noqa: F401
)


DAG_FILE = "weather_ultra_srt_ncst_bronze.py"


def _module():
    return load_transform_module(
        DAG_FILE,
        module_name="weather_ultra_srt_ncst_bronze_under_test",
    )


def test_observation_dag_is_inert_and_paused_by_default(monkeypatch):
    monkeypatch.delenv("ASK_SEOUL_KMA_OBSERVATION_DAG_SCHEDULE", raising=False)
    monkeypatch.delenv("ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED", raising=False)

    module = _module()
    dag = module.dag

    assert dag.dag_id == "weather_ultra_srt_ncst_bronze"
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["is_paused_upon_creation"] is True
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["dagrun_timeout"] == timedelta(minutes=40)
    assert dag.kwargs["start_date"].tzinfo.key == "Asia/Seoul"


def test_proposed_hourly_minute_45_schedule_is_only_an_explicit_override(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_KMA_OBSERVATION_DAG_SCHEDULE", "45 * * * *")

    module = _module()

    assert module.dag.kwargs["schedule"] == "45 * * * *"


def test_enabled_landing_uses_shared_api_pool_and_bounded_timeout(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED", "true")

    module = _module()
    land = module.dag.task_dict["land_observation_raw"]

    assert isinstance(land, FakePythonOperator)
    assert land.kwargs["pool"] == "kma_api_requests"
    assert land.kwargs["pool_slots"] == 1
    assert land.kwargs["execution_timeout"] <= timedelta(minutes=20)
    # R2 checkpoint writes are idempotent. A single short retry absorbs a
    # transient DNS/transport outage without widening the 40-minute run SLA.
    assert land.kwargs["retries"] == 1
    assert land.kwargs["retry_delay"] == timedelta(seconds=30)
    assert land.kwargs["retry_exponential_backoff"] is False


def test_load_and_verify_hold_the_exclusive_weather_pool_lock():
    module = _module()
    dag = module.dag

    for task_id in ("load_observation_bronze", "verify_observation_bronze"):
        task = dag.task_dict[task_id]
        assert task.kwargs["pool"] == "trino_weather_heavy"
        assert task.kwargs["pool_slots"] == 2
        assert task.kwargs["execution_timeout"] <= timedelta(minutes=10)
        assert task.kwargs["retries"] == 0


def test_task_timeouts_fit_inside_run_deadline_with_explicit_slack():
    module = _module()
    dag = module.dag

    total_task_budget = sum(
        (
            task.kwargs["execution_timeout"]
            for task in dag.task_dict.values()
        ),
        timedelta(),
    )

    assert total_task_budget <= timedelta(minutes=35)
    assert dag.kwargs["dagrun_timeout"] - total_task_budget >= timedelta(minutes=5)


def test_graph_publishes_asset_only_after_exact_verification():
    module = _module()
    dag = module.dag
    order = [
        "validate_observation_runtime",
        "plan_observation_collection",
        "land_observation_raw",
        "load_observation_bronze",
        "verify_observation_bronze",
        "publish_observation_bronze_asset",
    ]

    assert set(dag.task_ids) == set(order)
    for upstream, downstream in zip(order, order[1:]):
        assert dag.task_dict[upstream].downstream_task_ids == {downstream}
    publish = dag.task_dict["publish_observation_bronze_asset"]
    assert publish.kwargs["outlets"] == [
        FakeAsset("iceberg://weather/observation/bronze")
    ]
    assert publish.upstream_task_ids == {"verify_observation_bronze"}


def test_dag_has_no_quality_dbt_d1_or_serving_work_on_the_critical_path():
    module = _module()
    source = Path(module.__file__).read_text(encoding="utf-8").lower()

    assert "weather_quality" not in source
    assert "dbt" not in source
    assert "d1" not in source
    assert "serving" not in source


def test_plan_uses_exact_canonical_80_grids_and_eight_categories():
    module = _module()

    plan = module.plan_observation_collection(
        now=module.datetime(2026, 8, 22, 0, 45, tzinfo=module.timezone.utc)
    )

    assert plan["source_id"] == "kma_ultra_srt_ncst"
    assert plan["base_date"] == "20260822"
    assert plan["base_time"] == "0900"
    assert len(plan["grids"]) == 80
    assert len({(grid["nx"], grid["ny"]) for grid in plan["grids"]}) == 80
    assert plan["categories"] == ["PTY", "REH", "RN1", "T1H", "UUU", "VEC", "VVV", "WSD"]
    assert plan["expected_grid_count"] == 80
    assert plan["expected_row_count"] == 640


def test_backfill_plan_uses_airflow_logical_date_not_wall_clock():
    module = _module()

    plan = module.plan_observation_collection(
        logical_date=module.datetime(2026, 8, 23, 14, 45, tzinfo=module.timezone.utc)
    )

    assert plan["base_date"] == "20260823"
    assert plan["base_time"] == "2300"


def test_backfill_plan_prefers_canonical_run_id_timestamp():
    module = _module()

    plan = module.plan_observation_collection(
        run_id="scheduled__2026-08-23T14:45:00+00:00",
        logical_date=module.datetime(2026, 8, 24, 2, 10, tzinfo=module.timezone.utc),
    )

    assert plan["base_date"] == "20260823"
    assert plan["base_time"] == "2300"


def test_validate_runtime_fails_closed_until_guards_and_ledger_are_ready(monkeypatch):
    module = _module()
    monkeypatch.setattr(module, "validate_dev_runtime", lambda **_kwargs: None)
    monkeypatch.setattr(module, "shared_guards_enabled", lambda: False)

    with pytest.raises(module.WeatherBronzeConfigurationError, match="shared guards"):
        module.validate_observation_runtime()


def test_landing_wrapper_maps_plan_and_airflow_identity_to_runtime(monkeypatch):
    module = _module()
    captured = {}

    class Result:
        def to_xcom(self):
            return {"grid_count": 80, "row_count": 640, "is_publishable": True}

    class Landing:
        def collect(self, run, request):
            captured.update(run=run, request=request)
            return Result()

    class Ti:
        def xcom_pull(self, *, task_ids):
            assert task_ids == "plan_observation_collection"
            return {
                "base_date": "20260822",
                "base_time": "0900",
                "grids": [{"nx": 50 + index, "ny": 120} for index in range(80)],
            }

    monkeypatch.setattr(module, "build_observation_landing", lambda: Landing())

    result = module.land_observation_raw(
        ti=Ti(),
        dag=type("Dag", (), {"dag_id": "weather_ultra_srt_ncst_bronze"})(),
        run_id="scheduled__one",
    )

    assert result["row_count"] == 640
    assert captured["run"].run_id == "scheduled__one"
    assert len(captured["request"].grids) == 80


def test_publish_gate_requires_the_exact_verifier_result():
    module = _module()

    class Ti:
        def __init__(self, value):
            self.value = value

        def xcom_pull(self, *, task_ids):
            assert task_ids == "verify_observation_bronze"
            return self.value

    with pytest.raises(module.WeatherCompletenessError, match="640"):
        module.publish_observation_bronze_asset(ti=Ti(639), outlet_events={})


def test_verifier_uses_expected_source_revisions_not_the_airflow_run_id(monkeypatch):
    module = _module()
    captured = {}
    revisions = [
        {"grid_id": f"kma_{index}_120", "source_revision": f"revision-{index}"}
        for index in range(80)
    ]

    class Ti:
        def xcom_pull(self, *, task_ids):
            assert task_ids == "load_observation_bronze"
            return {
                "run_id": "scheduled__two",
                "observed_slot": "2026-08-22T09:00:00+09:00",
                "qualified_table": "iceberg.weather.bronze_kma_ultra_srt_ncst",
                "expected_grid_revisions": revisions,
            }

    monkeypatch.setattr(module, "trino_cursor", lambda: (object(), "iceberg", "weather"))

    def verify(cursor, **kwargs):
        captured.update(cursor=cursor, **kwargs)
        return 640

    monkeypatch.setattr(module, "verify_observation_bronze_run_slot", verify)

    assert module.verify_observation_bronze(ti=Ti()) == 640
    assert captured["expected_grid_revisions"] == revisions
    assert "dag_run_id" not in captured
