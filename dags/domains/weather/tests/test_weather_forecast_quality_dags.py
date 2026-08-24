from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from weather_transform_test_support import (  # noqa: F401
    FakeAsset,
    FakePythonOperator,
    FakeTaskInstance,
    load_transform_module,
    restore_airflow_modules_after_transform_import,
)


QUALITY_TASK_IDS = {
    "validate_dev_runtime",
    "resolve_quality_window",
    "dbt_deps",
    "dbt_build_quality_candidate",
    "publish_quality_manifest",
    "dbt_build_quality_published",
    "mark_weather_forecast_quality_ready",
}


def _daily():
    return load_transform_module(
        "weather_forecast_quality_daily.py",
        module_name="weather_forecast_quality_daily_under_test",
    )


def _backfill():
    return load_transform_module(
        "weather_forecast_quality_backfill.py",
        module_name="weather_forecast_quality_backfill_under_test",
    )


def test_daily_quality_dag_is_paused_unscheduled_and_bound_to_the_internal_lane(monkeypatch):
    monkeypatch.delenv("ASK_SEOUL_WEATHER_QUALITY_DAG_SCHEDULE", raising=False)
    module = _daily()
    dag = module.dag

    assert dag.dag_id == "weather_forecast_quality_daily"
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["is_paused_upon_creation"] is True
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["dagrun_timeout"] == timedelta(minutes=45)
    assert set(dag.task_ids) == QUALITY_TASK_IDS

    for task_id in (
        "dbt_build_quality_candidate",
        "publish_quality_manifest",
        "dbt_build_quality_published",
    ):
        task = dag.task_dict[task_id]
        assert isinstance(task, FakePythonOperator)
        assert task.kwargs["pool"] == "trino_weather_legacy_heavy"
        assert task.kwargs["priority_weight"] == 10
        assert task.kwargs["weight_rule"] == "absolute"
        assert task.kwargs["execution_timeout"] <= timedelta(minutes=15)

    for task_id in ("dbt_build_quality_candidate", "dbt_build_quality_published"):
        task = dag.task_dict[task_id]
        assert task.kwargs["retries"] == 1
        assert task.kwargs["retry_exponential_backoff"] is True
        assert task.kwargs["max_retry_delay"] == timedelta(minutes=5)
        assert task.kwargs["op_kwargs"]["dbt_command"] == "build"


def test_backfill_quality_dag_is_manual_single_date_and_paused():
    module = _backfill()
    dag = module.dag

    assert dag.dag_id == "weather_forecast_quality_backfill"
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["is_paused_upon_creation"] is True
    assert {"target", "backfill_date", "confirmation"} == set(dag.kwargs["params"])


def test_quality_dags_have_a_strict_linear_publication_gate_without_serving_dependencies():
    module = _daily()
    dag = module.dag
    ordered = (
        "validate_dev_runtime",
        "resolve_quality_window",
        "dbt_deps",
        "dbt_build_quality_candidate",
        "publish_quality_manifest",
        "dbt_build_quality_published",
        "mark_weather_forecast_quality_ready",
    )
    for upstream, downstream in zip(ordered, ordered[1:]):
        assert dag.task_dict[upstream].downstream_task_ids == {downstream}

    marker = dag.task_dict["mark_weather_forecast_quality_ready"]
    assert marker.kwargs["outlets"] == [
        FakeAsset("iceberg://weather/gold/forecast-quality-ready")
    ]
    source = open(module.__file__, encoding="utf-8").read()
    assert "D1" not in source
    assert "Worker" not in source
    assert "WEATHER_GOLD_PUBLICATION_READY_ASSET" not in source
    assert "weather_serving" not in source
    assert "from airflow import DAG" in source


def test_daily_window_and_internal_asset_metadata_are_frozen_from_one_xcom_contract():
    module = _daily()
    dag = module.dag
    resolve = dag.task_dict["resolve_quality_window"]
    vars = resolve.python_callable(
        now=datetime(2026, 8, 22, 3, 5, tzinfo=ZoneInfo("Asia/Seoul")),
        run_id="scheduled__quality",
    )
    marker = dag.task_dict["mark_weather_forecast_quality_ready"]
    outlet_event = type("Outlet", (), {"extra": None})()
    metadata = marker.python_callable(
        ti=FakeTaskInstance(pulls={("resolve_quality_window", None): vars}),
        run_id="scheduled__quality",
        outlet_events={marker.kwargs["outlets"][0]: outlet_event},
    )

    assert metadata == {
        "quality_dag_run_id": "scheduled__quality",
        "evaluation_run_id": "scheduled__quality",
    }
    assert outlet_event.extra == metadata
