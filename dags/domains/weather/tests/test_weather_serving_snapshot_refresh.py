import types
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from weather_transform_test_support import (
    FakeAsset,
    FakePythonOperator,
    FakeTaskInstance,
    load_transform_module,
)
from weather_transform_test_support import (  # noqa: F401
    restore_airflow_modules_after_transform_import,
)


REFRESH_DBT_TASK_IDS = (
    "dbt_run_serving_snapshot_refresh",
    "dbt_test_serving_snapshot_refresh",
)


def _module():
    return load_transform_module(
        "weather_serving_snapshot_refresh.py",
        module_name="weather_serving_snapshot_refresh_under_test",
    )


def test_hourly_snapshot_refresh_runs_only_public_weather_serving_selector():
    module = _module()
    dag = module.dag

    assert dag.dag_id == "weather_serving_snapshot_refresh"
    assert dag.kwargs["schedule"] == "0 * * * *"
    assert dag.kwargs["max_active_runs"] == 1
    assert set(dag.task_ids) == {
        "validate_dev_runtime",
        # 공개 Gold 를 함께 쓰는 transform 과의 상호배제 가드.
        "guard_conflicting_weather_transform",
        "resolve_weather_serving_as_of_hour",
        *REFRESH_DBT_TASK_IDS,
        "mark_weather_serving_snapshot_ready",
        "publish_dbt_run_metrics",
    }

    assert dag.task_dict["validate_dev_runtime"].kwargs["op_kwargs"] == {
        "domain": "weather",
        "requested_target": "{{ params.target }}",
    }
    for task_id, dbt_command in zip(REFRESH_DBT_TASK_IDS, ("run", "test")):
        task = dag.task_dict[task_id]
        assert isinstance(task, FakePythonOperator)
        assert task.python_callable is module.run_dbt_phase
        assert task.kwargs["op_kwargs"] == {
            "dbt_command": dbt_command,
            "selector": "ask_seoul_weather_serving_snapshot_refresh",
            "include_project_vars": True,
            "snapshot_task_id": None,
            "serving_as_of_task_id": module.SERVING_AS_OF_HOUR_TASK_ID,
            "threads": 2,
        }
        assert task.kwargs["pool"] == module.TRINO_WEATHER_LEGACY_HEAVY_POOL
        assert task.kwargs["weight_rule"] == "absolute"
        assert task.kwargs["priority_weight"] == module.SERVING_SNAPSHOT_PRIORITY_WEIGHT
        assert task.kwargs["retries"] == 1
        assert task.kwargs["retry_delay"] == module.DBT_RETRY_DELAY
        assert task.kwargs["on_failure_callback"] is module.record_weather_problem


def test_hourly_snapshot_refresh_marks_the_existing_publication_asset_without_bronze_identity():
    module = _module()
    dag = module.dag
    marker = dag.task_dict["mark_weather_serving_snapshot_ready"]

    assert marker.kwargs["outlets"] == [module.WEATHER_GOLD_PUBLICATION_READY_ASSET_REF]
    assert marker.upstream_task_ids == {"dbt_test_serving_snapshot_refresh"}
    assert marker.downstream_task_ids == {"publish_dbt_run_metrics"}

    outlet_event = types.SimpleNamespace(extra=None)
    ti = FakeTaskInstance(
        pulls={
            (module.SERVING_AS_OF_HOUR_TASK_ID, None): "2026-08-11 10:00:00"
        }
    )
    metadata = module.mark_weather_serving_snapshot_ready(
        ti=ti,
        run_id="scheduled__2026-08-11T00:00:00+00:00",
        outlet_events={module.WEATHER_GOLD_PUBLICATION_READY_ASSET_REF: outlet_event},
        now=datetime(2026, 8, 11, 10, 59, 59, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert metadata == {
        "gold_dag_run_id": "scheduled__2026-08-11T00:00:00+00:00",
        "refresh_kind": "hourly_serving_snapshot",
        "serving_as_of_hour": "2026-08-11 10:00:00",
    }
    assert outlet_event.extra == metadata
    assert "bronze_dag_run_id" not in metadata


def test_hourly_snapshot_refresh_fails_closed_when_frozen_hour_is_stale():
    module = _module()
    outlet_event = types.SimpleNamespace(extra=None)
    ti = FakeTaskInstance(
        pulls={
            (module.SERVING_AS_OF_HOUR_TASK_ID, None): "2026-08-11 10:00:00"
        }
    )

    with pytest.raises(module.AirflowFailException, match="frozen hour"):
        module.mark_weather_serving_snapshot_ready(
            ti=ti,
            run_id="scheduled__2026-08-11T00:00:00+00:00",
            outlet_events={
                module.WEATHER_GOLD_PUBLICATION_READY_ASSET_REF: outlet_event
            },
            now=datetime(2026, 8, 11, 11, 0, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        )

    assert outlet_event.extra is None


def test_hourly_snapshot_refresh_freezes_one_kst_hour_for_run_and_test():
    module = _module()
    dag = module.dag
    anchor = dag.task_dict[module.SERVING_AS_OF_HOUR_TASK_ID]

    assert anchor.python_callable is module.resolve_weather_serving_as_of_hour
    assert anchor.upstream_task_ids == {"guard_conflicting_weather_transform"}
    assert anchor.downstream_task_ids == {"dbt_run_serving_snapshot_refresh"}
    assert module.resolve_weather_serving_as_of_hour(
        now=datetime(2026, 8, 11, 10, 59, 59, tzinfo=ZoneInfo("Asia/Seoul"))
    ) == "2026-08-11 10:00:00"


def test_hourly_snapshot_refresh_has_no_source_collection_or_bronze_snapshot_pin():
    module = _module()
    source = open(module.__file__, encoding="utf-8").read()

    assert "resolve_weather_snapshot_run" not in source
    assert "WEATHER_BRONZE_ASSET" not in source
    assert '"source freshness"' not in source
    assert "weather_vilage_fcst_transform" not in source
    assert "ask_seoul_weather_serving_snapshot_refresh" in source


def test_hourly_snapshot_refresh_publishes_its_dbt_metrics_as_non_gating_teardown(tmp_path, monkeypatch):
    module = _module()
    run_results = tmp_path / "run_results.json"
    run_results.write_text("{}", encoding="utf-8")
    captured = {}

    monkeypatch.setattr(
        module,
        "dump_dbt_run_results",
        lambda path, *, domain, target: captured.update(path=path, domain=domain, target=target) or [{}, {}],
    )
    ti = FakeTaskInstance(
        task_id="publish_dbt_run_metrics",
        pulls={
            ("dbt_test_serving_snapshot_refresh", None): {
                "run_results_path": str(run_results)
            }
        },
    )

    assert module.publish_dbt_run_metrics(ti=ti, params={"target": "dev"}) == {
        "rows": 2,
        "skipped": False,
    }
    assert captured == {"path": str(run_results), "domain": "weather", "target": "dev"}
    metrics = module.dag.task_dict["publish_dbt_run_metrics"]
    assert metrics.is_teardown is True
    assert metrics.on_failure_fail_dagrun is False


def test_hourly_snapshot_refresh_uses_the_shared_weather_publication_asset_reference():
    module = _module()

    assert module.WEATHER_GOLD_PUBLICATION_READY_ASSET_REF == FakeAsset(
        "iceberg://weather/gold/publication-ready"
    )
