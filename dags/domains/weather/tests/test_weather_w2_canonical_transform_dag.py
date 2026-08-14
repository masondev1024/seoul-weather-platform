import types
from pathlib import Path

import pytest

from weather_transform_test_support import (
    FakeAsset,
    FakePythonOperator,
    FakeTaskInstance,
    load_transform_module,
)
from weather_transform_test_support import (
    restore_airflow_modules_after_transform_import,  # noqa: F401
)


CANONICAL_PHASES = (
    ("dbt_deps", "deps", None, False),
    (
        "dbt_run_w2_canonical_models",
        "run",
        "ask_seoul_weather_w2_canonical_models",
        True,
    ),
    (
        "dbt_test_w2_canonical_contracts",
        "test",
        "ask_seoul_weather_w2_canonical_contracts",
        True,
    ),
)


def load_canonical_module():
    return load_transform_module(
        "weather_w2_canonical_transform.py",
        "weather_w2_canonical_transform_under_test",
    )


def test_canonical_w2_dag_has_a_small_independent_phase_chain():
    module = load_canonical_module()

    assert module.DAG_ID == "weather_w2_canonical_transform"
    assert module.TRINO_HEAVY_POOL == "trino_weather_heavy"
    assert module.DBT_PIPELINE == "weather-w2-canonical-transform"
    assert tuple(
        (
            spec.task_id,
            spec.dbt_command,
            spec.selector,
            spec.include_project_vars,
        )
        for spec in module.DBT_PHASE_SPECS
    ) == CANONICAL_PHASES
    assert module.DBT_PHASE_TASK_IDS == tuple(phase[0] for phase in CANONICAL_PHASES)
    assert module.dag.kwargs["schedule"] == [FakeAsset(module.WEATHER_BRONZE_ASSET)]
    assert module.dag.kwargs["max_active_runs"] == 1
    assert module.dag.kwargs["is_paused_upon_creation"] is True
    assert module.DEFAULT_PARAMS["target"].schema["enum"] == ["dev", "prod"]

    expected_order = [
        "validate_dev_runtime",
        module.SNAPSHOT_TASK_ID,
        *(phase[0] for phase in CANONICAL_PHASES),
        "publish_dbt_run_metrics",
    ]
    for upstream, downstream in zip(expected_order, expected_order[1:]):
        assert module.dag.task_dict[upstream].downstream_task_ids == {downstream}

    for task_id, command, selector, include_vars in CANONICAL_PHASES:
        task = module.dag.task_dict[task_id]
        assert isinstance(task, FakePythonOperator)
        assert task.python_callable is module.run_dbt_phase
        assert task.kwargs["op_kwargs"] == {
            "dbt_command": command,
            "selector": selector,
            "include_project_vars": include_vars,
            "snapshot_task_id": module.SNAPSHOT_TASK_ID,
            "threads": None if task_id == "dbt_deps" else 2,
        }
        if task_id == "dbt_deps":
            assert "pool" not in task.kwargs or task.kwargs["pool"] in (
                None,
                "default_pool",
            )
        else:
            assert task.kwargs["pool"] == module.TRINO_HEAVY_POOL
        assert task.kwargs["weight_rule"] == "absolute"
        if task_id == module.DBT_PHASE_TASK_IDS[-1]:
            assert task.kwargs["on_failure_callback"] == [
                module.record_weather_problem,
                module.record_weather_gold_product_failure,
            ]
        else:
            assert task.kwargs["on_failure_callback"] is module.record_weather_problem

    metrics = module.dag.task_dict["publish_dbt_run_metrics"]
    assert metrics.is_teardown is True
    assert metrics.on_failure_fail_dagrun is False
    assert metrics.kwargs["on_failure_callback"] is module.record_weather_problem


def test_canonical_w2_failure_callback_uses_current_attempt_artifact_xcom():
    module = load_canonical_module()

    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "dbt_run_results_xcom_key=WEATHER_DBT_RUN_RESULTS_XCOM_KEY" in source


def test_canonical_w2_dag_pins_a_publishable_bronze_snapshot(monkeypatch):
    module = load_canonical_module()
    calls = []

    class Manifest:
        def require_publishable(self, run_id):
            calls.append(run_id)
            return run_id

    monkeypatch.setattr(module, "build_weather_manifest", lambda: Manifest())
    event = types.SimpleNamespace(
        extra={
            "source_id": "kma_vilage_fcst",
            "bronze_run_id": "weather-run-42",
            "bronze_dag_run_id": "weather-run-42",
            "event_at": "2026-07-17T09:00:00+09:00",
            "load_date": "2026-07-17",
            "row_count": 3,
            "payload_hash": "b" * 64,
            "is_publishable": True,
        }
    )

    assert module.resolve_weather_snapshot_run(
        triggering_asset_events={module.WEATHER_BRONZE_ASSET: [event]}
    ) == "weather-run-42"
    assert calls == ["weather-run-42"]


def test_canonical_w2_dag_does_not_own_guarded_w1_resources():
    module = load_canonical_module()
    source = Path(module.__file__).read_text(encoding="utf-8")

    assert "weather_admin_dong_grid_bridge_history" not in source
    assert "bridge_weather_admin_dong_grid" not in source
    assert "dbt_seed" not in source
    assert "dbt_run_w1" not in source


def test_canonical_w2_dag_rejects_missing_bronze_asset_event():
    module = load_canonical_module()

    with pytest.raises(module.AirflowFailException, match="at least one"):
        module.resolve_weather_snapshot_run(triggering_asset_events={})


def _valid_bronze_event():
    return types.SimpleNamespace(
        extra={
            "source_id": "kma_vilage_fcst",
            "bronze_run_id": "weather-run-42",
            "bronze_dag_run_id": "weather-run-42",
            "event_at": "2026-07-17T09:00:00+09:00",
            "load_date": "2026-07-17",
            "row_count": 3,
            "payload_hash": "b" * 64,
            "is_publishable": True,
        }
    )


def test_resolve_weather_snapshot_run_pins_admin_dong_crosswalk_snapshot(monkeypatch):
    module = load_canonical_module()

    class Manifest:
        def require_publishable(self, run_id):
            return run_id

    monkeypatch.setattr(module, "build_weather_manifest", lambda: Manifest())
    monkeypatch.setattr(
        module,
        "resolve_admin_dong_crosswalk_snapshot_id",
        lambda: 8738321387624398062,
    )
    ti = FakeTaskInstance(task_id=module.SNAPSHOT_TASK_ID)

    run_id = module.resolve_weather_snapshot_run(
        triggering_asset_events={module.WEATHER_BRONZE_ASSET: [_valid_bronze_event()]},
        ti=ti,
    )

    assert run_id == "weather-run-42"
    assert (
        module.ADMIN_DONG_CROSSWALK_PIN_XCOM_KEY,
        8738321387624398062,
    ) in ti.pushes


def test_resolve_weather_snapshot_run_fails_closed_when_crosswalk_snapshot_unavailable(
    monkeypatch,
):
    module = load_canonical_module()

    class Manifest:
        def require_publishable(self, run_id):
            return run_id

    monkeypatch.setattr(module, "build_weather_manifest", lambda: Manifest())

    def _raise():
        raise module.AdminDongCrosswalkSnapshotUnavailableError(
            "admin_dong crosswalk Iceberg snapshot is unavailable"
        )

    monkeypatch.setattr(module, "resolve_admin_dong_crosswalk_snapshot_id", _raise)
    ti = FakeTaskInstance(task_id=module.SNAPSHOT_TASK_ID)

    with pytest.raises(module.AirflowFailException, match="unavailable"):
        module.resolve_weather_snapshot_run(
            triggering_asset_events={module.WEATHER_BRONZE_ASSET: [_valid_bronze_event()]},
            ti=ti,
        )
