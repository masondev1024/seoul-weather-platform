import types
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import pytest

from weather_transform_test_support import (
    EXPECTED_DBT_PHASES,
    FakeAsset,
    FakePythonOperator,
    FakeTaskInstance,
    load_transform_module,
)
from weather_transform_test_support import (
    restore_airflow_modules_after_transform_import,  # noqa: F401
)


def test_weather_dbt_factory_preserves_phase_contracts():
    module = load_transform_module()
    expected_task_ids = tuple(
        task_id
        for task_id, _dbt_command, _selector, _include_vars in EXPECTED_DBT_PHASES
    )

    assert module.DBT_PHASE_TASK_IDS == expected_task_ids
    assert (
        tuple(
            (
                spec.task_id,
                spec.dbt_command,
                spec.selector,
                spec.include_project_vars,
            )
            for spec in module.DBT_PHASE_SPECS
        )
        == EXPECTED_DBT_PHASES
    )
    assert list(module.dbt_phase_tasks) == list(expected_task_ids)
    with pytest.raises(AttributeError):
        module.DBT_PHASE_SPECS[0].task_id = "mutated"
    for task_id, dbt_command, selector, include_project_vars in EXPECTED_DBT_PHASES:
        task = module.dag.task_dict[task_id]
        assert isinstance(task, FakePythonOperator)
        assert task.python_callable is module.run_dbt_phase
        assert task.kwargs["op_kwargs"] == {
            "dbt_command": dbt_command,
            "selector": selector,
            "include_project_vars": include_project_vars,
            "snapshot_task_id": module.SNAPSHOT_TASK_ID,
            "serving_as_of_task_id": module.SERVING_AS_OF_HOUR_TASK_ID,
            "threads": None if task_id == "dbt_deps" else 2,
        }
        if task_id == "dbt_deps":
            assert "pool" not in task.kwargs or task.kwargs["pool"] in (
                None,
                "default_pool",
            )
        else:
            assert task.kwargs["pool"] == module.TRINO_WEATHER_LEGACY_HEAVY_POOL
        assert task.kwargs["retries"] == 1
        assert task.kwargs["retry_delay"] == module.DBT_RETRY_DELAY
        assert task.kwargs["on_failure_callback"] is module.record_weather_problem


def test_weather_transform_failure_callback_uses_current_attempt_artifact_xcom():
    module = load_transform_module()

    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "dbt_run_results_xcom_key=WEATHER_DBT_RUN_RESULTS_XCOM_KEY" in source


def test_weather_transform_trino_tasks_do_not_inflate_pool_priority_from_chain():
    module = load_transform_module()

    assert module.TRINO_WEATHER_LEGACY_HEAVY_POOL == "trino_weather_legacy_heavy"
    for task_id in module.DBT_PHASE_TASK_IDS:
        task = module.dag.task_dict[task_id]
        if task_id == "dbt_deps":
            assert "pool" not in task.kwargs or task.kwargs["pool"] in (
                None,
                "default_pool",
            )
        else:
            assert task.kwargs["pool"] == module.TRINO_WEATHER_LEGACY_HEAVY_POOL
        assert task.kwargs["weight_rule"] == "absolute"


def test_weather_transform_runs_spatial_seed_and_mart_phases():
    module = load_transform_module()
    dag = module.dag

    expected_task_order = [
        "resolve_weather_snapshot_run",
        "resolve_weather_serving_as_of_hour",
        *(
            task_id
            for task_id, _dbt_command, _selector, _include_vars in EXPECTED_DBT_PHASES
        ),
    ]

    assert set(expected_task_order) <= set(dag.task_ids)
    for upstream_task_id, downstream_task_id in zip(
        expected_task_order, expected_task_order[1:]
    ):
        assert dag.task_dict[upstream_task_id].downstream_task_ids == {
            downstream_task_id
        }

    for task_id in (
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_coverage_grid_mart",
        "dbt_test_coverage_grid_mart",
    ):
        assert dag.task_dict[task_id].kwargs["on_failure_callback"] is module.record_weather_problem

    # 정적 참조 phase 는 weather_reference_data_refresh 로 빠졌으므로 transform 에는
    # 더 이상 존재하지 않는다.
    for reference_task_id in (
        "dbt_seed_asac_axes",
        "dbt_run_common_admin_dong_dimension",
        "dbt_seed_place_mapping",
        "dbt_seed_coverage_grid",
    ):
        assert reference_task_id not in dag.task_ids

    source = Path(module.__file__).read_text(encoding="utf-8")
    assert "silver_kma_vilage_fcst" not in source
    assert "gold_weather_forecast_summary" not in source
    assert "weather_place_grid_mapping" not in source
    assert "assert_silver_" not in source
    assert "assert_gold_" not in source


def test_weather_transform_freezes_one_kst_hour_before_all_dbt_phases():
    module = load_transform_module()
    anchor = module.dag.task_dict[module.SERVING_AS_OF_HOUR_TASK_ID]

    assert anchor.python_callable is module.resolve_weather_serving_as_of_hour
    assert anchor.upstream_task_ids == {module.SNAPSHOT_TASK_ID}
    assert anchor.downstream_task_ids == {"dbt_deps"}
    assert module.resolve_weather_serving_as_of_hour(
        now=datetime(2026, 8, 11, 10, 59, 59, tzinfo=ZoneInfo("Asia/Seoul"))
    ) == "2026-08-11 10:00:00"


def test_weather_transform_runs_spatial_marts_before_full_gold_and_metrics():
    module = load_transform_module()
    dag = module.dag

    terminal_order = (
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_place_mart",
        "dbt_test_place_mart",
        "dbt_run_coverage_grid_mart",
        "dbt_test_coverage_grid_mart",
        "dbt_run_gold",
        "dbt_test_gold",
        "mark_weather_gold_publication_ready",
        "publish_dbt_run_metrics",
    )

    for upstream_task_id, downstream_task_id in zip(
        terminal_order, terminal_order[1:]
    ):
        assert dag.task_dict[upstream_task_id].downstream_task_ids == {
            downstream_task_id
        }
    assert (
        dag.task_dict["dbt_run_gold"].kwargs["op_kwargs"]["selector"]
        == "ask_seoul_weather_transform_serving_gold"
    )
    assert (
        dag.task_dict["dbt_test_gold"].kwargs["op_kwargs"]["selector"]
        == "ask_seoul_weather_transform_serving_gold"
    )


def test_weather_gold_terminal_asset_is_emitted_only_after_gold_contracts():
    module = load_transform_module()
    dag = module.dag
    marker = dag.task_dict["mark_weather_gold_publication_ready"]

    assert dag.task_dict["dbt_test_gold"].downstream_task_ids == {
        "mark_weather_gold_publication_ready"
    }
    assert marker.kwargs["outlets"] == [
        module.WEATHER_GOLD_PUBLICATION_READY_ASSET_REF
    ]
    assert marker.downstream_task_ids == {"publish_dbt_run_metrics"}
    assert "outlets" not in dag.task_dict["publish_dbt_run_metrics"].kwargs


def test_weather_gold_terminal_asset_records_gold_and_bronze_run_identity():
    module = load_transform_module()
    outlet_event = types.SimpleNamespace(extra=None)
    ti = FakeTaskInstance(
        pulls={
            (module.SNAPSHOT_TASK_ID, None): "asset__weather-bronze-42",
            (module.SERVING_AS_OF_HOUR_TASK_ID, None): "2026-08-11 10:00:00",
        }
    )

    result = module.mark_weather_gold_publication_ready(
        ti=ti,
        run_id="asset_triggered__weather-gold-42",
        outlet_events={
            module.WEATHER_GOLD_PUBLICATION_READY_ASSET_REF: outlet_event
        },
        now=datetime(2026, 8, 11, 10, 59, 59, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert result == {
        "gold_dag_run_id": "asset_triggered__weather-gold-42",
        "bronze_dag_run_id": "asset__weather-bronze-42",
        "serving_as_of_hour": "2026-08-11 10:00:00",
    }
    assert outlet_event.extra == result


def test_weather_gold_terminal_asset_rejects_missing_bronze_identity():
    module = load_transform_module()

    with pytest.raises(module.AirflowFailException, match="Bronze snapshot"):
        module.mark_weather_gold_publication_ready(
            ti=FakeTaskInstance(),
            run_id="asset_triggered__weather-gold-42",
            outlet_events={
                module.WEATHER_GOLD_PUBLICATION_READY_ASSET_REF: (
                    types.SimpleNamespace(extra=None)
                )
            },
        )


def test_weather_gold_terminal_asset_skips_stale_serving_hour_without_emitting_asset():
    module = load_transform_module()
    outlet_event = types.SimpleNamespace(extra=None)
    ti = FakeTaskInstance(
        pulls={
            (module.SNAPSHOT_TASK_ID, None): "asset__weather-bronze-42",
            (module.SERVING_AS_OF_HOUR_TASK_ID, None): "2026-08-11 10:00:00",
        }
    )

    with pytest.raises(module.AirflowSkipException, match="stale serving hour"):
        module.mark_weather_gold_publication_ready(
            ti=ti,
            run_id="asset_triggered__weather-gold-42",
            outlet_events={
                module.WEATHER_GOLD_PUBLICATION_READY_ASSET_REF: outlet_event
            },
            now=datetime(2026, 8, 11, 11, 0, 0, tzinfo=ZoneInfo("Asia/Seoul")),
        )

    assert outlet_event.extra is None


def test_weather_serving_hour_state_rejects_missing_or_future_anchor():
    module = load_transform_module()
    now = datetime(2026, 8, 11, 10, 30, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    with pytest.raises(RuntimeError, match="valid frozen KST"):
        module.weather_serving_as_of_hour_state(ti=FakeTaskInstance(), now=now)

    future_ti = FakeTaskInstance(
        pulls={
            (module.SERVING_AS_OF_HOUR_TASK_ID, None): "2026-08-11 11:00:00"
        }
    )
    with pytest.raises(RuntimeError, match="in the future"):
        module.weather_serving_as_of_hour_state(ti=future_ti, now=now)


def test_weather_transform_passes_w2_canonical_revision_to_model_commands():
    module = load_transform_module()
    dag = module.dag
    # 정적 참조 phase 는 weather_reference_data_refresh 로 빠졌다.
    model_parsing_task_ids = (
        "dbt_source_freshness",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
        "dbt_run_place_mart",
        "dbt_test_place_mart",
        "dbt_run_coverage_grid_mart",
        "dbt_test_coverage_grid_mart",
    )

    for task_id in model_parsing_task_ids:
        assert (
            dag.task_dict[task_id].kwargs["op_kwargs"]["include_project_vars"] is True
        )
    assert (
        dag.task_dict["dbt_deps"].kwargs["op_kwargs"]["include_project_vars"] is False
    )


def test_weather_current_run_results_selects_latest_success_artifact(tmp_path):
    module = load_transform_module()
    earlier = tmp_path / "earlier" / "run_results.json"
    latest = tmp_path / "latest" / "run_results.json"
    failure = tmp_path / "failure" / "run_results.json"
    for path in (earlier, latest, failure):
        path.parent.mkdir()
        path.write_text("{}", encoding="utf-8")
    earlier_task = module.DBT_PHASE_TASK_IDS[-2]
    latest_task = module.DBT_PHASE_TASK_IDS[-1]
    ti = FakeTaskInstance(
        pulls={
            (earlier_task, None): {"run_results_path": str(earlier)},
            (latest_task, None): {"run_results_path": str(latest)},
            (latest_task, module.WEATHER_DBT_RUN_RESULTS_XCOM_KEY): str(failure),
        }
    )

    assert module._current_run_results_path(ti=ti) == str(latest)


def test_weather_current_run_results_falls_back_past_nonexistent_and_malformed_candidates(
    tmp_path,
):
    module = load_transform_module()
    existing = tmp_path / "existing" / "run_results.json"
    existing.parent.mkdir()
    existing.write_text("{}", encoding="utf-8")
    latest_task = module.DBT_PHASE_TASK_IDS[-1]
    malformed_task = module.DBT_PHASE_TASK_IDS[-2]
    fallback_task = module.DBT_PHASE_TASK_IDS[-3]
    ti = FakeTaskInstance(
        pulls={
            (latest_task, None): {
                "run_results_path": str(tmp_path / "missing" / "run_results.json")
            },
            (latest_task, module.WEATHER_DBT_RUN_RESULTS_XCOM_KEY): {"not": "a path"},
            (malformed_task, None): {"run_results_path": 7},
            (malformed_task, module.WEATHER_DBT_RUN_RESULTS_XCOM_KEY): [
                "also malformed"
            ],
            (fallback_task, None): {"run_results_path": str(existing)},
        }
    )

    assert module._current_run_results_path(task_instance=ti) == str(existing)


def test_weather_current_run_results_accepts_failure_artifact_xcom(tmp_path):
    module = load_transform_module()
    artifact = tmp_path / "failed" / "run_results.json"
    artifact.parent.mkdir()
    artifact.write_text("{}", encoding="utf-8")
    failed_task = module.DBT_PHASE_TASK_IDS[-4]
    ti = FakeTaskInstance(
        pulls={
            (failed_task, module.WEATHER_DBT_RUN_RESULTS_XCOM_KEY): str(artifact),
        }
    )

    assert module._current_run_results_path(ti=ti) == str(artifact)


def test_weather_implicit_metrics_never_reads_shared_run_results(tmp_path, monkeypatch):
    module = load_transform_module()
    shared = tmp_path / "target" / "run_results.json"
    shared.parent.mkdir()
    shared.write_text("{}", encoding="utf-8")

    def fail_if_published(*_args, **_kwargs):
        raise AssertionError("shared run_results.json must not be published")

    monkeypatch.setattr(module, "dump_dbt_run_results", fail_if_published)
    ti = FakeTaskInstance()

    assert module._current_run_results_path(ti=ti) is None
    assert module.publish_dbt_run_metrics(ti=ti, params={"target": "dev"}) == {
        "rows": 0,
        "skipped": True,
    }


def test_weather_transform_subscribes_to_bronze_asset_by_default():
    module = load_transform_module()

    assert module.dag.kwargs["schedule"] == [FakeAsset(module.WEATHER_BRONZE_ASSET)]


def test_weather_transform_names_common_admin_dong_failure_stage():
    module = load_transform_module()

    for task_id in (
        "dbt_run_common_admin_dong_dimension",
        "dbt_test_common_admin_dong_dimension",
    ):
        assert module.transform_stage_name(task_id) == "공용 행정동 차원 실행/검증"


def test_weather_transform_validates_dev_runtime_before_dbt():
    module = load_transform_module()
    guard = module.dag.task_dict["validate_dev_runtime"]

    assert guard.kwargs["op_kwargs"] == {
        "domain": "weather",
        "requested_target": "{{ params.target }}",
    }
    assert guard.downstream_task_ids == {"resolve_weather_snapshot_run"}


def test_weather_transform_limits_target_param_to_dev_or_prod():
    module = load_transform_module()

    target_param = module.DEFAULT_PARAMS["target"]

    assert target_param.schema["enum"] == ["dev", "prod"]
    assert target_param.value in {"dev", "prod"}


def test_weather_transform_target_param_default_follows_runtime_env(monkeypatch):
    monkeypatch.setenv("ASK_SEOUL_TARGET", "prod")
    monkeypatch.setenv("DBT_TARGET", "prod")

    module = load_transform_module()

    assert module.DEFAULT_PARAMS["target"].value == "prod"


def test_weather_transform_target_param_defaults_to_dev_without_runtime_env(monkeypatch):
    monkeypatch.delenv("ASK_SEOUL_TARGET", raising=False)
    monkeypatch.delenv("DBT_TARGET", raising=False)

    module = load_transform_module()

    assert module.DEFAULT_PARAMS["target"].value == "dev"


def test_weather_transform_publishes_dbt_run_metrics_as_non_gating_teardown():
    module = load_transform_module()
    dag = module.dag

    assert "publish_dbt_run_metrics" in dag.task_ids
    metrics = dag.task_dict["publish_dbt_run_metrics"]

    assert metrics.python_callable is module.publish_dbt_run_metrics
    assert metrics.is_teardown is True
    assert metrics.on_failure_fail_dagrun is False
    assert metrics.kwargs["trigger_rule"] == "all_done_setup_success"
    assert metrics.kwargs["on_failure_callback"] is module.record_weather_problem
    assert dag.task_dict["mark_weather_gold_publication_ready"].downstream_task_ids == {
        "publish_dbt_run_metrics"
    }
    assert metrics.downstream_task_ids == set()
    assert "fail_transform_if_upstream_failed" not in dag.task_ids
    assert "on_success_callback" not in dag.kwargs


def test_weather_transform_resolves_the_exact_triggering_snapshot(monkeypatch):
    module = load_transform_module()
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
            "event_at": "2026-07-15T12:00:00+09:00",
            "load_date": "2026-07-15",
            "row_count": 3,
            "payload_hash": "b" * 64,
            "is_publishable": True,
        }
    )

    assert module.resolve_weather_snapshot_run(
        triggering_asset_events={module.WEATHER_BRONZE_ASSET: [event]}
    ) == "weather-run-42"
    assert calls == ["weather-run-42"]


def test_weather_snapshot_resolver_rejects_missing_asset_event():
    module = load_transform_module()

    with pytest.raises(module.AirflowFailException, match="at least one"):
        module.resolve_weather_snapshot_run(triggering_asset_events={})


def test_weather_publish_dbt_run_metrics_forwards_domain_and_target(
    tmp_path, monkeypatch
):
    module = load_transform_module()
    run_results = tmp_path / "run_results.json"
    run_results.write_text("{}", encoding="utf-8")
    captured = {}

    def fake_dump(path, *, domain, target):
        captured.update(path=path, domain=domain, target=target)
        return [{}, {}]

    monkeypatch.setattr(module, "dump_dbt_run_results", fake_dump)

    assert module.publish_dbt_run_metrics(
        run_results_path=str(run_results), params={"target": "dev"}
    ) == {"rows": 2, "skipped": False}
    assert captured == {"path": str(run_results), "domain": "weather", "target": "dev"}
