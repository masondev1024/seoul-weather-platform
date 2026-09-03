from __future__ import annotations

import json
import subprocess
import sys
import types
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_dbt_execution_test_support import VALID_MANIFEST
from weather_transform_test_support import (
    FakeAsset,
    FakePythonOperator,
    FakeTaskInstance,
    load_transform_module,
)
from weather_transform_test_support import (  # noqa: F401
    restore_airflow_modules_after_transform_import,
)
from weather_quality_runtime import QualityWindowError


KST = ZoneInfo("Asia/Seoul")
QUALITY_TASK_IDS = {
    "validate_dev_runtime",
    "resolve_forecast_quality_window",
    "begin_forecast_quality_publication",
    "dbt_deps",
    "dbt_build_quality_candidate",
    "publish_forecast_quality_success",
}
TRINO_TASK_IDS = {
    "begin_forecast_quality_publication",
    "dbt_deps",
    "dbt_build_quality_candidate",
    "publish_forecast_quality_success",
}


def _module(filename):
    return load_transform_module(filename, module_name=f"{Path(filename).stem}_under_test")


@pytest.mark.parametrize(
    ("filename", "dag_id", "schedule"),
    [
        ("weather_forecast_quality_daily.py", "weather_forecast_quality_daily", None),
        ("weather_forecast_quality_backfill.py", "weather_forecast_quality_backfill", None),
    ],
)
def test_quality_dags_are_inert_paused_and_have_strict_topology(filename, dag_id, schedule):
    module = _module(filename)
    dag = module.dag

    assert dag.dag_id == dag_id
    assert dag.kwargs["schedule"] == schedule
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["is_paused_upon_creation"] is True
    assert dag.kwargs["dagrun_timeout"] <= timedelta(minutes=20)
    assert set(dag.task_ids) == QUALITY_TASK_IDS

    assert dag.task_dict["validate_dev_runtime"].downstream_task_ids == {
        "resolve_forecast_quality_window"
    }
    assert dag.task_dict["resolve_forecast_quality_window"].downstream_task_ids == {
        "begin_forecast_quality_publication"
    }
    assert dag.task_dict["resolve_forecast_quality_window"].kwargs["retries"] == 0
    assert dag.task_dict["begin_forecast_quality_publication"].downstream_task_ids == {
        "dbt_deps"
    }
    assert dag.task_dict["dbt_deps"].downstream_task_ids == {
        "dbt_build_quality_candidate"
    }
    assert dag.task_dict["dbt_build_quality_candidate"].downstream_task_ids == {
        "publish_forecast_quality_success"
    }


@pytest.mark.parametrize(
    "filename",
    ["weather_forecast_quality_daily.py", "weather_forecast_quality_backfill.py"],
)
def test_quality_dags_resource_limits_asset_and_selector_are_isolated(filename):
    module = _module(filename)
    dag = module.dag

    for task_id in TRINO_TASK_IDS:
        task = dag.task_dict[task_id]
        assert isinstance(task, FakePythonOperator)
        assert task.kwargs["pool"] == module.TRINO_WEATHER_HEAVY_POOL
        assert task.kwargs["pool_slots"] == 1
        assert task.kwargs["priority_weight"] == 10
        assert task.kwargs["execution_timeout"] <= timedelta(minutes=15)

    assert dag.task_dict["begin_forecast_quality_publication"].kwargs["retries"] == 0
    assert dag.task_dict["publish_forecast_quality_success"].kwargs["retries"] == 0
    assert dag.task_dict["dbt_deps"].kwargs["retries"] == 1
    assert dag.task_dict["dbt_build_quality_candidate"].kwargs["op_kwargs"] == {
        "dbt_command": "build",
        "selector": "ask_seoul_weather_quality_candidate",
        "include_project_vars": True,
        "threads": 2,
    }
    assert dag.task_dict["publish_forecast_quality_success"].kwargs["outlets"] == [
        module.QUALITY_READY_ASSET_REF
    ]
    assert module.WEATHER_FORECAST_QUALITY_READY_ASSET.endswith(
        "/forecast-quality-ready"
    )

    source = Path(__file__).resolve().parents[1].joinpath(filename).read_text(
        encoding="utf-8"
    )
    assert "WEATHER_GOLD_PUBLICATION_READY_ASSET" not in source
    assert "D1" not in source
    assert "KmaHttpAdapter" not in source
    assert "build_weather_landing" not in source
    assert "fetch_url" not in source


def test_existing_weather_writers_and_quality_dags_share_one_trino_heavy_pool():
    transform = load_transform_module(
        "weather_vilage_fcst_transform.py",
        module_name="weather_transform_pool_contract_under_test",
    )
    daily = _module("weather_forecast_quality_daily.py")
    backfill = _module("weather_forecast_quality_backfill.py")

    existing_writer_task = transform.dag.task_dict["dbt_run_silver"]
    daily_quality_task = daily.dag.task_dict["dbt_build_quality_candidate"]
    backfill_quality_task = backfill.dag.task_dict["dbt_build_quality_candidate"]

    assert transform.TRINO_WEATHER_LEGACY_HEAVY_POOL == daily.TRINO_WEATHER_HEAVY_POOL
    assert {
        existing_writer_task.kwargs["pool"],
        daily_quality_task.kwargs["pool"],
        backfill_quality_task.kwargs["pool"],
    } == {"trino_weather_heavy"}


def test_backfill_resolver_rejects_ranges_lists_current_future_and_requires_confirmation():
    module = _module("weather_forecast_quality_backfill.py")
    bad_params = [
        {"backfill_date": "2026-08-20/2026-08-21", "confirmation": "BACKFILL_ONE_KST_DATE"},
        {"backfill_date": ["2026-08-20"], "confirmation": "BACKFILL_ONE_KST_DATE"},
        {"backfill_date": "2026-08-22", "confirmation": "BACKFILL_ONE_KST_DATE"},
        {"backfill_date": "2026-08-20", "confirmation": ""},
    ]
    for params in bad_params:
        with pytest.raises(QualityWindowError):
            module.resolve_backfill_quality_window(
                backfill_date=params["backfill_date"],
                confirmation=params["confirmation"],
                now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
                run_id="manual__quality",
            )


def test_daily_window_payload_freezes_exactly_seven_completed_kst_dates():
    module = _module("weather_forecast_quality_daily.py")
    window = module.resolve_daily_quality_window(
        now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
        run_id="scheduled__quality",
    )
    payload = module.window_payload(window)

    assert payload["window_start_date"] == "2026-08-15"
    assert payload["window_end_date"] == "2026-08-21"
    assert (window.window_end_date - window.window_start_date).days + 1 == 7
    assert payload["dbt_vars"] == window.as_dbt_vars()


def test_daily_resolver_uses_fixed_airflow_context_time_across_midnight():
    module = _module("weather_forecast_quality_daily.py")

    payload = module.resolve_forecast_quality_window(
        run_id="scheduled__quality",
        data_interval_end=datetime(2026, 8, 22, 23, 59, tzinfo=KST),
    )
    retry_payload = module.resolve_forecast_quality_window(
        run_id="scheduled__quality",
        data_interval_end=datetime(2026, 8, 22, 23, 59, tzinfo=KST),
    )

    assert payload == retry_payload
    assert payload["window_start_date"] == "2026-08-15"
    assert payload["window_end_date"] == "2026-08-21"


def test_backfill_resolver_uses_fixed_airflow_context_time_for_current_date_gate():
    module = _module("weather_forecast_quality_backfill.py")

    payload = module.resolve_forecast_quality_window(
        run_id="manual__quality",
        params={
            "backfill_date": "2026-08-21",
            "confirmation": "BACKFILL_ONE_KST_DATE",
        },
        logical_date=datetime(2026, 8, 22, 0, 1, tzinfo=KST),
    )

    assert payload["window_start_date"] == "2026-08-21"
    assert payload["window_end_date"] == "2026-08-21"


def test_quality_asset_is_disconnected_from_existing_public_serving_asset():
    assets = Path(__file__).resolve().parents[3].joinpath("common/assets.py").read_text(
        encoding="utf-8"
    )

    assert (
        'WEATHER_FORECAST_QUALITY_READY_ASSET = "iceberg://weather/gold/forecast-quality-ready"'
        in assets
    )
    assert (
        'WEATHER_GOLD_PUBLICATION_READY_ASSET = "iceberg://weather/gold/publication-ready"'
        in assets
    )


def test_quality_dbt_uses_frozen_vars_and_only_allowlisted_env(tmp_path, monkeypatch):
    module = _module("weather_forecast_quality_daily.py")
    monkeypatch.setattr(module, "DBT_PROJECT", str(tmp_path / "weather"))
    window = module.resolve_daily_quality_window(
        now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
        run_id="scheduled__quality",
    )
    captured = []

    def fake_run(command, **kwargs):
        captured.append((command, kwargs))
        if command[1] == "ls":
            return types.SimpleNamespace(
                returncode=0,
                stdout='{"unique_id":"model.asac_seoul.quality","resource_type":"model"}\n',
                stderr="",
            )
        target_path = Path(command[command.index("--target-path") + 1])
        target_path.mkdir(parents=True, exist_ok=True)
        (target_path / "run_results.json").write_text("{}", encoding="utf-8")
        (target_path / "manifest.json").write_text(
            json.dumps(VALID_MANIFEST),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    ti = FakeTaskInstance(
        task_id="dbt_build_quality_candidate",
        pulls={(module.QUALITY_WINDOW_TASK_ID, None): module.window_payload(window)},
    )

    module.run_quality_dbt_phase(
        dbt_command="build",
        selector=module.QUALITY_SELECTOR,
        include_project_vars=True,
        threads=2,
        ti=ti,
        run_id="scheduled__quality",
        params={"target": "dev"},
    )

    command, kwargs = captured[-1]
    assert command[:2] == [module.DBT_BIN, "build"]
    assert command[command.index("--selector") + 1] == module.QUALITY_SELECTOR
    assert json.loads(command[command.index("--vars") + 1]) == {
        **module.WEATHER_DBT_CONTRACT_VARS,
        **window.as_dbt_vars(),
    }
    assert kwargs["env"]["TRINO_DBT_QUERY_MAX_RUN_TIME"] == "15m"
    assert "UNSAFE_ENV" not in kwargs["env"]


def test_runtime_rejects_non_allowlisted_quality_vars_or_env():
    from weather_dbt_runtime import run_weather_dbt_phase

    class Executor:
        def execute_dbt_phase(self, **_kwargs):
            raise AssertionError("must fail before executor")

    ti = FakeTaskInstance(task_id="dbt_build_quality_candidate")
    base = {
        "dbt_command": "build",
        "selector": "ask_seoul_weather_quality_candidate",
        "include_project_vars": True,
        "snapshot_task_id": None,
        "serving_as_of_task_id": None,
        "threads": 2,
        "context": {"ti": ti, "params": {"target": "dev"}, "run_id": "manual__x"},
        "dbt_executor": Executor(),
        "dbt_project": "/tmp/project",
        "dbt_bin": "dbt",
        "runner": subprocess.run,
        "pipeline": "weather-forecast-quality",
        "failure_exception": RuntimeError,
    }

    with pytest.raises(ValueError, match="project variable"):
        run_weather_dbt_phase(
            **base,
            extra_project_vars={"not_quality": "x"},
            allowed_extra_project_var_names=frozenset({"weather_quality_run_id"}),
        )
    with pytest.raises(ValueError, match="environment variable"):
        run_weather_dbt_phase(
            **base,
            extra_env={"DBT_ENV_SECRET": "x"},
            allowed_extra_env_names=frozenset({"TRINO_DBT_QUERY_MAX_RUN_TIME"}),
        )
    with pytest.raises(ValueError, match="unsupported value"):
        run_weather_dbt_phase(
            **base,
            extra_env={"TRINO_DBT_QUERY_MAX_RUN_TIME": "1h"},
            allowed_extra_env_names=frozenset({"TRINO_DBT_QUERY_MAX_RUN_TIME"}),
            expected_extra_env_values={"TRINO_DBT_QUERY_MAX_RUN_TIME": "15m"},
        )


def test_publication_success_and_failure_paths_use_frozen_window(monkeypatch):
    module = _module("weather_forecast_quality_daily.py")
    window = module.resolve_daily_quality_window(
        now=datetime(2026, 8, 22, 3, 5, tzinfo=KST),
        run_id="scheduled__quality",
    )
    calls = []
    cursor = object()
    target = module.QualityPublicationTarget()
    monkeypatch.setattr(
        module,
        "_quality_publication_cursor_and_target",
        lambda: (cursor, target),
    )
    monkeypatch.setattr(module, "record_weather_problem", lambda context: calls.append(("problem", context)))
    monkeypatch.setattr(
        module,
        "begin_quality_publication",
        lambda c, *, window, target, dag_id: calls.append(("begin", c, window, target, dag_id))
        or types.SimpleNamespace(evaluation_run_id=window.evaluation_run_id, status="RUNNING"),
    )
    monkeypatch.setattr(
        module,
        "publish_quality_success",
        lambda c, *, window, target, dag_id: calls.append(("success", c, window, target, dag_id))
        or types.SimpleNamespace(evaluation_run_id=window.evaluation_run_id, status="SUCCESS"),
    )
    monkeypatch.setattr(
        module,
        "record_failed_publication",
        lambda c, *, window, target, dag_id: calls.append(("failed", c, window, target, dag_id)),
    )
    context = {
        "ti": FakeTaskInstance(
            pulls={(module.QUALITY_WINDOW_TASK_ID, None): module.window_payload(window)}
        )
    }

    assert module.begin_forecast_quality_publication(**context) == {
        "evaluation_run_id": "scheduled__quality",
        "status": "RUNNING",
    }
    assert module.publish_forecast_quality_success(**context) == {
        "evaluation_run_id": "scheduled__quality",
        "status": "SUCCESS",
    }
    module.record_forecast_quality_failed_publication(context)

    assert [call[0] for call in calls] == ["begin", "success", "failed", "problem"]
    assert calls[0][2] == window
    assert calls[1][2] == window
    assert calls[2][2] == window
