import json
import types

from weather_transform_test_support import FakeTaskInstance, load_transform_module
from weather_transform_test_support import (
    restore_airflow_modules_after_transform_import,  # noqa: F401
)


def load_canonical_module():
    return load_transform_module(
        "weather_w2_canonical_transform.py",
        "weather_w2_canonical_transform_execution_under_test",
    )


def test_canonical_w2_phase_passes_pinned_snapshot_and_revision(monkeypatch):
    module = load_canonical_module()
    captured = {}
    run_results_path = "/tmp/weather-w2/run_results.json"

    def fake_execute_dbt_phase(**kwargs):
        captured.update(kwargs)
        return types.SimpleNamespace(
            existing_run_results_path=run_results_path,
            missing_expected_artifacts=False,
            attempts=(),
            completed=types.SimpleNamespace(returncode=0),
        )

    monkeypatch.setattr(module.weather_dbt, "execute_dbt_phase", fake_execute_dbt_phase)
    task_instance = FakeTaskInstance(
        task_id="dbt_run_w2_canonical_models",
        pulls={(module.SNAPSHOT_TASK_ID, None): "weather-run-42"},
    )

    result = module.run_dbt_phase(
        dbt_command="run",
        selector="ask_seoul_weather_w2_canonical_models",
        snapshot_task_id=module.SNAPSHOT_TASK_ID,
        threads=2,
        ti=task_instance,
        run_id="asset__weather-run-42",
        params={"target": "dev"},
    )

    assert captured["pipeline"] == "weather-w2-canonical-transform"
    assert captured["selector"] == "ask_seoul_weather_w2_canonical_models"
    assert captured["target"] == "dev"
    assert captured["threads"] == 2
    assert json.loads(captured["variables"]) == {
        "weather_w2_canonical_revision_date": "2025-04-01",
        "weather_snapshot_dag_run_id": "weather-run-42",
    }
    assert result["run_results_path"] == run_results_path
    assert task_instance.pushes == [
        (module.WEATHER_DBT_RUN_RESULTS_XCOM_KEY, run_results_path)
    ]
