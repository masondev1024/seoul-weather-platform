from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_dbt_runtime import (  # noqa: E402
    WEATHER_DBT_CONTRACT_VARS,
    WEATHER_DBT_RUN_RESULTS_XCOM_KEY,
    run_weather_dbt_phase,
)


class FakeTaskInstance:
    task_id = "dbt_build_quality_candidate"
    try_number = 1

    def __init__(self) -> None:
        self.pushed: list[tuple[str, object]] = []

    def xcom_pull(self, **_kwargs):
        return None

    def xcom_push(self, *, key: str, value: object) -> None:
        self.pushed.append((key, value))


class FakeExecutor:
    def __init__(self) -> None:
        self.kwargs: dict[str, object] | None = None

    def execute_dbt_phase(self, **kwargs):
        self.kwargs = kwargs
        completed = SimpleNamespace(returncode=0, stdout="", stderr="")
        return SimpleNamespace(
            existing_run_results_path="/tmp/run_results.json",
            attempts=(completed,),
            completed=completed,
            missing_expected_artifacts=(),
            existing_sources_path="/tmp/sources.json",
            existing_manifest_path="/tmp/manifest.json",
            selected_unique_ids=("model.asac_seoul.quality",),
        )


def test_quality_dbt_phase_carries_the_immutable_window_and_15_minute_session_limit():
    ti = FakeTaskInstance()
    executor = FakeExecutor()
    result = run_weather_dbt_phase(
        dbt_command="build",
        selector="ask_seoul_weather_quality_candidate",
        include_project_vars=True,
        snapshot_task_id=None,
        serving_as_of_task_id=None,
        threads=2,
        context={"ti": ti, "run_id": "scheduled__quality", "params": {"target": "dev"}},
        dbt_executor=executor,
        dbt_project="/tmp/project",
        dbt_bin="dbt",
        runner=lambda *_args, **_kwargs: None,
        pipeline="weather-forecast-quality",
        failure_exception=lambda _retryable, message: RuntimeError(message),
        additional_variables={"weather_quality_run_id": "scheduled__quality"},
        environment_overrides={"TRINO_DBT_QUERY_MAX_RUN_TIME": "15m"},
    )

    assert executor.kwargs is not None
    variables = json.loads(executor.kwargs["variables"])
    assert variables == WEATHER_DBT_CONTRACT_VARS | {
        "weather_quality_run_id": "scheduled__quality"
    }
    assert executor.kwargs["environ"]["TRINO_DBT_QUERY_MAX_RUN_TIME"] == "15m"
    assert ti.pushed == [(WEATHER_DBT_RUN_RESULTS_XCOM_KEY, "/tmp/run_results.json")]
    assert result["status"] == "success"
