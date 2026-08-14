from __future__ import annotations

import ast
from pathlib import Path


WEATHER_DAG = Path(__file__).parents[1] / "weather_vilage_fcst_bronze.py"


def _function_call_names(path: Path, name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    function = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name
    )
    return {
        node.func.id
        for node in ast.walk(function)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def test_weather_bronze_failure_recorder_leaves_discord_delivery_to_common_callback():
    calls = _function_call_names(WEATHER_DAG, "record_and_notify_kma_run_failed")

    assert "record_kma_run_failed" in calls
    assert "notify_weather_bronze_failure" not in calls
