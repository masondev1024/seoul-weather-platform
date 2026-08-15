from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "weather_dbt_execution.py"
RAW_DBT = "/home/airflow/dbt-venv/bin/dbt"
DBT_OL = "/home/airflow/dbt-venv/bin/dbt-ol"
VALID_MANIFEST = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
        "dbt_version": "1.10.22",
        "generated_at": "2026-08-15T00:00:00Z",
        "invocation_id": "unit-test-invocation",
        "project_name": "asac_seoul",
        "adapter_type": "trino",
    },
    "nodes": {
        "model.asac_seoul.weather_test": {
            "unique_id": "model.asac_seoul.weather_test",
            "resource_type": "model",
            "name": "weather_test",
        }
    },
}


def load_execution_module():
    name = "weather_dbt_execution_under_test"
    domain_path = str(MODULE_PATH.parent)
    path_was_added = domain_path not in sys.path
    if path_was_added:
        sys.path.insert(0, domain_path)
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    missing = object()
    previous_module = sys.modules.get(name, missing)
    sys.modules[name] = module
    assert spec.loader is not None
    try:
        spec.loader.exec_module(module)
    finally:
        if previous_module is missing:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = previous_module
        if path_was_added:
            sys.path.remove(domain_path)
    return module


def completed(command, *, returncode=0, stdout="", stderr=""):
    return types.SimpleNamespace(
        args=command,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def option(command: list[str], name: str) -> str:
    return command[command.index(name) + 1]


def write_artifacts(command: list[str], *, sources: bool = False) -> None:
    target = Path(option(command, "--target-path"))
    target.mkdir(parents=True, exist_ok=True)
    (target / "manifest.json").write_text(
        json.dumps(VALID_MANIFEST, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    (target / ("sources.json" if sources else "run_results.json")).write_text(
        "{}", encoding="utf-8"
    )
