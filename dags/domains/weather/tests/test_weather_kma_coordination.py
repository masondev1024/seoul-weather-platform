from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


DOMAIN_ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = DOMAIN_ROOT.parents[1]
REPOSITORY_ROOT = DAGS_ROOT.parent
sys.path.insert(0, str(DOMAIN_ROOT))
sys.path.insert(0, str(DAGS_ROOT))

from weather_transform_test_support import (
    load_transform_module,
    restore_airflow_modules_after_transform_import,  # noqa: F401
)


def coordination_module():
    return importlib.import_module("weather_ingest.kma_coordination")


@pytest.mark.parametrize("value", [None, "", "0", "false", "FALSE", "no", "off"])
def test_disabled_rollout_preserves_existing_runtime(monkeypatch, value):
    """Catches a default-on flag that would mutate the hot-mounted forecast DAG."""
    if value is None:
        monkeypatch.delenv("ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED", raising=False)
    else:
        monkeypatch.setenv("ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED", value)

    module = coordination_module()

    assert module.shared_guards_enabled() is False
    assert module.kma_api_pool_kwargs() == {}
    assert (
        module.weather_heavy_pool("trino_weather_legacy_heavy")
        == "trino_weather_legacy_heavy"
    )
    assert module.weather_heavy_pool_kwargs(
        "trino_weather_legacy_heavy", pool_slots=2
    ) == {
        "pool": "trino_weather_legacy_heavy",
        "pool_slots": 1,
    }


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_enabled_rollout_serializes_api_and_weather_work(monkeypatch, value):
    """Catches enabled mode selecting separate API or legacy Trino pools."""
    monkeypatch.setenv("ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED", value)

    module = coordination_module()

    assert module.shared_guards_enabled() is True
    assert module.kma_api_pool_kwargs() == {
        "pool": "kma_api_requests",
        "pool_slots": 1,
    }
    assert (
        module.weather_heavy_pool("trino_weather_legacy_heavy")
        == "trino_weather_heavy"
    )
    assert module.weather_heavy_pool_kwargs(
        "trino_weather_legacy_heavy", pool_slots=1
    ) == {
        "pool": "trino_weather_heavy",
        "pool_slots": 1,
    }
    assert module.weather_heavy_pool_kwargs(
        "trino_weather_legacy_heavy", pool_slots=2
    ) == {
        "pool": "trino_weather_heavy",
        "pool_slots": 2,
    }


def test_invalid_rollout_flag_fails_closed(monkeypatch):
    """Catches a typo silently choosing either rollout branch."""
    monkeypatch.setenv("ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED", "enabled-ish")

    module = coordination_module()

    with pytest.raises(module.KmaCoordinationConfigurationError, match="boolean"):
        module.shared_guards_enabled()


def test_forecast_dag_preserves_default_pool_when_rollout_is_disabled(monkeypatch):
    """Catches accidental scheduling changes before the opt-in rollout."""
    monkeypatch.delenv("ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED", raising=False)

    assert _forecast_landing_pools() == [
        ["weather_vilage_fcst_bronze", "default_pool", 1],
        ["weather_vilage_fcst_recollect", "default_pool", 1],
    ]


def test_forecast_dags_share_one_api_pool_when_rollout_is_enabled(monkeypatch):
    """Catches the forecast side bypassing observation request serialization."""
    monkeypatch.setenv("ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED", "true")

    assert _forecast_landing_pools() == [
        ["weather_vilage_fcst_bronze", "kma_api_requests", 1],
        ["weather_vilage_fcst_recollect", "kma_api_requests", 1],
    ]


@pytest.mark.parametrize(
    ("filename", "module_name", "task_id"),
    [
        (
            "weather_vilage_fcst_transform.py",
            "transform_shared_pool_enabled",
            "dbt_run_silver",
        ),
        (
            "weather_reference_data_refresh.py",
            "reference_shared_pool_enabled",
            "dbt_seed_asac_axes",
        ),
        (
            "weather_serving_snapshot_refresh.py",
            "serving_shared_pool_enabled",
            "dbt_run_serving_snapshot_refresh",
        ),
        (
            "weather_iceberg_maintenance.py",
            "maintenance_shared_pool_enabled",
            "weather_traffic_bronze__bronze_kma_vilage_fcst__optimize",
        ),
    ],
)
def test_enabled_rollout_moves_existing_trino_dags_to_one_weather_pool(
    monkeypatch,
    filename,
    module_name,
    task_id,
):
    """Catches a legacy Weather DAG escaping the 5-GiB Trino concurrency guard."""
    monkeypatch.setenv("ASK_SEOUL_KMA_SHARED_GUARDS_ENABLED", "true")

    module = load_transform_module(filename, module_name=module_name)

    assert module.dag.task_dict[task_id].kwargs["pool"] == "trino_weather_heavy"


def _forecast_landing_pools() -> list[list[object]]:
    script = """
import importlib.util
import json
from pathlib import Path

module_path = Path('dags/domains/weather/weather_vilage_fcst_bronze.py').resolve()
spec = importlib.util.spec_from_file_location('forecast_pool_probe', module_path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
pools = []
for dag in (module.dag, module.recollect_dag):
    task = dag.task_dict['land_kma_raw']
    pools.append([dag.dag_id, task.pool, task.pool_slots])
print('__FORECAST_POOLS__' + json.dumps(pools))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPOSITORY_ROOT,
        env=os.environ.copy(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    line = next(
        line
        for line in result.stdout.splitlines()
        if line.startswith("__FORECAST_POOLS__")
    )
    return json.loads(line.removeprefix("__FORECAST_POOLS__"))
