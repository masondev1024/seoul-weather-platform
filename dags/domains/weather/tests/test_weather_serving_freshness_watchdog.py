"""Thin wrapper contract for the Weather independent serving watchdog (#776)."""

import importlib.util
import sys
import types
from pathlib import Path


DAG_PATH = Path(__file__).resolve().parents[1] / "weather_serving_freshness_watchdog.py"


def _load_module(monkeypatch):
    calls = []
    factory_module = types.ModuleType("common.serving.dag_factory")
    factory_module.build_serving_freshness_watchdog_dag = (
        lambda **kwargs: calls.append(kwargs) or object()
    )
    errors_module = types.ModuleType("common.errors.airflow")
    errors_module.problem_failure_callback = lambda **kwargs: ("callback", kwargs)
    monkeypatch.setitem(sys.modules, "common.serving.dag_factory", factory_module)
    monkeypatch.setitem(sys.modules, "common.errors.airflow", errors_module)

    spec = importlib.util.spec_from_file_location(
        "weather_serving_freshness_watchdog_under_test", DAG_PATH
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module, calls


def test_weather_watchdog_uses_four_place_products_and_measured_grace(monkeypatch):
    monkeypatch.setenv("DBT_TARGET", "prod")
    monkeypatch.delenv("ASK_SEOUL_TARGET", raising=False)

    _, calls = _load_module(monkeypatch)

    products = [
        "weather_place_current_outlook",
        "weather_place_precipitation_window",
        "weather_place_risk_window",
        "weather_place_forecast_change_daily",
    ]
    assert calls == [
        {
            "domain": "weather",
            "product_ids": products,
            "schedule": "35 * * * *",
            "dag_id": "weather_serving_freshness_watchdog",
            "target": "prod",
            "exact_domain_contracts": True,
            "partitioned_domain_scope": True,
            "naive_freshness_timezones": {
                product_id: "Asia/Seoul" for product_id in products
            },
            "publication_grace_minutes": 5,
            "failure_callback": (
                "callback",
                {"domain": "weather", "source_system": "serving_freshness_watchdog"},
            ),
        }
    ]


def test_weather_watchdog_target_follows_runtime_env(monkeypatch):
    monkeypatch.setenv("DBT_TARGET", "dev")
    monkeypatch.delenv("ASK_SEOUL_TARGET", raising=False)

    _, calls = _load_module(monkeypatch)

    assert calls[0]["target"] == "dev"
