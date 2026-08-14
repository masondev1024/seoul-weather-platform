import importlib.util
import sys
import types
from pathlib import Path


DAG_PATH = Path(__file__).resolve().parents[1] / "weather_serving_export.py"


class FakeAsset:
    def __init__(self, uri):
        self.uri = uri

    def __eq__(self, other):
        return isinstance(other, FakeAsset) and self.uri == other.uri


def test_weather_serving_export_is_a_thin_common_publisher_wrapper(monkeypatch):
    captured = {}
    sentinel_dag = object()
    factory_module = types.ModuleType("common.serving.dag_factory")

    def build_serving_export_dag(**kwargs):
        captured.update(kwargs)
        return sentinel_dag

    factory_module.build_serving_export_dag = build_serving_export_dag
    monkeypatch.setitem(sys.modules, "common.serving.dag_factory", factory_module)
    airflow_sdk = types.ModuleType("airflow.sdk")
    airflow_sdk.Asset = FakeAsset
    monkeypatch.setitem(sys.modules, "airflow.sdk", airflow_sdk)

    spec = importlib.util.spec_from_file_location(
        "weather_serving_export_under_test",
        DAG_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    assert module.dag is sentinel_dag
    assert captured == {
        "domain": "weather",
        "product_ids": [
            "weather_place_current_outlook",
            "weather_place_precipitation_window",
            "weather_place_risk_window",
            "weather_place_forecast_change_daily",
        ],
        "exact_domain_contracts": True,
        "require_public_projection": True,
        "verify_content_parity": True,
        "schedule": FakeAsset("iceberg://weather/gold/publication-ready"),
        "dag_id": "weather_serving_export",
        "target": "dev",
        "schema": "weather",
    }


def test_weather_export_subscribes_only_to_the_validated_gold_terminal_asset():
    source = DAG_PATH.read_text(encoding="utf-8")

    assert "WEATHER_GOLD_PUBLICATION_READY_ASSET" in source
    assert "schedule=Asset(WEATHER_GOLD_PUBLICATION_READY_ASSET)" in source


def test_weather_export_is_visible_to_airflow_safe_mode():
    source = DAG_PATH.read_text(encoding="utf-8").lower()

    assert "airflow" in source
    assert "dag" in source


def test_weather_export_is_the_only_weather_serving_dag():
    assert not (DAG_PATH.parent / "weather_insight_serving_export.py").exists()
