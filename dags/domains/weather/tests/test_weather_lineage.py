import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


DOMAIN_DIR = Path(__file__).resolve().parents[1]


def load_lineage_module():
    module_path = DOMAIN_DIR / "weather_lineage.py"
    spec = importlib.util.spec_from_file_location(
        "weather_lineage_under_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_weather_lineage_is_noop_without_explicit_selective_enable(monkeypatch):
    module = load_lineage_module()
    monkeypatch.delenv("AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE", raising=False)
    monkeypatch.setattr(
        module.importlib,
        "import_module",
        lambda _name: pytest.fail(
            "OpenLineage provider must not be imported by default"
        ),
    )
    dag = object()

    assert module.enable_lineage_if_configured(dag) is dag


def test_weather_lineage_dynamically_enables_dag_when_opted_in(monkeypatch):
    module = load_lineage_module()
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE", "true")
    enabled = []
    provider = SimpleNamespace(enable_lineage=lambda dag: enabled.append(dag) or dag)
    monkeypatch.setattr(module.importlib, "import_module", lambda _name: provider)
    dag = object()

    assert module.enable_lineage_if_configured(dag) is dag
    assert enabled == [dag]


def test_weather_lineage_returns_the_provider_result(monkeypatch):
    module = load_lineage_module()
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE", "true")
    enabled_dag = object()
    monkeypatch.setattr(
        module.importlib,
        "import_module",
        lambda _name: SimpleNamespace(enable_lineage=lambda _dag: enabled_dag),
    )

    assert module.enable_lineage_if_configured(object()) is enabled_dag


def test_weather_lineage_opt_in_fails_explicitly_without_provider(monkeypatch):
    module = load_lineage_module()
    monkeypatch.setenv("AIRFLOW__OPENLINEAGE__SELECTIVE_ENABLE", "true")
    monkeypatch.setattr(
        module.importlib,
        "import_module",
        lambda _name: (_ for _ in ()).throw(ModuleNotFoundError("provider missing")),
    )

    with pytest.raises(
        RuntimeError, match="OpenLineage selective enable is configured"
    ):
        module.enable_lineage_if_configured(object())
