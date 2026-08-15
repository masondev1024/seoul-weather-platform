import importlib.util
import json
import types

import pytest

from common.serving import dag_factory
from common.serving.dag_factory import publication_record_payload
from common.serving.publisher import ProductRecord, PublicationError, PublicationReport


def _capture_wrapper_build_calls(monkeypatch, path, module_name):
    import sys
    import types

    captured = []
    sentinel_dag = object()
    factory_module = types.ModuleType("common.serving.dag_factory")

    def build_serving_export_dag(**kwargs):
        captured.append(dict(kwargs))
        return sentinel_dag

    factory_module.build_serving_export_dag = build_serving_export_dag
    monkeypatch.setitem(sys.modules, "common.serving.dag_factory", factory_module)
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return captured


def test_publication_xcom_payload_exposes_stage_and_snapshot_rollback_state():
    record = ProductRecord(
        product_id="weather_place_risk_window",
        model_name="gold_weather_place_risk_window",
        publication_id="publication-1",
        source_run_id="run-1",
        published_at="2026-07-29T00:00:00+00:00",
        serving_status="failed",
        reason="API smoke test 실패",
        api_smoke_detail={
            "http_status": 503,
            "error_code": "product_not_ready",
        },
        stage="api_smoke",
        rollback_status="restored",
        projection_schema_hash="projection-hash-1",
        source_content_hash="source-hash-1",
        d1_content_hash="d1-hash-1",
    )

    payload = publication_record_payload(record)

    assert payload["publication_id"] == "publication-1"
    assert payload["stage"] == "api_smoke"
    assert payload["rollback_status"] == "restored"
    assert payload["projection_schema_hash"] == "projection-hash-1"
    assert payload["source_content_hash"] == "source-hash-1"
    assert payload["d1_content_hash"] == "d1-hash-1"
    assert payload["api_smoke_detail"] == {
        "http_status": 503,
        "error_code": "product_not_ready",
    }


def test_serving_export_factory_keeps_content_parity_opt_in_default_false():
    import inspect

    assert inspect.signature(dag_factory.build_serving_export_dag).parameters[
        "verify_content_parity"
    ].default is False


def test_manifest_path_uses_external_stable_manifest_only_when_configured(tmp_path):
    artifact_root = tmp_path / "artifacts" / "release-1"

    assert dag_factory._manifest_path(
        "weather",
        None,
        env={"ASK_SEOUL_DBT_ARTIFACT_ROOT": str(artifact_root)},
    ) == str(artifact_root / "target" / "manifest.json")
    assert dag_factory._manifest_path("weather", None, env={}) == (
        "/opt/airflow/dbt/domains/traffic_weather/target/manifest.json"
    )


@pytest.mark.parametrize("configured", ["", "relative", ".", ".."])
def test_manifest_path_rejects_unsafe_configured_artifact_root(configured):
    with pytest.raises(RuntimeError, match="ASK_SEOUL_DBT_ARTIFACT_ROOT"):
        dag_factory._manifest_path(
            "weather",
            None,
            env={"ASK_SEOUL_DBT_ARTIFACT_ROOT": configured},
        )


def test_serving_export_factory_serializes_all_publishers_in_shared_d1_pool():
    dag = dag_factory.build_serving_export_dag(
        domain="test_domain",
        product_ids=("test_product",),
        dag_id="test_domain_serving_export_pool_contract",
    )

    assert dag.get_task("publish_to_d1").pool == "serving_d1_publish"
    assert dag_factory.SERVING_D1_PUBLISH_POOL == "serving_d1_publish"


def test_domain_catalog_retirement_uses_only_disabled_contract_product_ids(tmp_path):
    manifest = {
        "nodes": {
            "model.project.gold_weather_grid_current_outlook": {
                "resource_type": "model",
                "name": "gold_weather_grid_current_outlook",
                "config": {
                    "meta": {
                        "serving": {
                            "enabled": False,
                            "external": False,
                            "retire_on_publish": True,
                            "product_id": "weather_grid_current_outlook",
                        }
                    }
                },
            }
        }
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    class Catalog:
        product_ids = None

        def delete_catalog_product_ids(self, product_ids):
            self.product_ids = tuple(product_ids)

    catalog = Catalog()
    retire = getattr(dag_factory, "retire_domain_catalog_entries", None)
    assert retire is not None, "catalog retirement helper is missing"

    assert retire(catalog, manifest_path=manifest_path, domain="weather") == (
        "weather_grid_current_outlook",
    )
    assert catalog.product_ids == ("weather_grid_current_outlook",)


def _retirement_manifest(tmp_path):
    manifest = {
        "nodes": {
            f"model.project.{model_name}": {
                "resource_type": "model",
                "name": model_name,
                "config": {
                    "meta": {
                        "serving": {
                            "enabled": False,
                            "external": False,
                            "retire_on_publish": True,
                            "product_id": product_id,
                        }
                    }
                },
            }
            for model_name, product_id in (
                ("gold_weather_grid_current_outlook", "weather_grid_current_outlook"),
                ("gold_weather_grid_precipitation_window", "weather_grid_precipitation_window"),
            )
        }
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path


def test_publication_task_retires_exact_catalog_products_only_after_success(tmp_path):
    events = []
    report = PublicationReport()

    class Catalog:
        def delete_catalog_product_ids(self, product_ids):
            events.append(("retire", tuple(product_ids)))

    def publish_fn(*_args, **_kwargs):
        events.append(("publish",))
        return report

    published, retired = dag_factory.publish_then_retire_catalog(
        publish_fn=publish_fn,
        contracts=(),
        source=object(),
        d1=Catalog(),
        smoke=object(),
        source_run_id="run-1",
        verify_content_parity=False,
        manifest_path=_retirement_manifest(tmp_path),
        domain="weather",
    )

    assert published is report
    assert retired == (
        "weather_grid_current_outlook",
        "weather_grid_precipitation_window",
    )
    assert events == [
        ("publish",),
        ("retire", retired),
    ]


def test_publication_task_does_not_retire_catalog_when_publish_fails(tmp_path):
    class Catalog:
        deleted = False

        def delete_catalog_product_ids(self, _product_ids):
            self.deleted = True

    catalog = Catalog()

    def publish_fn(*_args, **_kwargs):
        raise PublicationError(PublicationReport(failures=["publisher failed"]))

    with pytest.raises(PublicationError, match="publisher failed"):
        dag_factory.publish_then_retire_catalog(
            publish_fn=publish_fn,
            contracts=(),
            source=object(),
            d1=catalog,
            smoke=object(),
            source_run_id="run-1",
            verify_content_parity=False,
            manifest_path=_retirement_manifest(tmp_path),
            domain="weather",
        )

    assert catalog.deleted is False


def test_publication_task_propagates_catalog_retirement_failure_for_airflow_retry(tmp_path):
    report = PublicationReport()

    class Catalog:
        def delete_catalog_product_ids(self, _product_ids):
            raise RuntimeError("D1 retirement write failed")

    with pytest.raises(RuntimeError, match="D1 retirement write failed"):
        dag_factory.publish_then_retire_catalog(
            publish_fn=lambda *_args, **_kwargs: report,
            contracts=(),
            source=object(),
            d1=Catalog(),
            smoke=object(),
            source_run_id="run-1",
            verify_content_parity=False,
            manifest_path=_retirement_manifest(tmp_path),
            domain="weather",
        )


def test_watchdog_target_must_match_execution_environment():
    assert dag_factory.validate_watchdog_target(
        "dev", env={"DBT_TARGET": "dev"}
    ) == "dev"
    assert dag_factory.validate_watchdog_target(
        " DEV ", env={"DBT_TARGET": "dev"}
    ) == "dev"

    with pytest.raises(RuntimeError, match="disagrees with environment"):
        dag_factory.validate_watchdog_target(
            "dev", env={"DBT_TARGET": "prod"}
        )


def test_publication_scope_uses_the_latest_terminal_asset_subset():
    configured = ("incident", "flow-latest", "flow-profile")
    context = {
        "triggering_asset_events": {
            "terminal": [
                types.SimpleNamespace(extra={"product_ids": ["incident"]}),
                types.SimpleNamespace(
                    extra={"product_ids": ["flow-latest", "flow-profile"]}
                ),
            ]
        }
    }

    assert dag_factory.resolve_publication_product_ids(
        context,
        configured,
        metadata_key="product_ids",
    ) == ("flow-latest", "flow-profile")


@pytest.mark.parametrize(
    "context",
    [
        {},
        {"triggering_asset_events": {"terminal": []}},
        {
            "triggering_asset_events": {
                "terminal": [types.SimpleNamespace(extra={"product_ids": ["unknown"]})]
            }
        },
    ],
)
def test_publication_scope_fails_closed_without_a_valid_subset(context):
    with pytest.raises(RuntimeError):
        dag_factory.resolve_publication_product_ids(
            context,
            ("incident", "flow"),
            metadata_key="product_ids",
        )


def test_publication_scope_accepts_an_explicit_empty_terminal_scope():
    context = {
        "triggering_asset_events": {
            "terminal": [types.SimpleNamespace(extra={"product_ids": []})]
        }
    }

    assert dag_factory.resolve_publication_product_ids(
        context,
        ("incident", "flow"),
        metadata_key="product_ids",
    ) == ()


def test_non_target_serving_wrappers_do_not_opt_into_content_parity(monkeypatch):
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]

    wrappers = (
        ("citydata_serving_export_under_test", "domains/citydata/citydata_serving_export.py"),
        ("culture_serving_export_under_test", "domains/culture/culture_serving_export.py"),
        ("transit_serving_export_under_test", "domains/transit/transit_serving_export.py"),
    )
    missing = [relative_path for _, relative_path in wrappers if not (root / relative_path).is_file()]
    if missing:
        pytest.skip(
            "Weather-only repository excludes non-target serving wrappers: "
            + ", ".join(missing)
        )

    calls = []
    for module_name, relative_path in wrappers:
        calls.extend(_capture_wrapper_build_calls(monkeypatch, root / relative_path, module_name))

    assert calls
    assert all("verify_content_parity" not in call for call in calls)


def test_d1_product_event_carries_runtime_publication_id_and_delay(monkeypatch):
    captured = []
    monkeypatch.setattr(
        dag_factory,
        "record_product_event",
        lambda context, **kwargs: captured.append((context, kwargs)) or kwargs,
    )
    record = ProductRecord(
        product_id="weather_place_risk_window",
        model_name="gold_weather_place_risk_window",
        publication_id="publication-1",
        source_run_id="run-1",
        published_at="2026-07-30T00:10:00+00:00",
        serving_status="published",
        reason="ok",
        published_row_count=427,
        freshness="2026-07-30T00:03:00+00:00",
    )

    dag_factory.record_publication_events({"run_id": "run-1"}, "weather", [record])

    assert captured == [
        (
            {"run_id": "run-1"},
            {
                "domain": "weather",
                "layer": "d1",
                "product_ids": ("weather_place_risk_window",),
                "status": "success",
                "row_count": 427,
                "rows_source": "publication_ledger",
                "publication_id": "publication-1",
                "quality": {
                    "publication_delay": {
                        "value": 7,
                        "unit": "minute",
                        "quality_state": "observed",
                        "null_meaning": None,
                    }
                },
            },
        )
    ]


def test_failed_d1_product_event_does_not_treat_default_zero_as_observed(
    monkeypatch,
):
    captured = []
    monkeypatch.setattr(
        dag_factory,
        "record_product_event",
        lambda context, **kwargs: captured.append((context, kwargs)) or kwargs,
    )
    record = ProductRecord(
        product_id="weather_place_risk_window",
        model_name="gold_weather_place_risk_window",
        publication_id="publication-failed",
        source_run_id="run-1",
        published_at="2026-07-30T00:10:00+00:00",
        serving_status="failed",
        reason="D1 write failed",
    )

    dag_factory.record_publication_events({"run_id": "run-1"}, "weather", [record])

    event = captured[0][1]
    assert event["status"] == "failed"
    assert event["row_count"] is None
    assert event["rows_source"] == "not_observed"
