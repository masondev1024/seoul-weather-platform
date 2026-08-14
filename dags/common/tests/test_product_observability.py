from __future__ import annotations

from datetime import datetime, timezone

import pytest

import common.ops.product_observability as product_observability
from common.ops.product_observability import (
    build_product_event,
    build_traffic_product_health,
    build_weather_product_health,
    record_domain_stage_event,
    record_product_event,
)


class _TaskInstance:
    dag_id = "weather_vilage_fcst_bronze"
    task_id = "verify_kma_bronze_runtime"
    run_id = "scheduled__2026-07-30T00:00:00+00:00"
    try_number = 2


def _context(**extra):
    context = {
        "ti": _TaskInstance(),
        "logical_date": datetime(2026, 7, 30, 0, 0, tzinfo=timezone.utc),
        "params": {"target": "prod"},
    }
    context.update(extra)
    return context


def test_product_event_keeps_product_ids_runtime_publication_and_quality_together():
    key, event = build_product_event(
        _context(),
        domain="weather",
        layer="bronze",
        product_ids=("weather_place_current_outlook",),
        row_count=427,
        rows_source="bronze_run_manifest",
        quality={"coverage": {"value": 1.0, "quality_state": "observed"}},
    )

    assert key.startswith(
        "ops/product-events/observed_date=2026-07-30/domain=weather/"
        "layer=bronze/event_id="
    )
    assert key.endswith(".json")
    assert event["schema_version"] == "product-observability/v2"
    assert len(event["event_id"]) == 64
    assert event["product_id"] == "weather_place_current_outlook"
    assert event["product_ids"] == ["weather_place_current_outlook"]
    assert event["publication_id"] is None
    assert event["row_count"] == 427
    assert event["rows_source"] == "bronze_run_manifest"
    assert event["quality"]["coverage"]["quality_state"] == "observed"


def test_product_event_keeps_observed_zero_distinct_from_unknown():
    observed_key, observed = build_product_event(
        _context(),
        domain="traffic",
        layer="raw",
        row_count=0,
        rows_source="raw_manifest",
    )
    unknown_key, unknown = build_product_event(
        _context(),
        domain="traffic",
        layer="raw",
    )

    assert observed["row_count"] == 0
    assert observed["rows_source"] == "raw_manifest"
    assert unknown["row_count"] is None
    assert unknown["rows_source"] == "not_observed"
    assert observed_key == unknown_key
    assert observed["event_id"] == unknown["event_id"]


@pytest.mark.parametrize(
    ("row_count", "rows_source"),
    [
        (None, "raw_manifest"),
        (1, "not_observed"),
        (-1, "raw_manifest"),
        (True, "raw_manifest"),
        (1, "unsupported"),
    ],
)
def test_product_event_rejects_inconsistent_row_observation(
    row_count,
    rows_source,
):
    with pytest.raises(ValueError):
        build_product_event(
            _context(),
            domain="traffic",
            layer="raw",
            row_count=row_count,
            rows_source=rows_source,
        )


def test_product_event_uses_distinct_idempotent_keys_per_product_publication():
    first_key, first = build_product_event(
        _context(),
        domain="weather",
        layer="d1",
        product_ids=("weather_place_current_outlook",),
        publication_id="publication-a",
    )
    retry_key, retry = build_product_event(
        _context(),
        domain="weather",
        layer="d1",
        product_ids=("weather_place_current_outlook",),
        publication_id="publication-a",
    )
    second_key, second = build_product_event(
        _context(),
        domain="weather",
        layer="d1",
        product_ids=("weather_place_risk_window",),
        publication_id="publication-b",
    )

    assert first_key == retry_key
    assert first["event_id"] == retry["event_id"]
    assert first_key != second_key
    assert first["event_id"] != second["event_id"]
    assert first["rows_source"] == "not_observed"


def test_product_event_uses_runtime_target_when_callback_has_no_target_param(monkeypatch):
    writes = []
    monkeypatch.setattr(
        product_observability,
        "resolve_runtime_target",
        lambda: "prod",
    )
    monkeypatch.setattr(
        product_observability,
        "_put_r2",
        lambda key, payload, *, target: writes.append((key, payload, target)),
    )

    record_product_event(
        _context(params={}),
        domain="traffic",
        layer="raw",
        product_ids=("seoul_traffic_incident",),
    )

    assert len(writes) == 1
    assert writes[0][2] == "prod"


def test_product_event_counts_target_resolution_failure_without_dev_fallback(monkeypatch):
    counters = []
    monkeypatch.setattr(
        product_observability,
        "resolve_runtime_target",
        lambda: (_ for _ in ()).throw(RuntimeError("target is not configured")),
    )
    monkeypatch.setattr(
        product_observability.Stats,
        "incr",
        lambda name, **kwargs: counters.append((name, kwargs)),
    )
    monkeypatch.setattr(
        product_observability,
        "_put_r2",
        lambda *_args, **_kwargs: pytest.fail("must not select dev by default"),
    )

    record_product_event(
        _context(params={}),
        domain="traffic",
        layer="raw",
        product_ids=("seoul_traffic_incident",),
    )

    assert counters == [
        ("product_observability.write_failed", {"tags": {"kind": "target_resolution"}})
    ]


def test_weather_health_distinguishes_observed_zero_from_unknown_metric():
    health = build_weather_product_health(
        weather={
            "expected_base_time_count": 8,
            "base_time_count": 8,
            "expected_grid_count": 80,
            "base_date": "20260730",
            "base_time": "0500",
            "freshness_minutes": 20,
        },
        profile={
            "core_category_count": 4,
            "mapped_place_count": 80,
            "forecast_horizon_hours": 72,
        },
        detected_at=datetime(2026, 7, 30, 6, 0, tzinfo=timezone.utc),
    )

    assert health["metrics"]["expected_issue_count"] == {
        "value": 8,
        "unit": "issue",
        "quality_state": "observed",
        "null_meaning": None,
    }
    assert health["metrics"]["core_category_coverage"]["value"] == 1.0
    assert health["metrics"]["mapped_place_coverage"]["value"] == 1.0
    assert health["metrics"]["forecast_horizon"]["value"] == 72
    assert health["metrics"]["publication_delay"]["value"] is None
    assert health["metrics"]["publication_delay"]["quality_state"] == "unknown"
    assert health["metrics"]["publication_delay"]["null_meaning"] == "d1_event_not_joined"


def test_traffic_health_keeps_failed_gold_lookup_unknown_without_synthetic_zeroes():
    health = build_traffic_product_health(profile=None)

    assert set(health["metrics"]) == {
        "observed_link_count",
        "available_value_ratio",
        "link_age_p50",
        "link_age_p95",
        "source_observation_delay",
        "collection_delay",
        "stale_link_ratio",
        "publication_delay",
    }
    assert all(metric["value"] is None for metric in health["metrics"].values())
    assert all(
        metric["quality_state"] == "unknown" for metric in health["metrics"].values()
    )


def test_domain_stage_callback_records_only_after_task_success(monkeypatch):
    captured = []
    monkeypatch.setattr(
        "common.ops.product_observability.record_product_event",
        lambda context, **kwargs: captured.append((context, kwargs)) or kwargs,
    )
    callback = record_domain_stage_event("weather", "gold")
    context = _context()

    callback(context)

    assert captured == [
        (
            context,
            {
                "domain": "weather",
                "layer": "gold",
                "product_ids": (),
                "status": "success",
            },
        )
    ]


def test_domain_stage_failure_callback_records_failed_event(monkeypatch):
    captured = []
    monkeypatch.setattr(
        "common.ops.product_observability.record_product_event",
        lambda context, **kwargs: captured.append((context, kwargs)) or kwargs,
    )
    callback = record_domain_stage_event("traffic", "bronze", status="failed")
    context = _context(exception=RuntimeError("materialize failed"))

    callback(context)

    assert captured == [
        (
            context,
            {
                "domain": "traffic",
                "layer": "bronze",
                "product_ids": (),
                "status": "failed",
            },
        )
    ]
