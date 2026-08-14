"""Fail-open product events and health snapshots for the operating console.

Run metrics describe an Airflow task.  These records describe the data product
that task advanced, so raw/Bronze/Gold/D1 progress and product reliability stay
queryable without changing the business path when R2 observability is down.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence

from common.ops.contract import Layer, RowsSource, RunStatus
from common.ops.run_sink import _put_r2, _safe
from common.runtime_guard import resolve_runtime_target

try:
    from airflow.stats import Stats
except ImportError:  # pragma: no cover - Airflow supplies Stats in production.
    class Stats:  # type: ignore[no-redef]
        @staticmethod
        def incr(_name: str, **_kwargs: Any) -> None:
            return None


LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = "product-observability/v2"
_KST = timezone(timedelta(hours=9))
# 값 집합의 단일 출처는 관문(common.ops.contract) — 여기서 따로 세지 않는다.
# ``silver`` 가 빠져 있어 정제 단계 기록이 값 검증에서 튕기던 결함을 함께 고친다
# (ASK-Seoul#78 §16 정정 2 · V-4). 전 도메인 적용 전에 반드시 필요한 수정이다.
_VALID_LAYERS = {member.value for member in Layer}
_VALID_STATUSES = {member.value for member in RunStatus}
_VALID_ROWS_SOURCES = {member.value for member in RowsSource}


def _validate_row_observation(
    row_count: int | None,
    rows_source: str,
) -> None:
    if rows_source not in _VALID_ROWS_SOURCES:
        raise ValueError(f"unsupported product event rows_source: {rows_source!r}")
    if row_count is None:
        if rows_source != "not_observed":
            raise ValueError("row_count=None requires rows_source='not_observed'")
        return
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        raise ValueError("row_count must be a non-negative integer or None")
    if rows_source == "not_observed":
        raise ValueError("observed row_count requires an authoritative rows_source")


def _context_identity(context: Mapping[str, Any]) -> tuple[str, str, str, int | None]:
    task_instance = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    dag_id = str(
        getattr(dag, "dag_id", None)
        or getattr(task_instance, "dag_id", None)
        or "unknown"
    )
    task_id = str(getattr(task_instance, "task_id", None) or "unknown")
    run_id = str(
        getattr(task_instance, "run_id", None)
        or getattr(dag_run, "run_id", None)
        or context.get("run_id")
        or "unknown"
    )
    try_number = getattr(task_instance, "try_number", None)
    return dag_id, task_id, run_id, try_number


def _observed_at(context: Mapping[str, Any]) -> datetime:
    value = context.get("logical_date") or context.get("data_interval_end")
    if isinstance(value, datetime):
        return value
    return datetime.now(timezone.utc)


def _metric(
    value: int | float | None,
    *,
    unit: str,
    null_meaning: str | None = None,
) -> dict[str, Any]:
    return {
        "value": value,
        "unit": unit,
        "quality_state": "observed" if value is not None else "unknown",
        "null_meaning": null_meaning if value is None else None,
    }


def _unknown_metric(*, unit: str, reason: str) -> dict[str, Any]:
    return _metric(None, unit=unit, null_meaning=reason)


def _record_write_failure(*, kind: str) -> None:
    """Expose fail-open observability loss to the metrics reconciler."""
    try:
        Stats.incr("product_observability.write_failed", tags={"kind": kind})
    except Exception:  # noqa: BLE001 - metrics must not mask application success
        pass
    LOGGER.warning("[ops.product-observability] write failed (ignored): %s", kind)


def build_product_event(
    context: Mapping[str, Any],
    *,
    domain: str,
    layer: str,
    product_ids: Sequence[str] = (),
    status: str = "success",
    row_count: int | None = None,
    rows_source: str = "not_observed",
    quality: Mapping[str, Any] | None = None,
    publication_id: str | None = None,
) -> tuple[str, dict[str, Any]]:
    """Build one deterministic product transition record from Airflow context."""
    if layer not in _VALID_LAYERS:
        raise ValueError(f"unsupported product event layer: {layer!r}")
    if status not in _VALID_STATUSES:
        raise ValueError(f"unsupported product event status: {status!r}")
    _validate_row_observation(row_count, rows_source)
    observed_at = _observed_at(context)
    dag_id, task_id, run_id, try_number = _context_identity(context)
    observed_date = observed_at.astimezone(_KST).date().isoformat()
    normalized_product_ids = sorted({str(product_id) for product_id in product_ids})
    product_id = (
        normalized_product_ids[0]
        if len(normalized_product_ids) == 1
        else "|".join(normalized_product_ids) or "__domain_stage__"
    )
    identity = {
        "domain": domain,
        "layer": layer,
        "product_id": product_id,
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        "try_number": try_number,
        "publication_id": publication_id,
    }
    event_id = hashlib.sha256(
        json.dumps(identity, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    key = (
        f"ops/product-events/observed_date={observed_date}"
        f"/domain={_safe(domain)}/layer={_safe(layer)}"
        f"/event_id={event_id}.json"
    )
    event = {
        "schema_version": SCHEMA_VERSION,
        "observed_at": observed_at.astimezone(timezone.utc).isoformat(),
        "domain": domain,
        "layer": layer,
        "status": status,
        "event_id": event_id,
        "product_id": product_id,
        "product_ids": normalized_product_ids,
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "try_number": try_number,
        "row_count": row_count,
        "rows_source": rows_source,
        "quality": dict(quality or {}),
        "publication_id": publication_id,
    }
    return key, event


def record_product_event(
    context: Mapping[str, Any],
    **kwargs: Any,
) -> dict[str, Any]:
    """Persist one product transition without turning an application success into failure."""
    key, event = build_product_event(context, **kwargs)
    try:
        target = resolve_runtime_target()
    except Exception:  # noqa: BLE001 -- target failure must remain fail-open
        _record_write_failure(kind="target_resolution")
        return event
    try:
        _put_r2(
            key,
            json.dumps(event, ensure_ascii=False, sort_keys=True).encode("utf-8"),
            target=target,
        )
        LOGGER.info("[ops.product-events] %s", key)
    except Exception as exc:  # noqa: BLE001 -- observability must be fail-open
        _record_write_failure(kind=type(exc).__name__)
    return event


def record_domain_stage_event(domain: str, layer: str, *, status: str = "success"):
    """Return an Airflow callback for a domain-level stage transition."""
    def _callback(context: Mapping[str, Any]) -> dict[str, Any]:
        return record_product_event(
            context,
            domain=domain,
            layer=layer,
            product_ids=(),
            status=status,
        )

    return _callback


def build_weather_product_health(
    *,
    weather: Mapping[str, Any],
    profile: Mapping[str, Any] | None,
    detected_at: datetime,
) -> dict[str, Any]:
    """Shape Weather health metrics, keeping unavailable measurements explicit."""
    profile = profile or {}
    expected_issues = weather.get("expected_base_time_count")
    observed_issues = weather.get("base_time_count")
    expected_places = weather.get("expected_grid_count")
    mapped_places = profile.get("mapped_place_count")
    core_categories = profile.get("core_category_count")
    issued_at: datetime | None = None
    try:
        issued_at = datetime.strptime(
            f"{weather['base_date']}{str(weather['base_time']).zfill(4)}",
            "%Y%m%d%H%M",
        ).replace(tzinfo=_KST)
    except (KeyError, TypeError, ValueError):
        pass
    issue_delay = (
        max(0, int((detected_at.astimezone(_KST) - issued_at).total_seconds() // 60))
        if issued_at is not None
        else None
    )
    return {
        "domain": "weather",
        "metrics": {
            "expected_issue_count": _metric(expected_issues, unit="issue", null_meaning="bronze_summary_unavailable"),
            "observed_issue_count": _metric(observed_issues, unit="issue", null_meaning="bronze_summary_unavailable"),
            "core_category_coverage": _metric(
                round(min(1.0, int(core_categories) / 4), 4) if core_categories is not None else None,
                unit="ratio",
                null_meaning="latest_issue_profile_unavailable",
            ),
            "mapped_place_coverage": _metric(
                round(min(1.0, int(mapped_places) / int(expected_places)), 4)
                if mapped_places is not None and int(expected_places or 0) > 0
                else None,
                unit="ratio",
                null_meaning="latest_issue_profile_unavailable",
            ),
            "forecast_horizon": _metric(
                profile.get("forecast_horizon_hours"),
                unit="hour",
                null_meaning="latest_issue_profile_unavailable",
            ),
            "issue_delay": _metric(issue_delay, unit="minute", null_meaning="latest_issue_unavailable"),
            "collection_delay": _metric(
                weather.get("freshness_minutes"),
                unit="minute",
                null_meaning="collection_timestamp_unavailable",
            ),
            "publication_delay": _unknown_metric(
                unit="minute", reason="d1_event_not_joined"
            ),
        },
    }


def build_traffic_product_health(
    *, profile: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Shape Traffic link health from the latest-link Gold aggregate."""
    profile = profile or {}
    metric_specs = {
        "observed_link_count": ("observed_link_count", "link"),
        "available_value_ratio": ("available_value_ratio", "ratio"),
        "link_age_p50": ("link_age_p50", "minute"),
        "link_age_p95": ("link_age_p95", "minute"),
        "source_observation_delay": ("source_observation_delay", "minute"),
        "collection_delay": ("collection_delay", "minute"),
        "stale_link_ratio": ("stale_link_ratio", "ratio"),
    }
    metrics = {
        name: _metric(
            profile.get(source_name),
            unit=unit,
            null_meaning="traffic_link_gold_unavailable",
        )
        for name, (source_name, unit) in metric_specs.items()
    }
    metrics["publication_delay"] = _unknown_metric(
        unit="minute", reason="d1_event_not_joined"
    )
    return {"domain": "traffic", "metrics": metrics}


def build_product_health_snapshot(
    context: Mapping[str, Any],
    health: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    """Build an append-only daily product-health snapshot key and record."""
    observed_at = _observed_at(context)
    dag_id, task_id, run_id, try_number = _context_identity(context)
    domain = str(health.get("domain") or "unknown")
    key = (
        f"ops/product-health/observed_date={observed_at.astimezone(_KST).date().isoformat()}"
        f"/domain={_safe(domain)}/{_safe(dag_id)}__{_safe(run_id)}"
        f"__{_safe(task_id)}__try{try_number}.json"
    )
    return key, {
        "schema_version": SCHEMA_VERSION,
        "observed_at": observed_at.astimezone(timezone.utc).isoformat(),
        "domain": domain,
        "dag_id": dag_id,
        "task_id": task_id,
        "run_id": run_id,
        "try_number": try_number,
        "metrics": dict(health.get("metrics") or {}),
    }


def record_product_health(context: Mapping[str, Any], health: Mapping[str, Any]) -> dict[str, Any]:
    """Persist a health snapshot with the same fail-open boundary as product events."""
    key, snapshot = build_product_health_snapshot(context, health)
    try:
        target = resolve_runtime_target()
    except Exception:  # noqa: BLE001 -- target failure must remain fail-open
        _record_write_failure(kind="target_resolution")
        return snapshot
    try:
        _put_r2(
            key,
            json.dumps(snapshot, ensure_ascii=False, sort_keys=True).encode("utf-8"),
            target=target,
        )
        LOGGER.info("[ops.product-health] %s", key)
    except Exception as exc:  # noqa: BLE001 -- observability must be fail-open
        _record_write_failure(kind=type(exc).__name__)
    return snapshot
