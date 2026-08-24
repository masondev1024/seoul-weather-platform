"""Internal Iceberg publication manifest for forecast-quality Gold products.

This boundary deliberately has no D1, Worker, or serving-asset dependency.
Candidate rows become visible only after the final success record is committed.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from weather_quality_runtime import QualityEvaluationWindow, quality_window_from_dbt_vars


QUALITY_SCHEMA = "weather"
QUALITY_MANIFEST_TABLE = "weather_forecast_quality_publication_manifest"
CATALOG_ENV = "TRINO_ICEBERG_CATALOG"
DEFAULT_CATALOG = "iceberg"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class QualityPublicationError(RuntimeError):
    """Raised when the internal quality publication boundary is unsafe."""


@dataclass(frozen=True, slots=True)
class QualityPublicationResult:
    evaluation_run_id: str
    status: str
    created: bool


def quality_catalog(environ: Mapping[str, str] | None = None) -> str:
    values = os.environ if environ is None else environ
    catalog = str(values.get(CATALOG_ENV, "") or "").strip() or DEFAULT_CATALOG
    if not _IDENTIFIER_RE.fullmatch(catalog):
        raise QualityPublicationError("unsafe quality publication catalog")
    return catalog


def quality_manifest_relation(catalog: str) -> str:
    if not _IDENTIFIER_RE.fullmatch(catalog):
        raise QualityPublicationError("unsafe quality publication catalog")
    return f"{catalog}.{QUALITY_SCHEMA}.{QUALITY_MANIFEST_TABLE}"


def create_quality_manifest_sql(catalog: str) -> str:
    relation = quality_manifest_relation(catalog)
    return f"""CREATE TABLE IF NOT EXISTS {relation} (
    evaluation_run_id varchar,
    status varchar,
    evaluation_as_of timestamp(6),
    window_start_date date,
    window_end_date date,
    forecast_load_start_date date,
    forecast_load_end_date date,
    truth_policy_version varchar,
    vintage_policy_version varchar,
    evidence_policy_version varchar,
    pop_policy_version varchar,
    published_at timestamp(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(window_end_date)']
)"""


def _quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _timestamp(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return "timestamp " + _quote(utc_value.isoformat(sep=" ", timespec="microseconds"))


def _date(value: object) -> str:
    return "date " + _quote(str(value))


def _already_published_sql(catalog: str, window: QualityEvaluationWindow) -> str:
    return (
        "SELECT count(*) FROM "
        f"{quality_manifest_relation(catalog)} WHERE evaluation_run_id = "
        f"{_quote(window.evaluation_run_id)} AND status = 'SUCCESS'"
    )


def insert_quality_success_sql(
    catalog: str,
    window: QualityEvaluationWindow,
    *,
    published_at: datetime,
) -> str:
    relation = quality_manifest_relation(catalog)
    return f"""INSERT INTO {relation} (
    evaluation_run_id, status, evaluation_as_of, window_start_date, window_end_date,
    forecast_load_start_date, forecast_load_end_date, truth_policy_version,
    vintage_policy_version, evidence_policy_version, pop_policy_version, published_at
) VALUES (
    {_quote(window.evaluation_run_id)}, 'SUCCESS', {_timestamp(window.evaluation_as_of)},
    {_date(window.window_start_date)}, {_date(window.window_end_date)},
    {_date(window.forecast_load_start_date)}, {_date(window.forecast_load_end_date)},
    {_quote('observation-truth-policy/v2-internal')},
    {_quote('forecast-vintage-cutoff/v1')},
    {_quote('metric-evidence-gate/v1')}, {_quote('pop-threshold-0.5/v1')},
    {_timestamp(published_at)}
)"""


def publish_quality_success(
    cursor: Any,
    *,
    dbt_vars: Mapping[str, object],
    catalog: str | None = None,
    now: datetime | None = None,
) -> QualityPublicationResult:
    """Create the internal manifest and append one idempotent SUCCESS record."""

    window = quality_window_from_dbt_vars(dbt_vars)
    actual_catalog = catalog or quality_catalog()
    cursor.execute(create_quality_manifest_sql(actual_catalog))
    cursor.execute(_already_published_sql(actual_catalog, window))
    row = cursor.fetchone()
    existing = int(row[0]) if row and row[0] is not None else 0
    if existing:
        return QualityPublicationResult(
            evaluation_run_id=window.evaluation_run_id,
            status="SUCCESS",
            created=False,
        )
    published_at = now or datetime.now(timezone.utc)
    if published_at.tzinfo is None or published_at.utcoffset() is None:
        raise QualityPublicationError("quality publication requires an aware timestamp")
    cursor.execute(
        insert_quality_success_sql(
            actual_catalog,
            window,
            published_at=published_at,
        )
    )
    return QualityPublicationResult(
        evaluation_run_id=window.evaluation_run_id,
        status="SUCCESS",
        created=True,
    )
