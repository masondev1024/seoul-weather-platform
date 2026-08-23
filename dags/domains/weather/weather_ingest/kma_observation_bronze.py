"""Dedicated Iceberg Bronze contract for KMA current observations."""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, timezone
from typing import Callable, Sequence

from weather_ingest.common.runtime import create_schema_if_needed, sql_string
from weather_ingest.errors import WeatherCompletenessError, WeatherRawIntegrityError
from weather_ingest.kma_observation import (
    KST,
    REQUIRED_CATEGORIES,
    SOURCE_ID,
    observation_slot_utc,
    parse_and_normalize_kma_observation,
)
from weather_ingest.kma_observation_landing import ObservationLandingBatch


OBSERVATION_BRONZE_TABLE = "bronze_kma_ultra_srt_ncst"
OBSERVATION_BRONZE_COLUMNS = (
    "idempotency_key",
    "request_id",
    "source_id",
    "dag_run_id",
    "manifest_key",
    "observed_slot",
    "observed_at",
    "base_date",
    "base_time",
    "grid_id",
    "nx",
    "ny",
    "category",
    "observed_value",
    "unit",
    "quality_status",
    "raw_object_key",
    "payload_sha256",
    "source_revision",
    "http_status",
    "collected_at",
)


def create_observation_bronze_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.{OBSERVATION_BRONZE_TABLE}"
    create_schema_if_needed(cursor, qualified_schema)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            idempotency_key varchar,
            request_id varchar,
            source_id varchar,
            dag_run_id varchar,
            manifest_key varchar,
            observed_slot varchar,
            observed_at timestamp(6),
            base_date varchar,
            base_time varchar,
            grid_id varchar,
            nx integer,
            ny integer,
            category varchar,
            observed_value double,
            unit varchar,
            quality_status varchar,
            raw_object_key varchar,
            payload_sha256 varchar,
            source_revision varchar,
            http_status integer,
            collected_at timestamp(6)
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['day(observed_at)']
        )
        """
    )
    return qualified_table


def _idempotency_key(
    *,
    source_id: str,
    observed_at: datetime,
    grid_id: str,
    category: str,
    source_revision: str,
) -> str:
    observed_at_utc = observed_at.astimezone(timezone.utc).isoformat(
        timespec="microseconds"
    )
    material = "\x1f".join(
        (
            source_id,
            observed_at_utc,
            grid_id,
            category,
            source_revision,
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _validate_complete_landing(
    batch: ObservationLandingBatch,
    *,
    expected_grid_count: int,
) -> None:
    expected_rows = expected_grid_count * len(REQUIRED_CATEGORIES)
    if batch.source_id != SOURCE_ID:
        raise WeatherCompletenessError(
            "KMA observation Bronze source is not the observation product"
        )
    if not batch.is_publishable or not batch.manifest_key:
        raise WeatherCompletenessError(
            "KMA observation Bronze requires a verified complete manifest"
        )
    if batch.grid_count != expected_grid_count or len(batch.raw_objects) != (
        expected_grid_count
    ):
        raise WeatherCompletenessError(
            "KMA observation Bronze grid completeness failed"
        )
    if batch.row_count != expected_rows:
        raise WeatherCompletenessError(
            "KMA observation Bronze row completeness failed"
        )
    grid_keys = [(raw.nx, raw.ny) for raw in batch.raw_objects]
    if len(set(grid_keys)) != expected_grid_count:
        raise WeatherCompletenessError(
            "KMA observation Bronze raw grid identities are not unique"
        )


def build_observation_bronze_rows(
    batch: ObservationLandingBatch,
    *,
    read_raw: Callable[[str], bytes],
    expected_grid_count: int = 80,
) -> list[dict[str, object]]:
    """Revalidate complete Raw objects and map them to canonical Bronze rows."""
    _validate_complete_landing(batch, expected_grid_count=expected_grid_count)
    expected_observed_at = observation_slot_utc(batch.base_date, batch.base_time)
    expected_slot = expected_observed_at.astimezone(KST).isoformat()
    if batch.observed_slot != expected_slot:
        raise WeatherCompletenessError(
            "KMA observation Bronze manifest slot does not match base time"
        )
    rows: list[dict[str, object]] = []
    for raw in batch.raw_objects:
        try:
            payload = read_raw(raw.raw_object_key)
        except (KeyError, FileNotFoundError) as exc:
            raise WeatherRawIntegrityError(
                "KMA observation Bronze raw object is missing"
            ) from exc
        payload_hash = hashlib.sha256(payload).hexdigest()
        if payload_hash != raw.payload_sha256:
            raise WeatherRawIntegrityError(
                "KMA observation Bronze raw payload hash does not match"
            )
        metadata, records = parse_and_normalize_kma_observation(
            payload,
            base_date=batch.base_date,
            base_time=batch.base_time,
            nx=raw.nx,
            ny=raw.ny,
            collected_at=raw.collected_at,
        )
        categories = tuple(record.category for record in records)
        if (
            str(metadata["payload_sha256"]) != raw.payload_sha256
            or raw.category_count != len(REQUIRED_CATEGORIES)
            or raw.categories != REQUIRED_CATEGORIES
            or categories != REQUIRED_CATEGORIES
        ):
            raise WeatherRawIntegrityError(
                "KMA observation Bronze raw category contract does not match"
            )
        for record in records:
            observed_at = record.observed_at.astimezone(timezone.utc)
            row = {
                "idempotency_key": _idempotency_key(
                    source_id=SOURCE_ID,
                    observed_at=observed_at,
                    grid_id=record.grid_id,
                    category=record.category,
                    source_revision=record.source_revision,
                ),
                "request_id": raw.request_id,
                "source_id": SOURCE_ID,
                "dag_run_id": batch.run_id,
                "manifest_key": batch.manifest_key,
                "observed_slot": batch.observed_slot,
                "observed_at": observed_at.replace(tzinfo=None),
                "base_date": batch.base_date,
                "base_time": batch.base_time,
                "grid_id": record.grid_id,
                "nx": record.nx,
                "ny": record.ny,
                "category": record.category,
                "observed_value": float(record.value),
                "unit": record.unit,
                "quality_status": "provisional",
                "raw_object_key": raw.raw_object_key,
                "payload_sha256": raw.payload_sha256,
                "source_revision": record.source_revision,
                "http_status": raw.http_status,
                "collected_at": record.collected_at.astimezone(timezone.utc).replace(
                    tzinfo=None
                ),
            }
            rows.append(row)
    validate_observation_bronze_rows(
        rows,
        expected_grid_count=expected_grid_count,
    )
    return rows


def validate_observation_bronze_rows(
    rows: Sequence[dict[str, object]],
    *,
    expected_grid_count: int = 80,
) -> None:
    expected_rows = expected_grid_count * len(REQUIRED_CATEGORIES)
    if len(rows) != expected_rows:
        raise WeatherCompletenessError(
            "KMA observation Bronze row count mismatch: "
            f"expected={expected_rows}, actual={len(rows)}"
        )
    for row in rows:
        if tuple(row) != OBSERVATION_BRONZE_COLUMNS:
            raise WeatherCompletenessError(
                "KMA observation Bronze row schema does not match"
            )
        if row["source_id"] != SOURCE_ID:
            raise WeatherCompletenessError(
                "KMA observation Bronze row source does not match"
            )
        value = row["observed_value"]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(
            float(value)
        ):
            raise WeatherCompletenessError(
                "KMA observation Bronze value must be finite"
            )
        if row["quality_status"] != "provisional":
            raise WeatherCompletenessError(
                "KMA observation Bronze quality must remain provisional"
            )
    idempotency_keys = [str(row["idempotency_key"]) for row in rows]
    if len(set(idempotency_keys)) != len(idempotency_keys):
        raise WeatherCompletenessError(
            "KMA observation Bronze idempotency keys are duplicated"
        )
    if len({str(row["dag_run_id"]) for row in rows}) != 1:
        raise WeatherCompletenessError(
            "KMA observation Bronze must contain one run"
        )
    if len({str(row["observed_slot"]) for row in rows}) != 1:
        raise WeatherCompletenessError(
            "KMA observation Bronze must contain one observed slot"
        )
    grid_categories: dict[tuple[int, int], set[str]] = {}
    for row in rows:
        grid = (int(row["nx"]), int(row["ny"]))
        grid_categories.setdefault(grid, set()).add(str(row["category"]))
    if len(grid_categories) != expected_grid_count:
        raise WeatherCompletenessError(
            "KMA observation Bronze grid count does not match"
        )
    if any(categories != set(REQUIRED_CATEGORIES) for categories in grid_categories.values()):
        raise WeatherCompletenessError(
            "KMA observation Bronze categories per grid do not match"
        )


def observation_grid_revisions(
    rows: Sequence[dict[str, object]],
    *,
    expected_grid_count: int = 80,
) -> list[dict[str, str]]:
    """Return the bounded revision scope needed to verify an idempotent load."""
    validate_observation_bronze_rows(
        rows,
        expected_grid_count=expected_grid_count,
    )
    revisions_by_grid: dict[str, set[str]] = {}
    for row in rows:
        revisions_by_grid.setdefault(str(row["grid_id"]), set()).add(
            str(row["source_revision"])
        )
    if len(revisions_by_grid) != expected_grid_count or any(
        len(revisions) != 1 for revisions in revisions_by_grid.values()
    ):
        raise WeatherCompletenessError(
            "KMA observation Bronze grid revision scope does not match"
        )
    return [
        {"grid_id": grid_id, "source_revision": next(iter(revisions))}
        for grid_id, revisions in sorted(revisions_by_grid.items())
    ]


def _observation_slot_filter(
    source_id: str,
    observed_slot: str,
    observed_at: datetime,
):
    from pyiceberg.expressions import And, EqualTo

    return And(
        And(
            EqualTo("source_id", source_id),
            EqualTo("observed_slot", observed_slot),
        ),
        EqualTo("observed_at", observed_at),
    )


def _existing_observation_rows(
    table,
    *,
    source_id: str,
    observed_slot: str,
    observed_at: datetime,
) -> list[dict[str, object]]:
    """Read only the partition-prunable observation slot revision set."""
    arrow = table.scan(
        row_filter=_observation_slot_filter(
            source_id,
            observed_slot,
            observed_at,
        ),
        selected_fields=OBSERVATION_BRONZE_COLUMNS,
    ).to_arrow()
    return [dict(row) for row in arrow.to_pylist()]


_IDEMPOTENT_CONTENT_COLUMNS = (
    "source_id",
    "observed_slot",
    "observed_at",
    "base_date",
    "base_time",
    "grid_id",
    "nx",
    "ny",
    "category",
    "observed_value",
    "unit",
    "quality_status",
    "payload_sha256",
    "source_revision",
)


def _idempotent_content(row: dict[str, object]) -> tuple[object, ...]:
    try:
        return tuple(row[column] for column in _IDEMPOTENT_CONTENT_COLUMNS)
    except KeyError as exc:
        raise WeatherRawIntegrityError(
            "KMA observation Bronze existing revision schema is incomplete"
        ) from exc


def _arrow_table(rows: Sequence[dict[str, object]]):
    import pyarrow as pa

    types = {
        "observed_at": pa.timestamp("us"),
        "nx": pa.int32(),
        "ny": pa.int32(),
        "observed_value": pa.float64(),
        "http_status": pa.int32(),
        "collected_at": pa.timestamp("us"),
    }
    fields = []
    arrays = []
    for column in OBSERVATION_BRONZE_COLUMNS:
        arrow_type = types.get(column, pa.string())
        fields.append(pa.field(column, arrow_type))
        arrays.append(pa.array([row[column] for row in rows], type=arrow_type))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def append_observation_bronze_revisions(
    table,
    rows: list[dict[str, object]],
    *,
    expected_grid_count: int = 80,
) -> int:
    """Atomically append only novel source revisions for one observed slot."""
    validate_observation_bronze_rows(
        rows,
        expected_grid_count=expected_grid_count,
    )
    source_id = str(rows[0]["source_id"])
    observed_slot = str(rows[0]["observed_slot"])
    observed_at = rows[0]["observed_at"]
    if not isinstance(observed_at, datetime):
        raise WeatherCompletenessError(
            "KMA observation Bronze observed_at must be a datetime"
        )
    existing_rows = _existing_observation_rows(
        table,
        source_id=source_id,
        observed_slot=observed_slot,
        observed_at=observed_at,
    )
    existing_by_key: dict[str, dict[str, object]] = {}
    for existing in existing_rows:
        key = str(existing.get("idempotency_key") or "")
        if not key:
            raise WeatherRawIntegrityError(
                "KMA observation Bronze existing revision lacks identity"
            )
        if key in existing_by_key:
            raise WeatherCompletenessError(
                "KMA observation Bronze existing idempotency keys are duplicated"
            )
        existing_by_key[key] = existing

    novel_rows: list[dict[str, object]] = []
    for row in rows:
        key = str(row["idempotency_key"])
        existing = existing_by_key.get(key)
        if existing is None:
            novel_rows.append(row)
            continue
        if _idempotent_content(existing) != _idempotent_content(row):
            raise WeatherRawIntegrityError(
                "KMA observation Bronze identity has conflicting content"
            )

    if not novel_rows:
        return 0
    with table.transaction() as transaction:
        transaction.append(_arrow_table(novel_rows))
    return len(novel_rows)


def load_observation_bronze_table(schema: str):
    """Load the dedicated table through the configured Weather REST catalog."""
    from weather_ingest.bronze_pyiceberg import _pyiceberg_catalog

    return _pyiceberg_catalog().load_table(
        f"{schema}.{OBSERVATION_BRONZE_TABLE}"
    )


def verify_observation_bronze_run_slot(
    cursor,
    *,
    qualified_table: str,
    observed_slot: str,
    expected_grid_revisions: Sequence[dict[str, object]],
    expected_grid_count: int = 80,
) -> int:
    """Verify exact slot revisions through a partition-prunable predicate."""
    try:
        slot = datetime.fromisoformat(observed_slot)
    except (TypeError, ValueError) as exc:
        raise WeatherCompletenessError(
            "KMA observation Bronze observed_slot must be timezone-aware ISO-8601"
        ) from exc
    if slot.tzinfo is None or slot.utcoffset() is None:
        raise WeatherCompletenessError(
            "KMA observation Bronze observed_slot must be timezone-aware ISO-8601"
        )
    observed_at_literal = (
        slot.astimezone(timezone.utc)
        .replace(tzinfo=None)
        .isoformat(sep=" ", timespec="microseconds")
    )
    revision_scope: list[tuple[str, str]] = []
    seen_grids: set[str] = set()
    for entry in expected_grid_revisions:
        if not isinstance(entry, dict):
            raise WeatherCompletenessError(
                "KMA observation Bronze expected revision scope is invalid"
            )
        grid_id = str(entry.get("grid_id") or "").strip()
        source_revision = str(entry.get("source_revision") or "").strip()
        if not grid_id or not source_revision or grid_id in seen_grids:
            raise WeatherCompletenessError(
                "KMA observation Bronze expected revision scope is invalid"
            )
        seen_grids.add(grid_id)
        revision_scope.append((grid_id, source_revision))
    if len(revision_scope) != expected_grid_count:
        raise WeatherCompletenessError(
            "KMA observation Bronze expected revision scope does not match"
        )

    expected_values = ",\n".join(
        f"({sql_string(grid_id)}, {sql_string(source_revision)})"
        for grid_id, source_revision in sorted(revision_scope)
    )
    category_values = ", ".join(sql_string(value) for value in REQUIRED_CATEGORIES)
    cursor.execute(
        f"""
        WITH expected (grid_id, source_revision) AS (
            VALUES {expected_values}
        ),
        scoped AS (
            SELECT
                actual.source_id,
                actual.observed_slot,
                actual.grid_id,
                actual.nx,
                actual.ny,
                actual.category,
                actual.source_revision,
                actual.idempotency_key,
                count(*) OVER (PARTITION BY actual.grid_id) AS categories_per_grid
            FROM {qualified_table} AS actual
            JOIN expected
              ON actual.grid_id = expected.grid_id
             AND actual.source_revision = expected.source_revision
            WHERE actual.source_id = {sql_string(SOURCE_ID)}
              AND actual.observed_slot = {sql_string(observed_slot)}
              AND actual.observed_at = TIMESTAMP {sql_string(observed_at_literal)}
        )
        SELECT
            count(*) AS row_count,
            count(DISTINCT grid_id) AS grid_count,
            count(DISTINCT category) AS category_count,
            count(DISTINCT observed_slot) AS slot_count,
            count(DISTINCT source_id) AS source_count,
            count(DISTINCT concat(grid_id, ':', source_revision))
                AS grid_revision_count,
            count(DISTINCT idempotency_key) AS idempotency_count,
            min(categories_per_grid) AS categories_per_grid_min,
            max(categories_per_grid) AS categories_per_grid_max,
            count_if(category NOT IN ({category_values})) AS invalid_category_count
        FROM scoped
        """
    )
    row = cursor.fetchone()
    if row is None or len(row) != 10:
        raise WeatherCompletenessError(
            "KMA observation Bronze verification returned no aggregate"
        )
    names = (
        "row_count",
        "grid_count",
        "category_count",
        "slot_count",
        "source_count",
        "grid_revision_count",
        "idempotency_count",
        "categories_per_grid_min",
        "categories_per_grid_max",
        "invalid_category_count",
    )
    values = {name: int(value or 0) for name, value in zip(names, row, strict=True)}
    expected_rows = expected_grid_count * len(REQUIRED_CATEGORIES)
    expected = {
        "row_count": expected_rows,
        "grid_count": expected_grid_count,
        "category_count": len(REQUIRED_CATEGORIES),
        "slot_count": 1,
        "source_count": 1,
        "grid_revision_count": expected_grid_count,
        "idempotency_count": expected_rows,
        "categories_per_grid_min": len(REQUIRED_CATEGORIES),
        "categories_per_grid_max": len(REQUIRED_CATEGORIES),
        "invalid_category_count": 0,
    }
    for name, expected_value in expected.items():
        if values[name] != expected_value:
            raise WeatherCompletenessError(
                "KMA observation Bronze verification failed: "
                f"{name}={values[name]}, expected={expected_value}"
            )
    return values["row_count"]


__all__ = [
    "OBSERVATION_BRONZE_COLUMNS",
    "OBSERVATION_BRONZE_TABLE",
    "build_observation_bronze_rows",
    "create_observation_bronze_table",
    "load_observation_bronze_table",
    "observation_grid_revisions",
    "append_observation_bronze_revisions",
    "validate_observation_bronze_rows",
    "verify_observation_bronze_run_slot",
]
