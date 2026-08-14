"""KMA Bronze schema, validation, and canonical record mapping."""

from __future__ import annotations

from datetime import timezone
from zoneinfo import ZoneInfo

from weather_ingest.common.runtime import create_schema_if_needed
from weather_ingest.errors import WeatherCompletenessError, WeatherSourceSchemaError
from weather_ingest.kma import SOURCE_ID, request_params_json


BRONZE_TABLE = "bronze_kma_vilage_fcst"
KST = ZoneInfo("Asia/Seoul")
KMA_BRONZE_COLUMNS = (
    "request_id",
    "source_id",
    "request_params_json",
    "place_id",
    "base_date",
    "base_time",
    "nx",
    "ny",
    "category",
    "fcst_date",
    "fcst_time",
    "fcst_value",
    "raw_object_key",
    "payload_hash",
    "http_status",
    "result_code",
    "result_msg",
    "total_count",
    "item_count",
    "collected_at",
    "load_date",
    "dag_run_id",
    "page_no",
)


class BronzeValidationError(WeatherCompletenessError):
    """Permanent KMA Bronze data-contract failure; retrying cannot repair it."""


def ensure_kma_bronze_schema(cursor, qualified_table: str) -> None:
    for column_name, column_type in (
        ("request_params_json", "varchar"),
        ("load_date", "varchar"),
        ("page_no", "integer"),
    ):
        cursor.execute(
            f"ALTER TABLE {qualified_table} ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
        )


def create_kma_bronze_table(cursor, catalog: str, schema: str) -> str:
    qualified_schema = f"{catalog}.{schema}"
    qualified_table = f"{qualified_schema}.{BRONZE_TABLE}"
    create_schema_if_needed(cursor, qualified_schema)
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table} (
            request_id varchar,
            source_id varchar,
            request_params_json varchar,
            place_id varchar,
            base_date varchar,
            base_time varchar,
            nx integer,
            ny integer,
            category varchar,
            fcst_date varchar,
            fcst_time varchar,
            fcst_value varchar,
            raw_object_key varchar,
            payload_hash varchar,
            http_status integer,
            result_code varchar,
            result_msg varchar,
            total_count integer,
            item_count integer,
            collected_at timestamp(6),
            load_date varchar,
            dag_run_id varchar,
            page_no integer
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['load_date']
        )
        """
    )
    ensure_kma_bronze_schema(cursor, qualified_table)
    return qualified_table


def metadata_int(metadata: dict, key: str) -> int:
    value = metadata.get(key)
    if value is None or value == "":
        return 0
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise WeatherSourceSchemaError(
            f"KMA metadata field must be an integer: {key}"
        ) from exc


def validate_kma_row_count(
    rows: list[dict],
    metadata: dict,
    nx: int,
    ny: int,
    *,
    allow_partial_page: bool = False,
) -> None:
    if not rows:
        raise WeatherCompletenessError("KMA API returned no forecast rows.")

    total_count = metadata_int(metadata, "total_count")
    parsed_count = len(rows)
    if total_count > parsed_count and not allow_partial_page:
        raise WeatherCompletenessError(
            "KMA bronze validation failed: "
            f"total_count={total_count}, parsed row_count={parsed_count}, nx={nx}, ny={ny}"
        )


def validate_kma_bronze_row_batch(batch: dict) -> None:
    validate_kma_row_count(
        batch["rows"],
        batch["metadata"],
        int(batch["nx"]),
        int(batch["ny"]),
        allow_partial_page=True,
    )


def iter_kma_bronze_records(row_batches: list[dict], dag_run_id: str):
    for batch in row_batches:
        metadata = batch["metadata"]
        rows = batch["rows"]
        base_date = batch["base_date"]
        base_time = batch["base_time"]
        nx = int(batch["nx"])
        ny = int(batch["ny"])
        collected_at = batch["collected_at"]
        validate_kma_bronze_row_batch(batch)
        request_params = request_params_json(
            base_date,
            base_time,
            nx,
            ny,
            page_no=batch.get("page_no"),
            num_of_rows=batch.get("num_of_rows"),
        )
        load_date = collected_at.astimezone(KST).strftime("%Y-%m-%d")
        collected_at_utc = collected_at.astimezone(timezone.utc).replace(tzinfo=None)
        for row in rows:
            yield {
                "request_id": batch["request_id"],
                "source_id": SOURCE_ID,
                "request_params_json": request_params,
                "place_id": batch["place_id"],
                "base_date": row.get("baseDate"),
                "base_time": row.get("baseTime"),
                "nx": int(row.get("nx")),
                "ny": int(row.get("ny")),
                "category": row.get("category"),
                "fcst_date": row.get("fcstDate"),
                "fcst_time": row.get("fcstTime"),
                "fcst_value": row.get("fcstValue"),
                "raw_object_key": batch["raw_object_key"],
                "payload_hash": batch["raw_hash"],
                "http_status": int(batch["http_status"]),
                "result_code": metadata.get("result_code"),
                "result_msg": metadata.get("result_msg"),
                "total_count": metadata_int(metadata, "total_count"),
                "item_count": metadata_int(metadata, "row_count"),
                "collected_at": collected_at_utc,
                "load_date": load_date,
                "dag_run_id": dag_run_id,
                "page_no": int(batch.get("page_no") or 1),
            }
