"""Trino SQL persistence for canonical KMA Bronze records."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from weather_ingest.bronze_contract import (
    KMA_BRONZE_COLUMNS,
    iter_kma_bronze_records,
    validate_kma_row_count,
)
from weather_ingest.common.runtime import sql_int, sql_string, sql_timestamp
from weather_ingest.kma import SOURCE_ID


MAX_KMA_INSERT_QUERY_CHARS = 900_000
_INTEGER_COLUMNS = frozenset(
    {"nx", "ny", "http_status", "total_count", "item_count", "page_no"}
)


def _sql_record_value(column: str, value: Any) -> str:
    if column in _INTEGER_COLUMNS:
        return sql_int(value)
    if column == "collected_at":
        collected_at = value
        if not isinstance(collected_at, datetime):
            raise TypeError("collected_at must be a datetime")
        if collected_at.tzinfo is None:
            collected_at = collected_at.replace(tzinfo=timezone.utc)
        return sql_timestamp(collected_at)
    return sql_string(value)


def _serialize_record(record: dict[str, Any]) -> str:
    return (
        "("
        + ", ".join(
            _sql_record_value(column, record.get(column))
            for column in KMA_BRONZE_COLUMNS
        )
        + ")"
    )


def _insert_prefix(qualified_table: str) -> str:
    columns = ",\n            ".join(KMA_BRONZE_COLUMNS)
    return f"""
        INSERT INTO {qualified_table} (
            {columns}
        )
        VALUES """


def _insert_records(
    cursor,
    qualified_table: str,
    records: list[dict[str, Any]],
    *,
    max_insert_query_chars: int,
) -> None:
    insert_prefix = _insert_prefix(qualified_table)
    max_values_chars = max(1, max_insert_query_chars - len(insert_prefix))
    chunk: list[str] = []
    chunk_chars = 0
    for value in map(_serialize_record, records):
        value_chars = len(value) + (2 if chunk else 0)
        if chunk and chunk_chars + value_chars > max_values_chars:
            cursor.execute(f"{insert_prefix}{', '.join(chunk)}")
            chunk = []
            chunk_chars = 0
            value_chars = len(value)
        chunk.append(value)
        chunk_chars += value_chars
    if chunk:
        cursor.execute(f"{insert_prefix}{', '.join(chunk)}")


def _row_batch(
    *,
    rows: list[dict],
    metadata: dict,
    request_id: str,
    place_id: str,
    base_date: str,
    base_time: str,
    nx: int,
    ny: int,
    raw_object_key: str,
    raw_hash: str,
    http_status: int,
    collected_at: datetime,
    page_no: int | None,
    num_of_rows: int | None,
) -> dict[str, Any]:
    return {
        "rows": rows,
        "metadata": metadata,
        "request_id": request_id,
        "place_id": place_id,
        "base_date": base_date,
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
        "raw_object_key": raw_object_key,
        "raw_hash": raw_hash,
        "http_status": http_status,
        "collected_at": collected_at,
        "page_no": page_no,
        "num_of_rows": num_of_rows,
    }


def insert_kma_bronze_rows(
    cursor,
    qualified_table: str,
    rows: list[dict],
    metadata: dict,
    request_id: str,
    place_id: str,
    base_date: str,
    base_time: str,
    nx: int,
    ny: int,
    raw_object_key: str,
    raw_hash: str,
    http_status: int,
    collected_at: datetime,
    dag_run_id: str,
    page_no: int | None = None,
    num_of_rows: int | None = None,
    delete_existing: bool = True,
    allow_partial_page: bool = False,
) -> int:
    validate_kma_row_count(
        rows, metadata, nx, ny, allow_partial_page=allow_partial_page
    )
    batch = _row_batch(
        rows=rows,
        metadata=metadata,
        request_id=request_id,
        place_id=place_id,
        base_date=base_date,
        base_time=base_time,
        nx=nx,
        ny=ny,
        raw_object_key=raw_object_key,
        raw_hash=raw_hash,
        http_status=http_status,
        collected_at=collected_at,
        page_no=page_no,
        num_of_rows=num_of_rows,
    )
    records = list(iter_kma_bronze_records([batch], dag_run_id))

    if delete_existing:
        cursor.execute(
            f"""
            DELETE FROM {qualified_table}
            WHERE source_id = {sql_string(SOURCE_ID)}
                AND dag_run_id = {sql_string(dag_run_id)}
                AND base_date = {sql_string(base_date)}
                AND base_time = {sql_string(base_time)}
                AND nx = {sql_int(nx)}
                AND ny = {sql_int(ny)}
            """
        )
    _insert_records(
        cursor,
        qualified_table,
        records,
        max_insert_query_chars=MAX_KMA_INSERT_QUERY_CHARS,
    )
    return len(records)


def insert_kma_bronze_row_batches(
    cursor,
    qualified_table: str,
    row_batches: list[dict],
    dag_run_id: str,
    *,
    delete_existing: bool = True,
    max_insert_query_chars: int = MAX_KMA_INSERT_QUERY_CHARS,
) -> int:
    if not row_batches:
        return 0

    records = list(iter_kma_bronze_records(row_batches, dag_run_id))
    grid_filters = {
        f"(base_date = {sql_string(batch['base_date'])} "
        f"AND base_time = {sql_string(batch['base_time'])} "
        f"AND nx = {sql_int(batch['nx'])} AND ny = {sql_int(batch['ny'])})"
        for batch in row_batches
    }
    if delete_existing:
        cursor.execute(
            f"""
            DELETE FROM {qualified_table}
            WHERE source_id = {sql_string(SOURCE_ID)}
                AND dag_run_id = {sql_string(dag_run_id)}
                AND ({" OR ".join(sorted(grid_filters))})
            """
        )
    _insert_records(
        cursor,
        qualified_table,
        records,
        max_insert_query_chars=max_insert_query_chars,
    )
    return len(records)
