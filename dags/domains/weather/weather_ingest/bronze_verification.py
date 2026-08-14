"""Runtime verification for materialized KMA Bronze runs."""

from __future__ import annotations

from weather_ingest.bronze_contract import BRONZE_TABLE, BronzeValidationError
from weather_ingest.common.runtime import sql_string, trino_cursor
from weather_ingest.kma import SOURCE_ID


def verify_kma_bronze_runtime(
    raw_object_key: str | None = None,
    raw_object_keys: list[str] | None = None,
    dag_run_id: str | None = None,
    expected_rows: int | None = None,
    expected_raw_objects: int | None = None,
) -> int:
    cursor, catalog, schema = trino_cursor()
    qualified_table = f"{catalog}.{schema}.{BRONZE_TABLE}"
    filters = [f"source_id = {sql_string(SOURCE_ID)}"]
    if raw_object_keys:
        raw_key_values = ", ".join(sql_string(key) for key in raw_object_keys)
        filters.append(f"raw_object_key IN ({raw_key_values})")
    elif raw_object_key:
        filters.append(f"raw_object_key = {sql_string(raw_object_key)}")
    if dag_run_id:
        filters.append(f"dag_run_id = {sql_string(dag_run_id)}")
    cursor.execute(
        f"""
        SELECT
            count(*) AS row_count,
            count(DISTINCT raw_object_key) AS raw_object_count,
            max(collected_at) AS last_collected_at
        FROM {qualified_table}
        WHERE {" AND ".join(filters)}
        """
    )
    row = cursor.fetchone()
    row_count = int(row[0])
    if expected_rows is not None and row_count != expected_rows:
        raise BronzeValidationError(
            f"KMA bronze verification failed: expected_rows={expected_rows}, actual_rows={row_count}"
        )
    if expected_raw_objects is not None and int(row[1]) != expected_raw_objects:
        raise BronzeValidationError(
            "KMA bronze verification failed: "
            f"expected_raw_objects={expected_raw_objects}, actual_raw_objects={row[1]}"
        )
    if (
        expected_raw_objects is None
        and expected_rows
        and raw_object_key
        and int(row[1]) != 1
    ):
        raise BronzeValidationError(
            f"KMA bronze verification failed: raw_object_count={row[1]}"
        )
    print(
        "weather_vilage_fcst_bronze "
        f"row_count={row[0]} raw_object_count={row[1]} last_collected_at={row[2]}"
    )
    return row_count
