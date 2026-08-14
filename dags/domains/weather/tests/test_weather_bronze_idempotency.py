"""KMA Bronze idempotent Trino write tests."""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "domains" / "weather"))

from weather_bronze_test_support import RecordingCursor  # noqa: E402
from weather_ingest.bronze import (  # noqa: E402
    insert_kma_bronze_row_batches,
    insert_kma_bronze_rows,
)
from weather_ingest.bronze_contract import KMA_BRONZE_COLUMNS  # noqa: E402


def test_trino_bronze_insert_has_one_canonical_serializer_and_column_owner():
    source = (
        Path(__file__).resolve().parents[1] / "weather_ingest" / "bronze_trino.py"
    ).read_text(encoding="utf-8")

    assert source.count("INSERT INTO {qualified_table}") == 1
    assert source.count("sql_string(row.get") == 0


def test_kma_insert_replaces_same_retry_scope_before_append():
    cursor = RecordingCursor()

    inserted = insert_kma_bronze_rows(
        cursor=cursor,
        qualified_table="iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst",
        rows=[
            {
                "baseDate": "20260701",
                "baseTime": "0800",
                "nx": "60",
                "ny": "127",
                "category": "TMP",
                "fcstDate": "20260701",
                "fcstTime": "0900",
                "fcstValue": "25",
            }
        ],
        metadata={
            "result_code": "00",
            "result_msg": "NORMAL_SERVICE",
            "total_count": 1,
            "row_count": 1,
        },
        request_id="request-1",
        place_id="seoul-test-grid",
        base_date="20260701",
        base_time="0800",
        nx=60,
        ny=127,
        raw_object_key="raw/weather/kma/request-1.json",
        raw_hash="abc123",
        http_status=200,
        collected_at=datetime(2026, 7, 1, 0, 20, tzinfo=timezone.utc),
        dag_run_id="scheduled__2026-07-01T08:20:00+09:00",
    )

    assert inserted == 1
    assert len(cursor.statements) == 2
    delete_sql, insert_sql = cursor.statements
    assert delete_sql.startswith(
        "DELETE FROM iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst WHERE"
    )
    assert "source_id = 'kma_vilage_fcst'" in delete_sql
    assert "dag_run_id = 'scheduled__2026-07-01T08:20:00+09:00'" in delete_sql
    assert "base_date = '20260701'" in delete_sql
    assert "base_time = '0800'" in delete_sql
    assert "nx = 60" in delete_sql
    assert "ny = 127" in delete_sql
    assert insert_sql.startswith(
        "INSERT INTO iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst"
    )
    for column in KMA_BRONZE_COLUMNS:
        assert column in insert_sql
    assert "dag_run_id, page_no" in insert_sql


def test_kma_insert_batches_chunks_large_insert_without_repeating_delete():
    cursor = RecordingCursor()

    inserted = insert_kma_bronze_row_batches(
        cursor=cursor,
        qualified_table="iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst",
        dag_run_id="manual__chunk",
        max_insert_query_chars=1400,
        row_batches=[
            {
                "metadata": {
                    "result_code": "00",
                    "result_msg": "NORMAL_SERVICE",
                    "total_count": 4,
                    "row_count": 4,
                },
                "rows": [
                    {
                        "baseDate": "20260701",
                        "baseTime": "0800",
                        "nx": "60",
                        "ny": "127",
                        "category": f"T{i}",
                        "fcstDate": "20260701",
                        "fcstTime": "0900",
                        "fcstValue": "25",
                    }
                    for i in range(4)
                ],
                "request_id": "request-page-1",
                "place_id": "seoul-test-grid",
                "base_date": "20260701",
                "base_time": "0800",
                "nx": 60,
                "ny": 127,
                "raw_object_key": "raw/weather/kma/request-1.json",
                "raw_hash": "abc",
                "http_status": 200,
                "collected_at": datetime(2026, 7, 1, 0, 20, tzinfo=timezone.utc),
                "page_no": 1,
                "num_of_rows": 1000,
            }
        ],
    )

    delete_statements = [sql for sql in cursor.statements if sql.startswith("DELETE")]
    insert_statements = [sql for sql in cursor.statements if sql.startswith("INSERT")]
    assert inserted == 4
    assert len(delete_statements) == 1
    assert len(insert_statements) > 1


def test_kma_insert_fails_before_delete_when_response_is_partial():
    cursor = RecordingCursor()

    with pytest.raises(RuntimeError, match="total_count=2, parsed row_count=1"):
        insert_kma_bronze_rows(
            cursor=cursor,
            qualified_table="iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst",
            rows=[
                {
                    "baseDate": "20260701",
                    "baseTime": "0800",
                    "nx": "60",
                    "ny": "127",
                    "category": "TMP",
                    "fcstDate": "20260701",
                    "fcstTime": "0900",
                    "fcstValue": "25",
                }
            ],
            metadata={
                "result_code": "00",
                "result_msg": "NORMAL_SERVICE",
                "total_count": 2,
                "row_count": 1,
            },
            request_id="request-1",
            place_id="seoul-test-grid",
            base_date="20260701",
            base_time="0800",
            nx=60,
            ny=127,
            raw_object_key="raw/weather/kma/request-1.json",
            raw_hash="abc123",
            http_status=200,
            collected_at=datetime(2026, 7, 1, 0, 20, tzinfo=timezone.utc),
            dag_run_id="scheduled__2026-07-01T08:20:00+09:00",
        )

    assert cursor.statements == []


def test_kma_insert_allows_partial_page_only_when_dag_aggregate_was_checked():
    cursor = RecordingCursor()

    inserted = insert_kma_bronze_rows(
        cursor=cursor,
        qualified_table="iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst",
        rows=[
            {
                "baseDate": "20260701",
                "baseTime": "1700",
                "nx": "60",
                "ny": "127",
                "category": "TMP",
                "fcstDate": "20260701",
                "fcstTime": "1800",
                "fcstValue": "25",
            }
        ],
        metadata={
            "result_code": "00",
            "result_msg": "NORMAL_SERVICE",
            "total_count": 1001,
            "row_count": 1,
        },
        request_id="request-page-2",
        place_id="seoul-test-grid",
        base_date="20260701",
        base_time="1700",
        nx=60,
        ny=127,
        raw_object_key="raw/weather/kma/request-page-2.json",
        raw_hash="def456",
        http_status=200,
        collected_at=datetime(2026, 7, 1, 8, 20, tzinfo=timezone.utc),
        dag_run_id="scheduled__2026-07-01T17:20:00+09:00",
        page_no=2,
        num_of_rows=1000,
        delete_existing=False,
        allow_partial_page=True,
    )

    assert inserted == 1
    assert len(cursor.statements) == 1
    assert cursor.statements[0].startswith(
        "INSERT INTO iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst"
    )
    assert '"pageNo": "2"' in cursor.statements[0]
    assert '"numOfRows": "1000"' in cursor.statements[0]


def test_kma_insert_batches_deletes_once_and_inserts_all_rows():
    cursor = RecordingCursor()

    inserted = insert_kma_bronze_row_batches(
        cursor=cursor,
        qualified_table="iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst",
        dag_run_id="manual__batch",
        row_batches=[
            {
                "metadata": {
                    "result_code": "00",
                    "result_msg": "NORMAL_SERVICE",
                    "total_count": 2,
                    "row_count": 2,
                },
                "rows": [
                    {
                        "baseDate": "20260701",
                        "baseTime": "0800",
                        "nx": "60",
                        "ny": "127",
                        "category": "TMP",
                        "fcstDate": "20260701",
                        "fcstTime": "0900",
                        "fcstValue": "25",
                    },
                    {
                        "baseDate": "20260701",
                        "baseTime": "0800",
                        "nx": "60",
                        "ny": "127",
                        "category": "REH",
                        "fcstDate": "20260701",
                        "fcstTime": "0900",
                        "fcstValue": "70",
                    },
                ],
                "request_id": "request-page-1",
                "place_id": "seoul-test-grid",
                "base_date": "20260701",
                "base_time": "0800",
                "nx": 60,
                "ny": 127,
                "raw_object_key": "raw/weather/kma/request-1.json",
                "raw_hash": "abc",
                "http_status": 200,
                "collected_at": datetime(2026, 7, 1, 0, 20, tzinfo=timezone.utc),
                "page_no": 1,
                "num_of_rows": 1000,
            },
            {
                "metadata": {
                    "result_code": "00",
                    "result_msg": "NORMAL_SERVICE",
                    "total_count": 2,
                    "row_count": 1,
                },
                "rows": [
                    {
                        "baseDate": "20260701",
                        "baseTime": "0800",
                        "nx": "60",
                        "ny": "127",
                        "category": "TMP",
                        "fcstDate": "20260702",
                        "fcstTime": "0900",
                        "fcstValue": "23",
                    }
                ],
                "request_id": "request-page-2",
                "place_id": "seoul-test-grid",
                "base_date": "20260701",
                "base_time": "0800",
                "nx": 60,
                "ny": 127,
                "raw_object_key": "raw/weather/kma/request-2.json",
                "raw_hash": "def",
                "http_status": 200,
                "collected_at": datetime(2026, 7, 1, 0, 21, tzinfo=timezone.utc),
                "page_no": 2,
                "num_of_rows": 1000,
            },
        ],
    )

    assert inserted == 3
    assert len(cursor.statements) == 2
    delete_sql, insert_sql = cursor.statements
    assert delete_sql.startswith(
        "DELETE FROM iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst WHERE"
    )
    assert "source_id = 'kma_vilage_fcst'" in delete_sql
    assert "dag_run_id = 'manual__batch'" in delete_sql
    assert "base_date = '20260701'" in delete_sql
    assert "base_time = '0800'" in delete_sql
    assert "nx = 60" in delete_sql
    assert "ny = 127" in delete_sql
    assert "((base_date = '20260701'" in delete_sql
    assert "nx = 60" in delete_sql
    assert "ny = 127" in delete_sql
    assert insert_sql.startswith(
        "INSERT INTO iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst"
    )
