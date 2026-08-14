"""KMA Bronze schema and runtime-verification tests."""

import ast
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import weather_ingest.bronze as bronze  # noqa: E402
import weather_ingest.bronze_verification as bronze_verification  # noqa: E402
from weather_bronze_test_support import (  # noqa: E402
    RecordingCursor,
    VerificationCursor,
)
from weather_ingest.bronze import create_kma_bronze_table  # noqa: E402


def test_kma_verify_count_mismatch_is_permanent_validation_error(monkeypatch):
    cursor = VerificationCursor((4, 1, None))
    monkeypatch.setattr(
        bronze_verification,
        "trino_cursor",
        lambda: (cursor, "iceberg_dev", "weather"),
    )

    with pytest.raises(
        bronze.BronzeValidationError, match="expected_rows=5, actual_rows=4"
    ):
        bronze.verify_kma_bronze_runtime(expected_rows=5)


def test_kma_verify_connection_error_remains_retryable(monkeypatch):
    connection_error = ConnectionError("temporary Trino DNS failure")

    def fail_to_connect():
        raise connection_error

    monkeypatch.setattr(bronze_verification, "trino_cursor", fail_to_connect)

    with pytest.raises(ConnectionError) as raised:
        bronze.verify_kma_bronze_runtime(expected_rows=5)

    assert raised.value is connection_error


def test_airflow_wrapper_marks_only_validation_errors_non_retryable():
    dag_path = Path(__file__).resolve().parents[1] / "weather_vilage_fcst_bronze.py"
    tree = ast.parse(dag_path.read_text(encoding="utf-8"), filename=str(dag_path))
    wrapper = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "verify_kma_bronze_runtime"
    )
    assert any(
        isinstance(decorator, ast.Name) and decorator.id == "fail_fast_weather_bronze"
        for decorator in wrapper.decorator_list
    )


def test_kma_create_table_uses_load_date_partitioning_for_fresh_tables():
    cursor = RecordingCursor()

    qualified_table = create_kma_bronze_table(
        cursor, "iceberg_dev", "weather_traffic_bronze"
    )

    assert (
        qualified_table == "iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst"
    )
    assert len(cursor.statements) == 5
    assert (
        cursor.statements[0]
        == "CREATE SCHEMA IF NOT EXISTS iceberg_dev.weather_traffic_bronze"
    )
    assert (
        "CREATE TABLE IF NOT EXISTS iceberg_dev.weather_traffic_bronze.bronze_kma_vilage_fcst"
        in cursor.statements[1]
    )
    assert "partitioning = ARRAY['load_date']" in cursor.statements[1]
