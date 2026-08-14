from __future__ import annotations

import sys
import inspect
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.run_manifest import (  # noqa: E402
    RunNotPublishableError,
    STATUS_FAILED,
    STATUS_COALESCED,
    STATUS_STARTED,
    STATUS_SUCCESS,
    WeatherRun,
    WeatherRunManifest,
)
from weather_ingest.errors import WeatherCompletenessError  # noqa: E402


class RecordingCursor:
    def __init__(self, rows: list[tuple] | None = None) -> None:
        self.statements: list[str] = []
        self.rows = list(rows or [])

    def execute(self, statement: str) -> None:
        self.statements.append(" ".join(statement.split()))

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


def test_manifest_status_constants_are_the_only_lifecycle_literals():
    assert (STATUS_STARTED, STATUS_SUCCESS, STATUS_FAILED, STATUS_COALESCED) == (
        "STARTED",
        "SUCCESS",
        "FAILED",
        "COALESCED",
    )
    assert "status=STATUS_STARTED" in inspect.getsource(WeatherRunManifest.start)
    assert "status=STATUS_SUCCESS" in inspect.getsource(WeatherRunManifest.complete)
    assert "status=STATUS_FAILED" in inspect.getsource(WeatherRunManifest.fail)
    assert "status=STATUS_COALESCED" in inspect.getsource(WeatherRunManifest.coalesce)


def test_publish_records_one_atomic_publishable_manifest_mutation():
    cursor = RecordingCursor()
    manifest = WeatherRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.publish(
        WeatherRun(
            dag_id="weather_vilage_fcst_bronze",
            run_id="scheduled__2026-07-14T02:20:00Z",
        ),
        expected_rows=1001,
        actual_rows=1001,
        expected_raw_objects=2,
        actual_raw_objects=2,
    )

    mutations = [
        statement
        for statement in cursor.statements
        if statement.startswith(("DELETE ", "INSERT ", "MERGE "))
    ]
    assert len(mutations) == 1
    assert mutations[0].startswith("MERGE INTO ")
    assert "'kma_vilage_fcst'" in mutations[0]
    assert "'SUCCESS'" in mutations[0]
    assert "true" in mutations[0]
    assert "1001, 1001, 2, 2" in mutations[0]


def test_require_publishable_returns_the_exact_verified_snapshot_id():
    cursor = RecordingCursor(rows=[("scheduled__weather-42",)])
    manifest = WeatherRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    assert manifest.require_publishable("scheduled__weather-42") == (
        "scheduled__weather-42"
    )
    assert "dag_run_id = 'scheduled__weather-42'" in cursor.statements[0]
    assert "is_publishable" in cursor.statements[0]


def test_require_publishable_fails_when_the_exact_snapshot_is_not_verified():
    cursor = RecordingCursor()
    manifest = WeatherRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    with pytest.raises(RunNotPublishableError, match="not publishable"):
        manifest.require_publishable("scheduled__weather-missing")


def test_coalesce_records_replacement_identity_without_marking_snapshot_publishable():
    cursor = RecordingCursor()
    manifest = WeatherRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.coalesce(
        "scheduled__weather-old", replacement_run_id="scheduled__weather-new"
    )

    mutation = next(
        statement for statement in cursor.statements if statement.startswith("MERGE ")
    )
    assert f"'{STATUS_COALESCED}'" in mutation
    assert "false" in mutation
    assert "replaced_by=scheduled__weather-new" in mutation


def test_complete_records_subset_repair_success_as_nonpublishable():
    cursor = RecordingCursor()
    manifest = WeatherRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.complete(
        WeatherRun("weather_vilage_fcst_bronze_backfill", "manual__subset"),
        expected_rows=1001,
        actual_rows=1000,
        expected_raw_objects=2,
        actual_raw_objects=1,
        is_publishable=False,
    )

    mutation = next(
        statement for statement in cursor.statements if statement.startswith("MERGE ")
    )
    assert "'SUCCESS'" in mutation
    assert "false" in mutation
    assert "1001, 1000, 2, 1" in mutation


def test_complete_rejects_publishable_count_mismatch_before_sql():
    cursor_factory_calls = []
    manifest = WeatherRunManifest(
        cursor_factory=lambda: cursor_factory_calls.append(True)
    )

    with pytest.raises(WeatherCompletenessError, match="publishable"):
        manifest.complete(
            WeatherRun("weather_vilage_fcst_bronze", "scheduled__mismatch"),
            expected_rows=1001,
            actual_rows=1000,
            expected_raw_objects=2,
            actual_raw_objects=1,
            is_publishable=True,
        )

    assert cursor_factory_calls == []


def test_fail_records_error_type_without_leaking_exception_message():
    cursor = RecordingCursor()
    manifest = WeatherRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.fail(
        WeatherRun(dag_id="weather_vilage_fcst_bronze", run_id="manual__failed"),
        task_id="land_kma_raw",
        error=RuntimeError("KMA_SERVICE_KEY=do-not-store"),
        expected_raw_objects=80,
        actual_raw_objects=42,
    )

    mutation = next(
        statement for statement in cursor.statements if statement.startswith("MERGE ")
    )
    assert "'FAILED'" in mutation
    assert "false" in mutation
    assert "RuntimeError in land_kma_raw" in mutation
    assert "do-not-store" not in mutation


def test_start_records_expected_grid_slots_without_marking_run_publishable():
    cursor = RecordingCursor()
    manifest = WeatherRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    manifest.start(
        WeatherRun(dag_id="weather_vilage_fcst_bronze", run_id="manual__started"),
        expected_raw_objects=80,
    )

    mutation = next(
        statement for statement in cursor.statements if statement.startswith("MERGE ")
    )
    assert "'STARTED'" in mutation
    assert "false" in mutation
    assert "NULL, NULL, 80, NULL" in mutation


def test_manifest_tolerates_iceberg_namespace_creation_race():
    class NamespaceRaceCursor(RecordingCursor):
        def execute(self, statement: str) -> None:
            if statement.startswith("CREATE SCHEMA"):
                raise RuntimeError("Namespace already exists")
            super().execute(statement)

    cursor = NamespaceRaceCursor()

    WeatherRunManifest(
        cursor_factory=lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    ).start(WeatherRun("weather_vilage_fcst_bronze", "manual__race"))

    assert any(statement.startswith("CREATE TABLE") for statement in cursor.statements)
    assert any(statement.startswith("MERGE ") for statement in cursor.statements)
