import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.run_manifest import WeatherRun, WeatherRunManifest  # noqa: E402


class RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql):
        self.statements.append(" ".join(sql.split()))


def test_success_manifest_event_marks_run_publishable():
    cursor = RecordingCursor()
    manifest = WeatherRunManifest(
        lambda: (cursor, "iceberg_dev", "weather_traffic_bronze")
    )

    qualified_table = manifest.publish(
        WeatherRun("weather_vilage_fcst_bronze", "manual__weather_success"),
        expected_rows=798,
        actual_rows=798,
        expected_raw_objects=1,
        actual_raw_objects=1,
    )

    assert (
        qualified_table
        == "iceberg_dev.weather_traffic_bronze.bronze_collection_run_manifest"
    )
    assert len(cursor.statements) == 3
    assert (
        "CREATE TABLE IF NOT EXISTS iceberg_dev.weather_traffic_bronze.bronze_collection_run_manifest"
        in cursor.statements[1]
    )
    assert cursor.statements[2].startswith("MERGE INTO ")
    assert "'SUCCESS'" in cursor.statements[2]
    assert "'weather_vilage_fcst_bronze'" in cursor.statements[2]
    assert "true" in cursor.statements[2]
    assert "798, 798, 1, 1" in cursor.statements[2]
