import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_ingest.iceberg_maintenance import (  # noqa: E402
    FIXED_RETENTION,
    MAINTAINED_TABLES,
    OPERATIONS,
    QUALITY_FIXED_RETENTION,
    QUALITY_OPTIMIZE_FILE_SIZE_THRESHOLD,
    QUALITY_RETAIN_LAST,
    RETAIN_LAST,
    MaintainedTable,
    MaintenancePlanError,
    execute_maintenance_action,
    maintenance_catalog,
    operation_sql,
    qualified_name,
    resolve_maintained_tables,
    table_exists_sql,
)


class FakeCursor:
    def __init__(self, exists=True):
        self.exists = exists
        self.statements = []
        self._last_was_probe = False

    def execute(self, statement):
        self.statements.append(statement)
        self._last_was_probe = statement.strip().upper().startswith("SELECT")

    def fetchall(self):
        if self._last_was_probe:
            return [(1,)] if self.exists else []
        return []


def test_operation_order_is_optimize_then_expire_then_orphan():
    # 순서가 바뀌면 참조 중 파일을 지우거나 합치기 전에 만료하게 된다.
    assert OPERATIONS == ("optimize", "expire_snapshots", "remove_orphan_files")


def test_operation_sql_uses_fixed_conservative_retention():
    table = MaintainedTable("weather", "gold_weather_place_hourly_outlook")
    assert (
        operation_sql("iceberg", table, "optimize")
        == "ALTER TABLE iceberg.weather.gold_weather_place_hourly_outlook EXECUTE optimize"
    )
    assert operation_sql("iceberg", table, "expire_snapshots") == (
        "ALTER TABLE iceberg.weather.gold_weather_place_hourly_outlook "
        f"EXECUTE expire_snapshots(retention_threshold => '{FIXED_RETENTION}', "
        f"retain_last => {RETAIN_LAST})"
    )
    assert operation_sql("iceberg", table, "remove_orphan_files") == (
        "ALTER TABLE iceberg.weather.gold_weather_place_hourly_outlook "
        f"EXECUTE remove_orphan_files(retention_threshold => '{FIXED_RETENTION}')"
    )


def test_retention_is_seven_days_retain_one():
    assert FIXED_RETENTION == "7d"
    assert RETAIN_LAST == 1


def test_quality_tables_use_more_conservative_retention_and_small_file_optimize_threshold():
    assert QUALITY_FIXED_RETENTION == "30d"
    assert QUALITY_RETAIN_LAST == 7
    assert QUALITY_OPTIMIZE_FILE_SIZE_THRESHOLD == "32MB"

    table = MaintainedTable(
        "weather",
        "gold_weather_forecast_quality_daily_history",
        retention_threshold=QUALITY_FIXED_RETENTION,
        retain_last=QUALITY_RETAIN_LAST,
        optimize_file_size_threshold=QUALITY_OPTIMIZE_FILE_SIZE_THRESHOLD,
    )

    assert operation_sql("iceberg", table, "optimize") == (
        "ALTER TABLE iceberg.weather.gold_weather_forecast_quality_daily_history "
        "EXECUTE optimize(file_size_threshold => '32MB')"
    )
    assert operation_sql("iceberg", table, "expire_snapshots") == (
        "ALTER TABLE iceberg.weather.gold_weather_forecast_quality_daily_history "
        "EXECUTE expire_snapshots(retention_threshold => '30d', retain_last => 7)"
    )
    assert operation_sql("iceberg", table, "remove_orphan_files") == (
        "ALTER TABLE iceberg.weather.gold_weather_forecast_quality_daily_history "
        "EXECUTE remove_orphan_files(retention_threshold => '30d')"
    )


def test_operation_sql_rejects_unknown_operation():
    table = MaintainedTable("weather", "silver_kma_vilage_fcst")
    with pytest.raises(MaintenancePlanError):
        operation_sql("iceberg", table, "vacuum")


@pytest.mark.parametrize("bad", ["weather; drop", "we ather", "1weather", ""])
def test_qualified_name_rejects_unsafe_identifiers(bad):
    with pytest.raises(MaintenancePlanError):
        qualified_name("iceberg", MaintainedTable(bad, "t"))
    with pytest.raises(MaintenancePlanError):
        qualified_name("iceberg", MaintainedTable("weather", bad))
    with pytest.raises(MaintenancePlanError):
        qualified_name(bad, MaintainedTable("weather", "t"))


def test_maintenance_catalog_defaults_and_reads_env():
    assert maintenance_catalog({}) == "iceberg"
    assert maintenance_catalog({"TRINO_ICEBERG_CATALOG": "iceberg_dev"}) == "iceberg_dev"
    with pytest.raises(MaintenancePlanError):
        maintenance_catalog({"TRINO_ICEBERG_CATALOG": "bad;name"})


def test_maintained_tables_only_cover_fork_owned_schemas():
    # 공유 dim_admin_dong(iceberg.common)과 타 도메인 테이블은 포함하지 않는다.
    schemas = {t.schema for t in MAINTAINED_TABLES}
    assert schemas == {"weather", "weather_traffic_bronze"}
    names = {t.name for t in MAINTAINED_TABLES}
    assert "dim_admin_dong" not in names
    # bronze 원천과 대표 gold 상품이 실제로 들어 있다.
    assert MaintainedTable("weather_traffic_bronze", "bronze_kma_vilage_fcst") in MAINTAINED_TABLES
    assert MaintainedTable("weather", "gold_weather_place_hourly_outlook") in MAINTAINED_TABLES


def test_maintained_quality_tables_are_physical_only_and_exclude_views_d1_serving():
    expected_names = {
        "silver_kma_observation_truth",
        "silver_weather_quality_forecast_vintage",
        "silver_weather_forecast_observation_match",
        "gold_weather_forecast_quality_grid_score_history",
        "gold_weather_forecast_quality_hourly_history",
        "gold_weather_forecast_quality_daily_history",
        "weather_forecast_quality_publication_manifest",
    }
    quality_tables = {t.name: t for t in MAINTAINED_TABLES if t.name in expected_names}
    assert set(quality_tables) == expected_names
    assert "gold_weather_forecast_quality_grid_score" not in quality_tables
    assert "gold_weather_forecast_quality_hourly" not in quality_tables
    assert "gold_weather_forecast_quality_daily" not in quality_tables
    assert all("d1" not in table.name for table in MAINTAINED_TABLES)
    assert all(
        not table.name.endswith("_serving") for table in quality_tables.values()
    )
    assert all(
        table.retention_threshold == QUALITY_FIXED_RETENTION
        and table.retain_last == QUALITY_RETAIN_LAST
        and table.optimize_file_size_threshold == QUALITY_OPTIMIZE_FILE_SIZE_THRESHOLD
        for table in quality_tables.values()
    )


def test_resolve_maintained_tables_keeps_canonical_order_and_rejects_unknown():
    subset = ["weather.silver_kma_vilage_fcst", "weather_traffic_bronze.bronze_kma_vilage_fcst"]
    resolved = resolve_maintained_tables(subset)
    # canonical 순서 유지(bronze 가 silver 보다 앞).
    assert [t.label for t in resolved] == [
        "weather_traffic_bronze.bronze_kma_vilage_fcst",
        "weather.silver_kma_vilage_fcst",
    ]
    with pytest.raises(MaintenancePlanError):
        resolve_maintained_tables(["weather.not_ours"])


def test_execute_action_skips_missing_table_without_ddl():
    cursor = FakeCursor(exists=False)
    table = MaintainedTable("weather", "gold_weather_place_hourly_outlook")
    result = execute_maintenance_action(
        cursor, catalog="iceberg", table=table, operation="optimize"
    )
    assert result.status == "skipped_missing"
    # 존재 확인(SELECT)만 했고 ALTER 는 안 던졌다.
    assert all(not s.strip().upper().startswith("ALTER") for s in cursor.statements)


def test_execute_action_runs_ddl_when_table_exists():
    cursor = FakeCursor(exists=True)
    table = MaintainedTable("weather", "gold_weather_place_hourly_outlook")
    result = execute_maintenance_action(
        cursor, catalog="iceberg", table=table, operation="expire_snapshots"
    )
    assert result.status == "ok"
    alters = [s for s in cursor.statements if s.strip().upper().startswith("ALTER")]
    assert len(alters) == 1
    assert "expire_snapshots(retention_threshold => '7d', retain_last => 1)" in alters[0]


def test_table_exists_probe_is_read_only_and_scoped():
    table = MaintainedTable("weather", "silver_kma_vilage_fcst")
    sql = table_exists_sql("iceberg", table)
    assert sql.strip().upper().startswith("SELECT")
    assert "iceberg.information_schema.tables" in sql
    assert "table_schema = 'weather'" in sql
    assert "table_name = 'silver_kma_vilage_fcst'" in sql
