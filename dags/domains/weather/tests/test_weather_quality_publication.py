from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from weather_quality_publication import (  # noqa: E402
    QualityPublicationError,
    create_quality_manifest_sql,
    publish_quality_success,
    quality_catalog,
    quality_manifest_relation,
)
from weather_quality_runtime import resolve_daily_quality_window  # noqa: E402


class FakeCursor:
    def __init__(self, existing: int = 0) -> None:
        self.existing = existing
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)

    def fetchone(self):
        return (self.existing,)


def _dbt_vars() -> dict[str, str]:
    return resolve_daily_quality_window(
        now=datetime(2026, 8, 22, 3, 5, tzinfo=timezone.utc),
        run_id="scheduled__quality",
    ).as_dbt_vars()


def test_manifest_sql_is_internal_partitioned_iceberg_metadata() -> None:
    sql = create_quality_manifest_sql("iceberg")

    assert "iceberg.weather.weather_forecast_quality_publication_manifest" in sql
    assert "partitioning = ARRAY['day(window_end_date)']" in sql
    assert "d1" not in sql.lower()
    assert "worker" not in sql.lower()


def test_success_publication_is_idempotent_and_only_uses_validated_vars() -> None:
    cursor = FakeCursor()
    result = publish_quality_success(
        cursor,
        dbt_vars=_dbt_vars(),
        catalog="iceberg",
        now=datetime(2026, 8, 22, 0, 0, tzinfo=timezone.utc),
    )

    assert result.created is True
    assert len(cursor.statements) == 3
    assert cursor.statements[0].startswith("CREATE TABLE IF NOT EXISTS")
    assert cursor.statements[1].startswith("SELECT count(*)")
    assert cursor.statements[2].startswith("INSERT INTO")
    assert "'SUCCESS'" in cursor.statements[2]

    existing = FakeCursor(existing=1)
    repeated = publish_quality_success(existing, dbt_vars=_dbt_vars(), catalog="iceberg")
    assert repeated.created is False
    assert len(existing.statements) == 2


@pytest.mark.parametrize("catalog", ["bad;catalog", "../iceberg", "iceberg.d1"])
def test_manifest_identifier_boundary_rejects_unsafe_catalogs(catalog: str) -> None:
    with pytest.raises(QualityPublicationError, match="unsafe quality publication catalog"):
        quality_manifest_relation(catalog)


def test_catalog_uses_the_configured_iceberg_catalog_only() -> None:
    assert quality_catalog({"TRINO_ICEBERG_CATALOG": "iceberg_dev"}) == "iceberg_dev"
