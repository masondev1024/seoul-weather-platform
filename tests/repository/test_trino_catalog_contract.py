from __future__ import annotations

from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
MACRO_PATH = (
    REPOSITORY_ROOT
    / "dbt"
    / "domains"
    / "traffic_weather"
    / "macros"
    / "trino__list_relations_without_caching.sql"
)


def test_weather_trino_relation_listing_guards_missing_namespaces() -> None:
    """A missing Iceberg namespace must not call the R2 list-tables endpoint."""

    macro = MACRO_PATH.read_text(encoding="utf-8")

    probe_start = macro.index(".schemata")
    missing_namespace_return = macro.index("return([])", probe_start)
    materialized_view_listing = macro.index("system.metadata.materialized_views")

    assert probe_start < missing_namespace_return < materialized_view_listing
    assert "where schema_name =" in macro
    assert "from {{ relation.information_schema() }}.tables" in macro
