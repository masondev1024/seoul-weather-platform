from pathlib import Path

import pytest


WEATHER_MODELS = Path(__file__).resolve().parents[2] / "models" / "weather"
LEGACY_ADMIN_DONG_MODEL_PRESENT = bool(
    list(WEATHER_MODELS.rglob("silver_weather_forecast_by_admin_dong.sql"))
)


def read_model(name: str) -> str:
    matches = list(WEATHER_MODELS.rglob(f"{name}.sql"))
    assert len(matches) == 1
    return matches[0].read_text(encoding="utf-8")


def test_grid_silver_declares_incremental_merge_and_grain_key():
    sql = read_model("silver_kma_vilage_fcst")
    assert "materialized='incremental'" in sql
    assert "incremental_strategy='merge'" in sql
    assert (
        "unique_key=['place_id', 'nx', 'ny', 'issued_at', 'category', 'forecast_at']"
        in sql
    )
    # 증분 커서는 collected_at 워터마크 — dag_run_id 앙티조인 금지(DL-013 순환)
    assert "is_incremental()" in sql
    assert "max(collected_at)" in sql
    assert ">= (" in sql
    assert "weather_w1_lookback_minutes()" in sql
    assert "- interval '{{ weather_w1_lookback_minutes() }}' minute" in sql
    assert "var('weather_snapshot_load_date')" in sql
    assert "bronze.load_date = '{{ snapshot_load_date" in sql
    # R2 카탈로그 유령 뷰 409 우회(#70) + 스키마 드리프트 명시 실패(#137)
    assert "views_enabled=false" in sql
    assert "on_table_exists='drop'" in sql
    assert "on_schema_change='fail'" in sql


@pytest.mark.skipif(
    not LEGACY_ADMIN_DONG_MODEL_PRESENT,
    reason="Weather-only serving graph excludes the legacy unbounded admin-dong Silver.",
)
def test_admin_dong_silver_declares_incremental_merge_and_grain_key():
    sql = read_model("silver_weather_forecast_by_admin_dong")
    assert "materialized='incremental'" in sql
    assert "incremental_strategy='merge'" in sql
    assert "unique_key=['place_id', 'issued_at', 'forecast_at', 'category']" in sql
    assert "is_incremental()" in sql
    assert "max(collected_at)" in sql
    assert ">= (" in sql
    assert "weather_w1_lookback_minutes()" in sql
    assert "- interval '{{ weather_w1_lookback_minutes() }}' minute" in sql
    assert "views_enabled=false" in sql
    assert "on_table_exists='drop'" in sql
    assert "on_schema_change='fail'" in sql


@pytest.mark.skipif(
    not LEGACY_ADMIN_DONG_MODEL_PRESENT,
    reason="Weather-only serving graph excludes the legacy unbounded admin-dong Silver.",
)
def test_admin_dong_dedup_selects_grid_winner_before_place_fanout():
    sql = read_model("silver_weather_forecast_by_admin_dong")

    # A place belongs to one grid, so select the source winner at the native
    # grid grain before expanding it to places. This prevents a wide window or
    # duplicate source join after fanout from exhausting Trino memory.
    assert "selected_grid_forecast as (" in sql
    assert "max_by(" in sql
    assert "group by nx, ny, category, issued_at, forecast_at" in sql
    assert "joined_payload as (" in sql
    assert "from selected_grid_forecast as grid_forecast" in sql
    assert "row_number() over" not in sql
