from __future__ import annotations

from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[2]
MODEL = (
    PROJECT_DIR
    / "models"
    / "weather"
    / "transform"
    / "place_mart"
    / "silver_weather_forecast_by_admin_dong_serving.sql"
)
GOLD = (
    PROJECT_DIR
    / "models"
    / "weather"
    / "transform"
    / "gold"
    / "gold_weather_place_forecast_change_daily.sql"
)
SERVING_PLACE_FORECAST = (
    PROJECT_DIR
    / "models"
    / "weather"
    / "transform"
    / "place_mart"
    / "gold_weather_forecast_by_place_serving.sql"
)
HOURLY_OUTLOOK = (
    PROJECT_DIR
    / "models"
    / "weather"
    / "transform"
    / "gold"
    / "gold_weather_place_hourly_outlook.sql"
)


def test_serving_working_set_is_atomically_rebuilt_and_bounded_before_fanout() -> None:
    sql = MODEL.read_text(encoding="utf-8")

    assert "materialized='table'" in sql
    assert "on_table_exists='rename'" in sql
    assert "incremental_strategy='merge'" not in sql
    assert "ARRAY['day(forecast_at)']" in sql
    assert "cast(forecast_at as date) >= kst_window.min_forecast_date" in sql
    assert "weather_serving_as_of_hour()" in sql
    assert "interval '24' hour as min_issued_at" in sql
    assert "issued_at >= kst_window.min_issued_at" in sql
    assert "max_by(" in sql
    assert "bounded_grid_forecast as" in sql
    assert "dense_rank() over" in sql
    assert "partition by nx, ny, forecast_at" in sql
    assert "where issue_rank <= 2" in sql
    assert sql.index("selected_grid_forecast as") < sql.index(
        "bounded_grid_forecast as"
    )
    assert sql.index("bounded_grid_forecast as") < sql.index("joined_payload as")
    assert "row_number() over" not in sql


def test_public_forecast_change_reads_the_bounded_serving_relation() -> None:
    sql = GOLD.read_text(encoding="utf-8")

    assert "ref('silver_weather_forecast_by_admin_dong_serving')" in sql
    assert "ref('silver_weather_forecast_by_admin_dong')" not in sql


def test_other_weather_d1_products_use_the_same_bounded_working_set() -> None:
    serving_place_forecast = SERVING_PLACE_FORECAST.read_text(encoding="utf-8")
    hourly_outlook = HOURLY_OUTLOOK.read_text(encoding="utf-8")

    assert "ref('silver_weather_forecast_by_admin_dong_serving')" in serving_place_forecast
    assert "ref('silver_weather_forecast_by_admin_dong')" not in serving_place_forecast
    assert "ref('gold_weather_forecast_by_place_serving')" in hourly_outlook
    assert "ref('gold_weather_forecast_by_place')" not in hourly_outlook
