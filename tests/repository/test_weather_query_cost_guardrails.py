from __future__ import annotations

from pathlib import Path

from tools.weather_query_cost_guardrails import (
    validate_model_sql,
    validate_weather_public_products,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_weather_public_products_keep_bounded_upstream_relations() -> None:
    assert validate_weather_public_products(REPOSITORY_ROOT) == []


def test_cost_guard_rejects_unbounded_or_unapproved_product_sql() -> None:
    sql = """
    {{ config(materialized='table', full_refresh=true) }}
    select * from {{ ref('silver_kma_vilage_fcst') }}
    """

    errors = validate_model_sql("gold_weather_place_current_outlook", sql)

    assert errors == [
        "gold_weather_place_current_outlook: expected refs "
        "['gold_weather_place_hourly_outlook'], got ['silver_kma_vilage_fcst']",
        "gold_weather_place_current_outlook: full_refresh=true is forbidden",
        "gold_weather_place_current_outlook: missing bounded forecast window marker "
        "hourly.forecast_at >= kst_now.current_hour_at",
        "gold_weather_place_current_outlook: direct select * from is forbidden",
    ]
