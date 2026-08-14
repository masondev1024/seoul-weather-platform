"""Independently monitor the four Weather place products in prod D1 (#776)."""

from __future__ import annotations

from common.errors.airflow import problem_failure_callback
from common.runtime_guard import default_target
from common.serving.dag_factory import build_serving_freshness_watchdog_dag


WEATHER_PRODUCTS = [
    "weather_place_current_outlook",
    "weather_place_precipitation_window",
    "weather_place_risk_window",
    "weather_place_forecast_change_daily",
]
_KST_WALL_CLOCK_PRODUCTS = {
    # Weather serving freshness fields are timezone-naive Asia/Seoul wall-clock
    # timestamps by contract; the watchdog must not interpret them as UTC.
    product_id: "Asia/Seoul" for product_id in WEATHER_PRODUCTS
}

record_weather_serving_watchdog_problem = problem_failure_callback(
    domain="weather",
    source_system="serving_freshness_watchdog",
)

dag = build_serving_freshness_watchdog_dag(
    domain="weather",
    product_ids=WEATHER_PRODUCTS,
    schedule="35 * * * *",
    dag_id="weather_serving_freshness_watchdog",
    target=default_target(),
    exact_domain_contracts=True,
    naive_freshness_timezones=_KST_WALL_CLOCK_PRODUCTS,
    # Across 33 observed refresh-to-export prod cycles, p95 completion was 24.78
    # minutes and the maximum was 28.24. Running at :35 with five minutes of
    # publication grace (plus the factory's one five-minute task retry) avoids a
    # normal publication race while still detecting one missed hourly cycle.
    publication_grace_minutes=5,
    failure_callback=record_weather_serving_watchdog_problem,
)
