"""Publish the four Weather serving products through the common D1 publisher.

The ``Airflow`` token keeps this thin factory wrapper visible to DAG safe-mode
discovery even though Airflow imports live inside the common factory.
"""

from airflow.sdk import Asset

from common.assets import WEATHER_GOLD_PUBLICATION_READY_ASSET
from common.runtime_guard import default_target
from common.serving.dag_factory import build_serving_export_dag


dag = build_serving_export_dag(
    domain="weather",
    product_ids=[
        "weather_place_current_outlook",
        "weather_place_precipitation_window",
        "weather_place_risk_window",
        "weather_place_forecast_change_daily",
    ],
    exact_domain_contracts=True,
    # Mid-term outlook has an independent publisher; keep this DAG's four-product scope explicit.
    partitioned_domain_scope=True,
    require_public_projection=True,
    verify_content_parity=True,
    # Only the terminal marker runs after Gold write and contract test success.
    schedule=Asset(WEATHER_GOLD_PUBLICATION_READY_ASSET),
    dag_id="weather_serving_export",
    target=default_target(),
    schema="weather",
)
