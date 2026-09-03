"""Shared Airflow Asset URIs for ask-seoul DAG dependencies."""

WEATHER_BRONZE_ASSET = "iceberg://weather/bronze"
WEATHER_GOLD_PUBLICATION_READY_ASSET = "iceberg://weather/gold/publication-ready"
WEATHER_FORECAST_QUALITY_READY_ASSET = "iceberg://weather/gold/forecast-quality-ready"
TRAFFIC_BRONZE_ASSET = "iceberg://traffic/bronze"
CITYDATA_BRONZE_ASSET = "iceberg://citydata/bronze"
