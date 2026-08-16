-- Bounded forecast/category source for Grid serving Gold products.
{{ config(materialized='table') }}

select
    grid_id,
    coverage_scope,
    source_grid_place_id,
    nx,
    ny,
    request_id,
    source_id,
    request_params_json,
    category,
    issued_at,
    forecast_at,
    event_at,
    time_bucket,
    fcst_value_raw,
    fcst_value_num,
    raw_object_key,
    payload_hash,
    total_count,
    item_count,
    load_date,
    collected_at,
    dag_run_id
from {{ ref('silver_weather_forecast_by_coverage_grid_serving') }}
