-- Bounded place × forecast/category input for the four Weather D1 products.
-- The legacy place mart retains its historical source; this table deliberately
-- consumes the serving working set so a publication cycle never scans it.

{{ config(materialized='table') }}

select
    place_id,
    place_name,
    alias_names,
    gu,
    admin_dong,
    latitude,
    longitude,
    admin_dong_code,
    gu_code,
    nx,
    ny,
    mapping_method,
    grid_distance_m,
    source_admin_code,
    source_grid_place_id,
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
from {{ ref('silver_weather_forecast_by_admin_dong_serving') }}
