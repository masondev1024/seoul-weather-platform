-- Serving working set for the public place forecast-change product.
-- Keep the legacy full-history relation intact; this relation contains only
-- yesterday onward forecast targets so normal publication never hashes the
-- expired-history target during MERGE.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['place_id', 'issued_at', 'forecast_at', 'category'],
    on_schema_change='fail',
    full_refresh=false,
    views_enabled=false,
    on_table_exists='drop',
    properties={
        "partitioning": "ARRAY['day(forecast_at)']"
    },
    pre_hook=[
        "{% if is_incremental() %}delete from {{ this }} where cast(forecast_at as date) < cast(current_timestamp at time zone 'Asia/Seoul' as date) - interval '1' day{% endif %}"
    ],
    tags=['ask_seoul_weather_transform_serving_place_mart']
) }}

with kst_window as (
    select cast(current_timestamp at time zone 'Asia/Seoul' as date) - interval '1' day as min_forecast_date
),

grid_forecast as (
    select
        request_id,
        source_id,
        request_params_json,
        place_id as source_grid_place_id,
        nx,
        ny,
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
    from {{ ref('silver_kma_vilage_fcst') }}
    cross join kst_window
    where cast(forecast_at as date) >= kst_window.min_forecast_date
    {% if is_incremental() %}
      and collected_at >= (
          select coalesce(max(collected_at), timestamp '1970-01-01 00:00:00')
                 - interval '{{ weather_w1_lookback_minutes() }}' minute
          from {{ this }}
      )
    {% endif %}
),

place_grid as (
    select
        place_id,
        place_name,
        alias_names,
        gu,
        admin_dong,
        latitude,
        longitude,
        nx,
        ny,
        mapping_method,
        grid_distance_m,
        source_admin_code,
        admin_dong_code,
        gu_code
    from {{ ref('dim_weather_place') }}
),

-- Determine the existing #340 winner before fan-out.  This keeps the same
-- collected_at/raw-key/request-id precedence without a wide payload window.
selected_grid_forecast as (
    select
        nx,
        ny,
        category,
        issued_at,
        forecast_at,
        (winner)[1] as request_id,
        (winner)[2] as source_id,
        (winner)[3] as request_params_json,
        (winner)[4] as source_grid_place_id,
        (winner)[5] as event_at,
        (winner)[6] as time_bucket,
        (winner)[7] as fcst_value_raw,
        (winner)[8] as fcst_value_num,
        (winner)[9] as raw_object_key,
        (winner)[10] as payload_hash,
        (winner)[11] as total_count,
        (winner)[12] as item_count,
        (winner)[13] as load_date,
        (winner)[14] as collected_at,
        (winner)[15] as dag_run_id
    from (
        select
            nx,
            ny,
            category,
            issued_at,
            forecast_at,
            max_by(
                row(
                    request_id,
                    source_id,
                    request_params_json,
                    source_grid_place_id,
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
                ),
                row(collected_at, raw_object_key, request_id)
            ) as winner
        from grid_forecast
        group by nx, ny, category, issued_at, forecast_at
    ) ranked_grid
),

joined_payload as (
    select
        grid_forecast.request_id,
        grid_forecast.source_id,
        grid_forecast.request_params_json,
        place_grid.place_id,
        place_grid.place_name,
        place_grid.alias_names,
        place_grid.gu,
        place_grid.admin_dong,
        place_grid.latitude,
        place_grid.longitude,
        place_grid.admin_dong_code,
        place_grid.gu_code,
        place_grid.source_admin_code,
        grid_forecast.source_grid_place_id,
        grid_forecast.nx,
        grid_forecast.ny,
        place_grid.mapping_method,
        place_grid.grid_distance_m,
        grid_forecast.category,
        grid_forecast.issued_at,
        grid_forecast.forecast_at,
        grid_forecast.event_at,
        grid_forecast.time_bucket,
        grid_forecast.fcst_value_raw,
        grid_forecast.fcst_value_num,
        grid_forecast.raw_object_key,
        grid_forecast.payload_hash,
        grid_forecast.total_count,
        grid_forecast.item_count,
        grid_forecast.load_date,
        grid_forecast.collected_at,
        grid_forecast.dag_run_id
    from selected_grid_forecast as grid_forecast
    inner join place_grid
        on grid_forecast.nx = place_grid.nx
       and grid_forecast.ny = place_grid.ny
)

select
    request_id,
    source_id,
    request_params_json,
    place_id,
    place_name,
    alias_names,
    gu,
    admin_dong,
    latitude,
    longitude,
    admin_dong_code,
    gu_code,
    source_admin_code,
    source_grid_place_id,
    nx,
    ny,
    mapping_method,
    grid_distance_m,
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
from joined_payload
