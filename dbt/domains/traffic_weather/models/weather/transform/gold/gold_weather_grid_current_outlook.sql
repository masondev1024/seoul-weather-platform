-- Serving Gold: one nearest non-past forecast per KMA collector-contract grid.
{{ config(materialized='table') }}

with kst_now as (
    select {{ weather_serving_as_of_hour() }} as current_hour_at
),

ranked as (
    select
        hourly.*,
        row_number() over (
            partition by hourly.grid_id
            order by hourly.forecast_at asc, hourly.forecast_issued_at_max desc
        ) as current_row_num
    from {{ ref('gold_weather_grid_hourly_outlook') }} as hourly
    cross join kst_now
    where hourly.forecast_at >= kst_now.current_hour_at
)

select
    grid_id as product_row_id,
    grid_id,
    nx,
    ny,
    coverage_scope,
    forecast_at,
    forecast_category_count,
    forecast_issued_at_min,
    forecast_issued_at_max,
    forecast_collected_at_max,
    temp_c,
    humidity_pct,
    wind_ms,
    wind_dir_deg,
    precip_prob_pct,
    sky_code,
    sky_label,
    pty_code,
    pty_label,
    is_precipitating,
    pcp_raw,
    pcp_mm,
    sno_raw,
    sno_cm,
    forecast_lead_hours
from ranked
where current_row_num = 1
