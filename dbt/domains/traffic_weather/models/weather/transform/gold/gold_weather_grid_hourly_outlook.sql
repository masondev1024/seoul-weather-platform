-- Serving Gold: one wide KMA forecast row per collector-contract grid and hour.
-- Grain: (grid_id, forecast_at).  Grid products retain all 80 bbox cells and
-- intentionally do not claim an administrative-dong interpretation.
{{ config(materialized='table') }}

with forecast_long as (
    select
        cast(grid_id as varchar) as grid_id,
        cast(nx as integer) as nx,
        cast(ny as integer) as ny,
        cast(coverage_scope as varchar) as coverage_scope,
        upper(cast(category as varchar)) as category,
        cast(issued_at as timestamp(6)) as issued_at,
        cast(forecast_at as timestamp(6)) as forecast_at,
        cast(fcst_value_raw as varchar) as fcst_value_raw,
        cast(fcst_value_num as double) as fcst_value_num,
        cast(collected_at as timestamp(6)) as collected_at
    from {{ ref('gold_weather_forecast_by_grid_serving') }}
),

pivoted as (
    select
        grid_id,
        nx,
        ny,
        coverage_scope,
        forecast_at,
        count(distinct category) as forecast_category_count,
        min(issued_at) as forecast_issued_at_min,
        max(issued_at) as forecast_issued_at_max,
        max(cast({{ asac_axes.utc_to_kst('collected_at') }} as timestamp(6))) as forecast_collected_at_max,
        max(fcst_value_num) filter (where category = 'TMP') as temp_c,
        max(fcst_value_num) filter (where category = 'REH') as humidity_pct,
        max(fcst_value_num) filter (where category = 'WSD') as wind_ms,
        max(fcst_value_num) filter (where category = 'VEC') as wind_dir_deg,
        max(fcst_value_num) filter (where category = 'POP') as precip_prob_pct,
        max(fcst_value_raw) filter (where category = 'SKY') as sky_code,
        max(fcst_value_raw) filter (where category = 'PTY') as pty_code,
        max(fcst_value_raw) filter (where category = 'PCP') as pcp_raw,
        max(fcst_value_num) filter (where category = 'PCP') as pcp_mm,
        max(fcst_value_raw) filter (where category = 'SNO') as sno_raw,
        max(fcst_value_num) filter (where category = 'SNO') as sno_cm
    from forecast_long
    group by 1, 2, 3, 4, 5
)

select
    concat(grid_id, '|', to_iso8601(cast(forecast_at as timestamp(6)))) as product_row_id,
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
    {{ weather_sky_label('sky_code') }} as sky_label,
    pty_code,
    {{ weather_pty_label('pty_code') }} as pty_label,
    (pty_code is not null and pty_code <> '0') as is_precipitating,
    pcp_raw,
    pcp_mm,
    sno_raw,
    sno_cm,
    date_diff('hour', forecast_issued_at_max, forecast_at) as forecast_lead_hours
from pivoted
