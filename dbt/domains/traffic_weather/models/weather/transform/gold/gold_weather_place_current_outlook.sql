-- Serving Gold: one nearest non-past forecast per mapped place.
-- Grain: place_id.  "Current" means the earliest forecast_at at or after the
-- KST build hour, not an observed weather condition.

{{ config(materialized='table') }}

with kst_now as (
    select {{ weather_serving_as_of_hour() }} as current_hour_at
),

ranked as (
    select
        hourly.*,
        kst_now.current_hour_at as snapshot_as_of_hour,
        row_number() over (
            partition by hourly.place_id
            order by hourly.forecast_at asc, hourly.forecast_issued_at_max desc
        ) as current_row_num
    from {{ ref('gold_weather_place_hourly_outlook') }} as hourly
    cross join kst_now
    where hourly.forecast_at >= kst_now.current_hour_at
)

select
    place_id as product_row_id,
    place_id,
    place_name,
    alias_names,
    admin_dong_code,
    admin_dong,
    gu_code,
    gu,
    latitude,
    longitude,
    forecast_at,
    forecast_category_count,
    forecast_issued_at_min,
    forecast_issued_at_max,
    forecast_collected_at_max,
    snapshot_as_of_hour,
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
    forecast_lead_hours,
    representative_raw_object_key,
    representative_payload_hash,
    representative_dag_run_id
from ranked
where current_row_num = 1
