-- Serving Gold: consecutive forecast precipitation windows for each mapped place.
-- Grain: (place_id, window_start_at).  A window is forecast evidence, not a
-- rain observation or a guarantee that precipitation will occur.

{{ config(materialized='table') }}

with kst_now as (
    select {{ weather_serving_as_of_hour() }} as current_hour_at
),

precipitation_hours as (
    select
        hourly.*
    from {{ ref('gold_weather_place_hourly_outlook') }} as hourly
    cross join kst_now
    where hourly.forecast_at >= kst_now.current_hour_at
      and hourly.is_precipitating
),

with_previous as (
    select
        *,
        lag(forecast_at) over (
            partition by place_id
            order by forecast_at
        ) as previous_forecast_at
    from precipitation_hours
),

window_marked as (
    select
        *,
        case
            when previous_forecast_at is null
              or date_diff('hour', previous_forecast_at, forecast_at) > 1 then 1
            else 0
        end as new_window_flag
    from with_previous
),

windowed as (
    select
        *,
        sum(new_window_flag) over (
            partition by place_id
            order by forecast_at
            rows between unbounded preceding and current row
        ) as window_number
    from window_marked
)

select
    concat(place_id, '|', to_iso8601(cast(min(forecast_at) as timestamp(6)))) as product_row_id,
    place_id,
    max(place_name) as place_name,
    max(admin_dong_code) as admin_dong_code,
    max(admin_dong) as admin_dong,
    max(gu_code) as gu_code,
    max(gu) as gu,
    min(forecast_at) as window_start_at,
    max(forecast_at) as window_end_at,
    count(*) as precipitation_hour_count,
    max(precip_prob_pct) as precip_prob_max_pct,
    max(pcp_mm) as pcp_max_mm,
    max(sno_cm) as sno_max_cm,
    min(forecast_issued_at_min) as forecast_issued_at_min,
    max(forecast_issued_at_max) as forecast_issued_at_max,
    max(forecast_collected_at_max) as forecast_collected_at_max
from windowed
group by place_id, window_number
