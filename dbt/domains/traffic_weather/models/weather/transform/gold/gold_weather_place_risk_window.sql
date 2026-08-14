-- Serving Gold: non-official weather-risk candidates at mapped-place forecast grain.
-- Grain: (place_id, forecast_at).  Threshold labels are explainable planning
-- signals and are explicitly not KMA special-weather alerts.

{{ config(materialized='table') }}

with kst_now as (
    select {{ weather_serving_as_of_hour() }} as current_hour_at
),

hourly as (
    select hourly.*
    from {{ ref('gold_weather_place_hourly_outlook') }} as hourly
    cross join kst_now
    where hourly.forecast_at >= kst_now.current_hour_at
),

flagged as (
    select
        *,
        (temp_c >= 33) as heat_risk,
        (temp_c <= -12) as cold_risk,
        (pty_code in ('1', '4', '5') and coalesce(pcp_mm, 0) >= 15) as heavy_rain_risk,
        (coalesce(sno_cm, 0) >= 1) as snow_risk,
        (wind_ms >= 14) as wind_risk
    from hourly
)

select
    concat(place_id, '|', to_iso8601(cast(forecast_at as timestamp(6)))) as product_row_id,
    place_id,
    place_name,
    admin_dong_code,
    admin_dong,
    gu_code,
    gu,
    forecast_at,
    temp_c,
    wind_ms,
    precip_prob_pct,
    pty_code,
    pcp_mm,
    sno_cm,
    heat_risk,
    cold_risk,
    heavy_rain_risk,
    snow_risk,
    wind_risk,
    (heat_risk or cold_risk or heavy_rain_risk or snow_risk or wind_risk) as any_risk,
    array_join(filter(array[
        if(heat_risk, '폭염후보', cast(null as varchar)),
        if(cold_risk, '한파후보', cast(null as varchar)),
        if(heavy_rain_risk, '호우후보', cast(null as varchar)),
        if(snow_risk, '대설후보', cast(null as varchar)),
        if(wind_risk, '강풍후보', cast(null as varchar))
    ], value -> value is not null), ', ') as risk_labels,
    forecast_issued_at_min,
    forecast_issued_at_max,
    forecast_collected_at_max
from flagged
where heat_risk or cold_risk or heavy_rain_risk or snow_risk or wind_risk
