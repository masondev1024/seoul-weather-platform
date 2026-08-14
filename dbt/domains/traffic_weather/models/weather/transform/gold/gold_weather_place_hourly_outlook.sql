-- Serving Gold: place × forecast-hour KMA outlook in a compact WIDE shape.
-- Grain: (place_id, forecast_at).  The metric columns are forecasts, not
-- observed weather, and category coverage is exposed instead of imputed.

{{ config(materialized='table') }}

with forecast_long as (
    select
        cast(place_id as varchar) as place_id,
        cast(place_name as varchar) as place_name,
        cast(alias_names as varchar) as alias_names,
        cast(admin_dong_code as varchar) as admin_dong_code,
        cast(admin_dong as varchar) as admin_dong,
        cast(gu_code as varchar) as gu_code,
        cast(gu as varchar) as gu,
        cast(latitude as double) as latitude,
        cast(longitude as double) as longitude,
        upper(cast(category as varchar)) as category,
        cast(issued_at as timestamp(6)) as issued_at,
        cast(forecast_at as timestamp(6)) as forecast_at,
        cast(fcst_value_raw as varchar) as fcst_value_raw,
        cast(fcst_value_num as double) as fcst_value_num,
        cast(collected_at as timestamp(6)) as collected_at,
        cast(raw_object_key as varchar) as raw_object_key,
        cast(payload_hash as varchar) as payload_hash,
        cast(dag_run_id as varchar) as dag_run_id
    from {{ ref('gold_weather_forecast_by_place_serving') }}
),

ranked_forecast_long as (
    select
        forecast.*,
        dense_rank() over (
            partition by place_id, forecast_at
            order by issued_at desc nulls last
        ) as issue_rank
    from forecast_long as forecast
),

latest_forecast_long as (
    select *
    from ranked_forecast_long
    where issue_rank = 1
),

pivoted as (
    select
        place_id,
        forecast_at,
        max(place_name) as place_name,
        max(alias_names) as alias_names,
        max(admin_dong_code) as admin_dong_code,
        max(admin_dong) as admin_dong,
        max(gu_code) as gu_code,
        max(gu) as gu,
        max(latitude) as latitude,
        max(longitude) as longitude,
        count(distinct category) as forecast_category_count,
        min(issued_at) as forecast_issued_at_min,
        max(issued_at) as forecast_issued_at_max,
        max(cast({{ asac_axes.utc_to_kst('collected_at') }} as timestamp(6))) as forecast_collected_at_max,
        count(distinct case
                when collected_at is not null
                 and (
                     (category in ('TMP', 'WSD') and fcst_value_num is not null)
                     or (
                         category in ('PTY', 'PCP', 'SNO')
                         and nullif(trim(fcst_value_raw), '') is not null
                     )
                 )
                    then category
            end) as risk_evidence_collected_category_count,
        min(
            case
                when collected_at is not null
                 and (
                     (category in ('TMP', 'WSD') and fcst_value_num is not null)
                     or (
                         category in ('PTY', 'PCP', 'SNO')
                         and nullif(trim(fcst_value_raw), '') is not null
                     )
                 )
                    then cast({{ asac_axes.utc_to_kst('collected_at') }} as timestamp(6))
            end
        ) as risk_evidence_collected_at_min,
        max(
            case
                when collected_at is not null
                 and (
                     (category in ('TMP', 'WSD') and fcst_value_num is not null)
                     or (
                         category in ('PTY', 'PCP', 'SNO')
                         and nullif(trim(fcst_value_raw), '') is not null
                     )
                 )
                    then cast({{ asac_axes.utc_to_kst('collected_at') }} as timestamp(6))
            end
        ) as risk_evidence_collected_at_max,
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
        max(fcst_value_num) filter (where category = 'SNO') as sno_cm,
        max(raw_object_key) as representative_raw_object_key,
        max(payload_hash) as representative_payload_hash,
        max(dag_run_id) as representative_dag_run_id
    from latest_forecast_long
    group by 1, 2
)

select
    concat(place_id, '|', to_iso8601(cast(forecast_at as timestamp(6)))) as product_row_id,
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
    risk_evidence_collected_category_count,
    risk_evidence_collected_at_min,
    risk_evidence_collected_at_max,
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
    date_diff('hour', forecast_issued_at_max, forecast_at) as forecast_lead_hours,
    representative_raw_object_key,
    representative_payload_hash,
    representative_dag_run_id
from pivoted
