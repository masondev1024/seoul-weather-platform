-- Serving Gold: latest KMA issue versus the previous issue at place × forecast-date grain.
-- This measures forecast revision, not deviation from observed weather or climatology.

{{ config(materialized='table') }}

with kst_today as (
    select cast({{ weather_serving_as_of_hour() }} as date) as today
),

issue_daily as (
    select
        cast(forecast.place_id as varchar) as place_id,
        cast(forecast.forecast_at as date) as forecast_date,
        cast(forecast.issued_at as timestamp(6)) as issued_at,
        max(cast(forecast.place_name as varchar)) as place_name,
        max(cast(forecast.admin_dong_code as varchar)) as admin_dong_code,
        max(cast(forecast.admin_dong as varchar)) as admin_dong,
        max(cast(forecast.gu_code as varchar)) as gu_code,
        max(cast(forecast.gu as varchar)) as gu,
        count(distinct forecast.category) as category_count,
        count(distinct forecast.forecast_at) as forecast_hour_count,
        coalesce(
            max(forecast.fcst_value_num) filter (where upper(forecast.category) = 'TMN'),
            min(forecast.fcst_value_num) filter (where upper(forecast.category) = 'TMP')
        ) as min_temp_c,
        coalesce(
            max(forecast.fcst_value_num) filter (where upper(forecast.category) = 'TMX'),
            max(forecast.fcst_value_num) filter (where upper(forecast.category) = 'TMP')
        ) as max_temp_c,
        max(forecast.fcst_value_num) filter (where upper(forecast.category) = 'POP') as max_precip_prob_pct,
        min(forecast.forecast_at) filter (
            where upper(forecast.category) = 'PTY'
              and forecast.fcst_value_raw is not null
              and forecast.fcst_value_raw <> '0'
        ) as first_precipitation_at,
        -- Bronze `collected_at` is stored as a UTC-naive timestamp.  This
        -- product contract exposes collection time in its canonical KST axis;
        -- normalize before the freshness field is published to D1.
        max(cast({{ asac_axes.utc_to_kst('forecast.collected_at') }} as timestamp(6))) as collected_at_max
    from {{ ref('silver_weather_forecast_by_admin_dong_serving') }} as forecast
    cross join kst_today
    where cast(forecast.forecast_at as date) >= kst_today.today
    group by 1, 2, 3
),

ranked_issues as (
    select
        issue_daily.*,
        dense_rank() over (
            partition by place_id, forecast_date
            order by issued_at desc
        ) as issue_rank
    from issue_daily
),

latest as (
    select *
    from ranked_issues
    where issue_rank = 1
),

previous as (
    select *
    from ranked_issues
    where issue_rank = 2
)

select
    concat(latest.place_id, '|', cast(latest.forecast_date as varchar)) as product_row_id,
    latest.place_id,
    latest.place_name,
    latest.admin_dong_code,
    latest.admin_dong,
    latest.gu_code,
    latest.gu,
    latest.forecast_date,
    latest.issued_at as latest_issued_at,
    previous.issued_at as previous_issued_at,
    date_diff('hour', previous.issued_at, latest.issued_at) as issue_gap_hours,
    latest.category_count as latest_category_count,
    previous.category_count as previous_category_count,
    latest.forecast_hour_count as latest_forecast_hour_count,
    previous.forecast_hour_count as previous_forecast_hour_count,
    latest.min_temp_c as latest_min_temp_c,
    previous.min_temp_c as previous_min_temp_c,
    latest.min_temp_c - previous.min_temp_c as min_temp_change_c,
    latest.max_temp_c as latest_max_temp_c,
    previous.max_temp_c as previous_max_temp_c,
    latest.max_temp_c - previous.max_temp_c as max_temp_change_c,
    latest.max_precip_prob_pct as latest_max_precip_prob_pct,
    previous.max_precip_prob_pct as previous_max_precip_prob_pct,
    latest.max_precip_prob_pct - previous.max_precip_prob_pct as max_precip_prob_change_pct,
    latest.first_precipitation_at as latest_first_precipitation_at,
    previous.first_precipitation_at as previous_first_precipitation_at,
    case
        when previous.issued_at is null then 'no_previous_issue'
        when latest.min_temp_c is null
          or previous.min_temp_c is null
          or latest.max_temp_c is null
          or previous.max_temp_c is null
          or latest.max_precip_prob_pct is null
          or previous.max_precip_prob_pct is null then 'partial_comparison'
        when latest.min_temp_c is distinct from previous.min_temp_c
          or latest.max_temp_c is distinct from previous.max_temp_c
          or latest.max_precip_prob_pct is distinct from previous.max_precip_prob_pct
          or latest.first_precipitation_at is distinct from previous.first_precipitation_at then 'changed'
        else 'unchanged'
    end as change_state,
    latest.collected_at_max as latest_collected_at_max,
    previous.collected_at_max as previous_collected_at_max
from latest
left join previous
    on latest.place_id = previous.place_id
   and latest.forecast_date = previous.forecast_date
