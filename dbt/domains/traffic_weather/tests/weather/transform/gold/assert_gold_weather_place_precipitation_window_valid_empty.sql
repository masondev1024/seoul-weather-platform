with kst_now as (
    select {{ weather_serving_as_of_hour() }} as current_hour_at
),

source_hourly as (
    select hourly.*
    from {{ ref('gold_weather_place_hourly_outlook') }} as hourly
    cross join kst_now
    where hourly.forecast_at >= kst_now.current_hour_at
),

source_state as (
    select
        count(*) as source_row_count,
        count_if(pty_code is null) as missing_pty_count,
        count_if(is_precipitating) as precipitating_hour_count
    from source_hourly
),

classification_failures as (
    select 'precipitation_classification_mismatch' as failure_type, place_id
    from source_hourly
    where pty_code is not null
      and is_precipitating is distinct from (pty_code <> '0')
),

source_by_place as (
    select place_id, count(*) as source_precipitating_hour_count
    from source_hourly
    where is_precipitating
    group by place_id
),

product_by_place as (
    select
        place_id,
        sum(precipitation_hour_count) as product_precipitating_hour_count
    from {{ ref('gold_weather_place_precipitation_window') }}
    group by place_id
),

reconciliation_failures as (
    select
        case
            when source_by_place.place_id is null then 'product_place_without_precipitating_source'
            when product_by_place.place_id is null then 'missing_product_place'
            else 'precipitating_hour_count_mismatch'
        end as failure_type,
        coalesce(source_by_place.place_id, product_by_place.place_id) as place_id
    from source_by_place
    full outer join product_by_place
        on source_by_place.place_id = product_by_place.place_id
    where source_by_place.place_id is null
       or product_by_place.place_id is null
       or source_by_place.source_precipitating_hour_count
            <> product_by_place.product_precipitating_hour_count
)

select 'empty_hourly_source' as failure_type, cast(null as varchar) as place_id
from source_state
where source_row_count = 0

union all

select 'missing_pty_classification' as failure_type, cast(null as varchar) as place_id
from source_state
where missing_pty_count > 0

union all

select 'precipitating_source_without_product' as failure_type, cast(null as varchar) as place_id
from source_state
where precipitating_hour_count > 0
  and not exists (
      select 1 from {{ ref('gold_weather_place_precipitation_window') }}
  )

union all

select failure_type, place_id from classification_failures

union all

select failure_type, place_id from reconciliation_failures
