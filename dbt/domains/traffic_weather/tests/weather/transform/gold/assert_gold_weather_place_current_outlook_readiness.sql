with target as (
    select *
    from {{ ref('gold_weather_place_current_outlook') }}
),

anchor_state as (
    select
        count(*) as target_row_count,
        count(distinct snapshot_as_of_hour) as anchor_count,
        count_if(snapshot_as_of_hour is null) as null_anchor_count,
        min(snapshot_as_of_hour) as snapshot_as_of_hour
    from target
),

expected_ranked as (
    select
        hourly.place_id,
        hourly.forecast_at,
        hourly.forecast_issued_at_max,
        row_number() over (
            partition by hourly.place_id
            order by hourly.forecast_at asc, hourly.forecast_issued_at_max desc
        ) as expected_row_num
    from {{ ref('gold_weather_place_hourly_outlook') }} as hourly
    cross join anchor_state
    where anchor_state.snapshot_as_of_hour is not null
      and hourly.forecast_at >= anchor_state.snapshot_as_of_hour
),

expected as (
    select place_id, forecast_at, forecast_issued_at_max
    from expected_ranked
    where expected_row_num = 1
),

reconciliation_failures as (
    select
        case
            when expected.place_id is null then 'unexpected_target_place'
            when target.place_id is null then 'missing_target_place'
            else 'nearest_forecast_identity_mismatch'
        end as failure_type,
        coalesce(expected.place_id, target.place_id) as place_id
    from expected
    full outer join target
        on expected.place_id = target.place_id
    where expected.place_id is null
       or target.place_id is null
       or expected.forecast_at is distinct from target.forecast_at
       or expected.forecast_issued_at_max is distinct from target.forecast_issued_at_max
),

target_duplicate_failures as (
    select 'duplicate_target_place' as failure_type, place_id
    from target
    group by place_id
    having count(*) <> 1
),

target_time_failures as (
    select
        case
            when forecast_at < snapshot_as_of_hour then 'forecast_before_build_anchor'
            else 'issue_after_forecast'
        end as failure_type,
        place_id
    from target
    where forecast_at < snapshot_as_of_hour
       or forecast_issued_at_max > forecast_at
)

select 'target_empty' as failure_type, cast(null as varchar) as place_id
from anchor_state
where target_row_count = 0

union all

select 'invalid_build_anchor' as failure_type, cast(null as varchar) as place_id
from anchor_state
where target_row_count > 0
  and (anchor_count <> 1 or null_anchor_count <> 0)

union all

select failure_type, place_id from reconciliation_failures

union all

select failure_type, place_id from target_duplicate_failures

union all

select failure_type, place_id from target_time_failures
