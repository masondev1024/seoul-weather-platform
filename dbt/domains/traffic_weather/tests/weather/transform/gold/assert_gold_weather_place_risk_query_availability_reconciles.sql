with kst_now as (
    select {{ weather_serving_as_of_hour() }} as snapshot_as_of_hour
),
population as (
    select place_id
    from {{ ref('dim_weather_place') }}
),
source_hourly as (
    select
        hourly.place_id,
        cast(hourly.forecast_at as timestamp(6)) as forecast_at,
        cast(hourly.risk_evidence_collected_category_count as bigint) as risk_evidence_collected_category_count,
        cast(hourly.risk_evidence_collected_at_min as timestamp(6)) as risk_evidence_collected_at_min,
        cast(hourly.risk_evidence_collected_at_max as timestamp(6)) as risk_evidence_collected_at_max,
        hourly.risk_evidence_collected_category_count = 5
            and hourly.temp_c is not null
            and hourly.wind_ms is not null
            and hourly.pty_code is not null
            and hourly.pcp_raw is not null
            and hourly.sno_raw is not null as slot_complete
    from {{ ref('gold_weather_place_hourly_outlook') }} as hourly
    cross join kst_now
    where cast(hourly.forecast_at as timestamp(6)) >= kst_now.snapshot_as_of_hour
),

source_schedule as (
    select distinct forecast_at
    from source_hourly
),

source_schedule_with_neighbors as (
    select
        forecast_at,
        lag(forecast_at) over (order by forecast_at) as previous_forecast_at,
        lead(forecast_at) over (order by forecast_at) as next_forecast_at
    from source_schedule
),

hourly_cadence_transition as (
    select min(forecast_at) as first_three_hour_slot_at
    from source_schedule_with_neighbors
    where forecast_at = previous_forecast_at + interval '3' hour
      and next_forecast_at = forecast_at + interval '3' hour
),

horizon as (
    select
        kst_now.snapshot_as_of_hour,
        case
            when min(hourly_cadence_transition.first_three_hour_slot_at) is not null
                then min(hourly_cadence_transition.first_three_hour_slot_at) - interval '3' hour
            else max(source_schedule.forecast_at)
        end as global_forecast_horizon_at
    from kst_now
    left join source_schedule on true
    left join hourly_cadence_transition on true
    group by 1
),
expected_slots as (
    select slot_at
    from horizon
    cross join unnest(
        case
            when global_forecast_horizon_at is null then cast(array[] as array(timestamp(6)))
            else sequence(snapshot_as_of_hour, global_forecast_horizon_at, interval '1' hour)
        end
    ) as slot(slot_at)
),
slot_matrix as (
    select
        population.place_id,
        expected_slots.slot_at,
        source_hourly.slot_complete,
        source_hourly.risk_evidence_collected_at_min,
        source_hourly.risk_evidence_collected_at_max
    from population
    left join expected_slots on true
    left join source_hourly
      on population.place_id = source_hourly.place_id
     and expected_slots.slot_at = source_hourly.forecast_at
),
place_rollup as (
    select
        place_id,
        cast(count(slot_at) as bigint) as expected_forecast_hour_count,
        cast(count_if(coalesce(slot_complete, false)) as bigint) as observed_forecast_hour_count,
        min(case when not coalesce(slot_complete, false) then slot_at end) as first_incomplete_at
    from slot_matrix
    group by 1
),
complete_prefix as (
    select
        slot_matrix.place_id,
        min(slot_matrix.slot_at) as available_from_at,
        max(slot_matrix.slot_at) as available_to_at,
        min(slot_matrix.risk_evidence_collected_at_min) as forecast_collected_at_min,
        max(slot_matrix.risk_evidence_collected_at_max) as forecast_collected_at_max
    from slot_matrix
    inner join place_rollup
      on slot_matrix.place_id = place_rollup.place_id
    where slot_matrix.slot_at is not null
      and coalesce(slot_matrix.slot_complete, false)
      and (place_rollup.first_incomplete_at is null or slot_matrix.slot_at < place_rollup.first_incomplete_at)
    group by 1
),
expected as (
    select
        population.place_id,
        horizon.snapshot_as_of_hour,
        complete_prefix.available_from_at,
        complete_prefix.available_to_at,
        complete_prefix.forecast_collected_at_min,
        complete_prefix.forecast_collected_at_max,
        place_rollup.expected_forecast_hour_count,
        place_rollup.observed_forecast_hour_count,
        case
            when place_rollup.expected_forecast_hour_count > 0
             and place_rollup.observed_forecast_hour_count = place_rollup.expected_forecast_hour_count
             and place_rollup.first_incomplete_at is null then 'complete'
            else 'incomplete'
        end as availability_status
    from population
    cross join horizon
    left join place_rollup
      on population.place_id = place_rollup.place_id
    left join complete_prefix
      on population.place_id = complete_prefix.place_id
),
actual as (
    select * from {{ ref('gold_weather_place_risk_query_availability') }}
)
select
    'availability_reconciliation_mismatch' as violation,
    expected.place_id as evidence
from expected
left join actual
  on expected.place_id = actual.place_id
where actual.place_id is null
   or actual.snapshot_as_of_hour <> expected.snapshot_as_of_hour
   or actual.available_from_at is distinct from expected.available_from_at
   or actual.available_to_at is distinct from expected.available_to_at
   or actual.forecast_collected_at_min is distinct from expected.forecast_collected_at_min
   or actual.forecast_collected_at_max is distinct from expected.forecast_collected_at_max
   or actual.expected_forecast_hour_count <> expected.expected_forecast_hour_count
   or actual.observed_forecast_hour_count <> expected.observed_forecast_hour_count
   or actual.availability_status <> expected.availability_status

union all

select 'invalid_prefix_freshness_order', cast(place_id as varchar)
from actual
where forecast_collected_at_min > forecast_collected_at_max
