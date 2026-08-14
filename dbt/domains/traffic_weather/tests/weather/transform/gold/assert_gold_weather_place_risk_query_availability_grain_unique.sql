with availability as (
    select * from {{ ref('gold_weather_place_risk_query_availability') }}
),
violations as (
    select 'duplicate_place_id' as violation, cast(place_id as varchar) as evidence
    from availability
    group by 2
    having count(*) <> 1

    union all

    select 'invalid_place_id', coalesce(cast(place_id as varchar), '<null>')
    from availability
    where place_id is null

    union all

    select 'invalid_snapshot_as_of_hour', cast(place_id as varchar)
    from availability
    where snapshot_as_of_hour is null

    union all

    select 'invalid_status', cast(place_id as varchar)
    from availability
    where availability_status not in ('complete', 'incomplete')
       or availability_status is null

    union all

    select 'invalid_count', cast(place_id as varchar)
    from availability
    where expected_forecast_hour_count < 0
       or observed_forecast_hour_count < 0
       or observed_forecast_hour_count > expected_forecast_hour_count

    union all

    select 'unexpected_population_count', cast(count(distinct place_id) as varchar)
    from availability
    having count(distinct place_id) <> 427
)
select * from violations
