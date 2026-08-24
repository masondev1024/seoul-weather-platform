{{ config(severity='warn', store_failures=true) }}

{% if execute %}
  {% do weather_quality_validate_runtime_contract() %}
  {% set window_start_date = weather_quality_window_start_date() %}
  {% set window_end_date = weather_quality_window_end_date() %}
  {% set evaluation_as_of = weather_quality_evaluation_as_of() %}
{% else %}
  {% set window_start_date = "cast('1970-01-01' as date)" %}
  {% set window_end_date = "cast('1970-01-01' as date)" %}
  {% set evaluation_as_of = "from_iso8601_timestamp('1970-01-01T00:00:00+00:00')" %}
{% endif %}

with expected_hours as (
    select
        cast({{ window_start_date }} as timestamp) - interval '9' hour
            + (hour_offset * interval '1' hour) as observed_at
    from unnest(sequence(0, 167)) as offsets(hour_offset)
    where cast({{ window_end_date }} as date) = cast({{ window_start_date }} as date) + interval '6' day
),

expected as (
    select
        coverage_grid.grid_id,
        coverage_grid.nx,
        coverage_grid.ny,
        expected_hours.observed_at,
        variable
    from expected_hours
    cross join {{ ref('dim_weather_coverage_grid') }} as coverage_grid
    cross join unnest(array[
        'temperature_air_2m',
        'precipitation_occurrence',
        'precipitation_occurrence_category'
    ]) as variables(variable)
    where expected_hours.observed_at < date_trunc('day', cast({{ evaluation_as_of }} as timestamp(6)) + interval '9' hour) - interval '9' hour
),

actual as (
    select
        grid_id,
        observed_at,
        variable,
        count(*) as row_count
    from {{ ref('silver_kma_observation_truth') }}
    group by grid_id, observed_at, variable
),

completeness_report as (
    select
        expected.grid_id,
        expected.observed_at,
        expected.variable,
        coalesce(actual.row_count, 0) as row_count
    from expected
    left join actual
        on expected.grid_id = actual.grid_id
       and expected.observed_at = actual.observed_at
       and expected.variable = actual.variable
    where coalesce(actual.row_count, 0) != 1
)

select *
from completeness_report
