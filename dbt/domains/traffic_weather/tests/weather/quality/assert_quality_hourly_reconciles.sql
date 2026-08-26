{{ config(severity='error') }}

{% if execute %}
  {% do weather_quality_validate_runtime_contract() %}
  {% set evaluation_run_id = weather_quality_run_id() %}
{% else %}
  {% set evaluation_run_id = "'parse_only'" %}
{% endif %}

with score as (
    select
        evaluation_run_id,
        evaluation_date_kst,
        valid_at,
        variable,
        forecast_horizon,
        sum(expected_count) as expected_count,
        sum(matched_count) as matched_count
    from {{ ref('gold_weather_forecast_quality_grid_score_history') }}
    where evaluation_run_id = {{ evaluation_run_id }}
    group by 1, 2, 3, 4, 5
),
hourly as (
    select *
    from {{ ref('gold_weather_forecast_quality_hourly_history') }}
    where evaluation_run_id = {{ evaluation_run_id }}
)

select hourly.*
from hourly
left join score
    on hourly.evaluation_run_id = score.evaluation_run_id
   and hourly.evaluation_date_kst = score.evaluation_date_kst
   and hourly.valid_at = score.valid_at
   and hourly.variable = score.variable
   and hourly.forecast_horizon = score.forecast_horizon
where score.evaluation_run_id is null
   or hourly.expected_count <> score.expected_count
   or hourly.matched_count <> score.matched_count
