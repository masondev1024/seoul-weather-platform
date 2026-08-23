{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['evaluation_run_id', 'evaluation_date_kst', 'variable', 'forecast_horizon'],
    on_schema_change='fail',
    full_refresh=false,
    views_enabled=false,
    on_table_exists='drop',
    properties={
        "partitioning": "ARRAY['day(evaluation_date_kst)']"
    },
    tags=['ask_seoul_weather_quality_candidate']
) }}

{% if execute %}
  {% do weather_quality_validate_runtime_contract() %}
  {% set evaluation_run_id = weather_quality_run_id() %}
{% else %}
  {% set evaluation_run_id = "'parse_only'" %}
{% endif %}

with score as (
    select *
    from {{ ref('gold_weather_forecast_quality_grid_score_history') }}
    where evaluation_run_id = {{ evaluation_run_id }}
),

aggregate_components as (
    select
        evaluation_run_id,
        evaluation_as_of,
        evaluation_date_kst,
        variable,
        forecast_horizon,
        max(lower_hours_before_valid) as lower_hours_before_valid,
        max(upper_hours_before_valid) as upper_hours_before_valid,
        sum(expected_count) as expected_count,
        sum(matched_count) as matched_count,
        sum(missing_vintage_count) as missing_vintage_count,
        sum(missing_truth_count) as missing_truth_count,
        sum(invalid_forecast_count) as invalid_forecast_count,
        sum(invalid_truth_count) as invalid_truth_count,
        sum(incompatible_contract_count) as incompatible_contract_count,
        count_if(truth_quality = 'provisional') > 0 as has_provisional_truth,
        sum(temperature_error) as temperature_error_sum,
        sum(temperature_absolute_error) as temperature_absolute_error_sum,
        sum(temperature_squared_error) as temperature_squared_error_sum,
        count(temperature_error) as temperature_sample_count,
        sum(brier_component) as brier_component_sum,
        count(brier_component) as precipitation_sample_count,
        sum(true_positive) as true_positive,
        sum(false_positive) as false_positive,
        sum(true_negative) as true_negative,
        sum(false_negative) as false_negative,
        sum(cast(categorical_match as integer)) as categorical_match_count,
        count(categorical_match) as categorical_sample_count
    from score
    group by 1, 2, 3, 4, 5
),

precipitation_bins as (
    select
        evaluation_run_id,
        evaluation_date_kst,
        variable,
        forecast_horizon,
        least(9, greatest(0, cast(floor(forecast_probability * 10) as integer))) as probability_bin,
        count(*) as bin_sample_count,
        avg(forecast_probability) as average_probability,
        avg(cast(observed_occurrence as double)) as observed_rate
    from score
    where forecast_probability is not null
      and observed_occurrence is not null
    group by 1, 2, 3, 4, 5
),

precipitation_ece as (
    select
        evaluation_run_id,
        evaluation_date_kst,
        variable,
        forecast_horizon,
        sum(abs(average_probability - observed_rate) * bin_sample_count)
            / nullif(sum(bin_sample_count), 0) as expected_calibration_error
    from precipitation_bins
    group by 1, 2, 3, 4
)

select
    components.*,
    cast(matched_count as double) / nullif(expected_count, 0) as matched_coverage,
    case
        when variable = 'temperature_air_2m'
            then temperature_absolute_error_sum / nullif(temperature_sample_count, 0)
    end as temperature_mae,
    case
        when variable = 'temperature_air_2m'
            then sqrt(temperature_squared_error_sum / nullif(temperature_sample_count, 0))
    end as temperature_rmse,
    case
        when variable = 'temperature_air_2m'
            then temperature_error_sum / nullif(temperature_sample_count, 0)
    end as temperature_bias,
    case
        when variable = 'precipitation_occurrence'
            then brier_component_sum / nullif(precipitation_sample_count, 0)
    end as precipitation_brier_score,
    ece.expected_calibration_error as precipitation_expected_calibration_error,
    cast(true_positive as double) / nullif(true_positive + false_positive, 0) as precipitation_precision,
    cast(true_positive as double) / nullif(true_positive + false_negative, 0) as precipitation_recall,
    2.0 * cast(true_positive as double)
        / nullif(2.0 * true_positive + false_positive + false_negative, 0) as precipitation_f1,
    cast(categorical_match_count as double) / nullif(categorical_sample_count, 0) as categorical_accuracy,
    {{ weather_quality_evidence_state('matched_count', 'expected_count', 'has_provisional_truth') }}
        as evidence_state,
    'metric-evidence-gate/v1' as evidence_policy_version,
    current_timestamp as history_loaded_at
from aggregate_components as components
left join precipitation_ece as ece
    on components.evaluation_run_id = ece.evaluation_run_id
   and components.evaluation_date_kst = ece.evaluation_date_kst
   and components.variable = ece.variable
   and components.forecast_horizon = ece.forecast_horizon
