{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['evaluation_run_id', 'evaluation_date_kst', 'forecast_horizon'],
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
{% endif %}

with grid_score as (
    select *
    from {{ ref('gold_weather_forecast_quality_grid_score_history') }}
),

base_aggregates as (
    select
        evaluation_run_id,
        max(evaluation_as_of) as evaluation_as_of,
        evaluation_date_kst,
        forecast_horizon,
        sum(expected_count) as expected_count,
        sum(matched_count) as matched_count,
        sum(missing_vintage_count) as missing_vintage_count,
        sum(missing_truth_count) as missing_truth_count,
        sum(invalid_forecast_count) as invalid_forecast_count,
        sum(invalid_truth_count) as invalid_truth_count,
        sum(incompatible_contract_count) as incompatible_contract_count,
        count_if(truth_status = 'provisional') as provisional_observation_count,
        sum(if(variable = 'temperature_air_2m', expected_count, 0)) as temperature_expected_count,
        count_if(variable = 'temperature_air_2m' and temperature_absolute_error is not null) as temperature_sample_count,
        sum(temperature_error) as temperature_error_sum,
        sum(temperature_absolute_error) as temperature_absolute_error_sum,
        sum(temperature_squared_error) as temperature_squared_error_sum,
        sum(if(variable = 'precipitation_occurrence', expected_count, 0)) as precipitation_expected_count,
        count_if(variable = 'precipitation_occurrence' and brier_component is not null) as precipitation_sample_count,
        sum(brier_component) as precipitation_brier_sum,
        coalesce(sum(true_positive), 0) as precipitation_true_positive_count,
        coalesce(sum(false_positive), 0) as precipitation_false_positive_count,
        coalesce(sum(true_negative), 0) as precipitation_true_negative_count,
        coalesce(sum(false_negative), 0) as precipitation_false_negative_count,
        sum(if(variable = 'precipitation_occurrence_category', expected_count, 0)) as pty_expected_count,
        count_if(variable = 'precipitation_occurrence_category' and categorical_match is not null) as pty_sample_count,
        count_if(variable = 'precipitation_occurrence_category' and categorical_match) as pty_correct_count,
        arbitrary(truth_policy_version) as truth_policy_version,
        arbitrary(vintage_policy_version) as vintage_policy_version,
        arbitrary(evidence_policy_version) as evidence_policy_version,
        arbitrary(pop_policy_version) as pop_policy_version,
        max(history_loaded_at) as history_loaded_at
    from grid_score
    group by
        evaluation_run_id,
        evaluation_date_kst,
        forecast_horizon
),

probability_bins as (
    select
        evaluation_run_id,
        evaluation_date_kst,
        forecast_horizon,
        least(cast(floor(forecast_probability * 10) as integer), 9) as probability_bin,
        count(*) as bin_sample_count,
        avg(forecast_probability) as bin_mean_probability,
        avg(if(observed_occurrence, 1.0, 0.0)) as bin_observed_rate
    from grid_score
    where variable = 'precipitation_occurrence'
      and brier_component is not null
      and forecast_probability is not null
      and observed_occurrence is not null
    group by
        evaluation_run_id,
        evaluation_date_kst,
        forecast_horizon,
        least(cast(floor(forecast_probability * 10) as integer), 9)
),

ece_aggregates as (
    select
        evaluation_run_id,
        evaluation_date_kst,
        forecast_horizon,
        sum(
            bin_sample_count
            * abs(bin_mean_probability - bin_observed_rate)
        ) / nullif(sum(bin_sample_count), 0) as precipitation_ece_10bin
    from probability_bins
    group by
        evaluation_run_id,
        evaluation_date_kst,
        forecast_horizon
)

select
    base.evaluation_run_id,
    base.evaluation_as_of,
    base.evaluation_date_kst,
    base.forecast_horizon,
    base.expected_count,
    base.matched_count as sample_count,
    base.matched_count,
    cast(base.matched_count as double) / nullif(base.expected_count, 0) as matched_coverage,
    base.missing_vintage_count,
    base.missing_truth_count,
    base.invalid_forecast_count,
    base.invalid_truth_count,
    base.incompatible_contract_count,
    base.provisional_observation_count,
    base.temperature_expected_count,
    base.temperature_sample_count,
    base.temperature_error_sum,
    base.temperature_absolute_error_sum,
    base.temperature_squared_error_sum,
    base.temperature_absolute_error_sum / nullif(base.temperature_sample_count, 0) as temperature_mae,
    sqrt(base.temperature_squared_error_sum / nullif(base.temperature_sample_count, 0)) as temperature_rmse,
    base.temperature_error_sum / nullif(base.temperature_sample_count, 0) as temperature_bias,
    base.precipitation_expected_count,
    base.precipitation_sample_count,
    base.precipitation_brier_sum,
    base.precipitation_brier_sum / nullif(base.precipitation_sample_count, 0) as precipitation_brier_score,
    base.precipitation_true_positive_count,
    base.precipitation_false_positive_count,
    base.precipitation_true_negative_count,
    base.precipitation_false_negative_count,
    cast(base.precipitation_true_positive_count as double)
        / nullif(base.precipitation_true_positive_count + base.precipitation_false_positive_count, 0) as precipitation_precision,
    cast(base.precipitation_true_positive_count as double)
        / nullif(base.precipitation_true_positive_count + base.precipitation_false_negative_count, 0) as precipitation_recall,
    cast(2 * base.precipitation_true_positive_count as double)
        / nullif(2 * base.precipitation_true_positive_count + base.precipitation_false_positive_count + base.precipitation_false_negative_count, 0) as precipitation_f1,
    ece.precipitation_ece_10bin,
    base.pty_expected_count,
    base.pty_sample_count,
    base.pty_correct_count,
    cast(base.pty_correct_count as double) / nullif(base.pty_sample_count, 0) as pty_accuracy,
    30 as evidence_min_sample_count,
    0.80 as evidence_min_matched_coverage,
    {{ weather_quality_evidence_state('base.matched_count', 'base.expected_count', 'base.provisional_observation_count > 0') }} as evidence_state,
    base.truth_policy_version,
    base.vintage_policy_version,
    base.evidence_policy_version,
    base.pop_policy_version,
    base.history_loaded_at
from base_aggregates as base
left join ece_aggregates as ece
    on base.evaluation_run_id = ece.evaluation_run_id
   and base.evaluation_date_kst = ece.evaluation_date_kst
   and base.forecast_horizon = ece.forecast_horizon
