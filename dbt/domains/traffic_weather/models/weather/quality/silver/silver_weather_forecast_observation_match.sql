{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['evaluation_run_id', 'grid_id', 'valid_at', 'variable', 'forecast_horizon'],
    on_schema_change='fail',
    full_refresh=false,
    views_enabled=false,
    on_table_exists='drop',
    properties={
        "partitioning": "ARRAY['day(valid_at)']"
    },
    tags=['ask_seoul_weather_quality_candidate']
) }}

{% if execute %}
  {% do weather_quality_validate_runtime_contract() %}
  {% set evaluation_run_id = weather_quality_run_id() %}
  {% set evaluation_as_of = weather_quality_evaluation_as_of() %}
  {% set window_start_date = weather_quality_window_start_date() %}
  {% set window_end_date = weather_quality_window_end_date() %}
  {% set truth_policy_version = weather_quality_truth_policy_version() %}
  {% set vintage_policy_version = weather_quality_vintage_policy_version() %}
  {% set evidence_policy_version = weather_quality_evidence_policy_version() %}
  {% set pop_policy_version = weather_quality_pop_policy_version() %}
{% else %}
  {% set evaluation_run_id = "'parse_only'" %}
  {% set evaluation_as_of = "from_iso8601_timestamp('1970-01-02T00:00:00+00:00')" %}
  {% set window_start_date = "cast('1970-01-01' as date)" %}
  {% set window_end_date = "cast('1970-01-01' as date)" %}
  {% set truth_policy_version = "'observation-truth-policy/v2-internal'" %}
  {% set vintage_policy_version = "'forecast-vintage-cutoff/v1'" %}
  {% set evidence_policy_version = "'metric-evidence-gate/v1'" %}
  {% set pop_policy_version = "'pop-threshold-0.5/v1'" %}
{% endif %}

with vintage_windows as (
    select 'D-1' as forecast_horizon, 27 as lower_hours_before_valid, 24 as upper_hours_before_valid
    union all
    select 'D-2' as forecast_horizon, 51 as lower_hours_before_valid, 48 as upper_hours_before_valid
    union all
    select 'D-3' as forecast_horizon, 75 as lower_hours_before_valid, 72 as upper_hours_before_valid
),

canonical_variables as (
    select
        'temperature_air_2m' as variable,
        'continuous' as forecast_value_kind,
        'continuous' as truth_value_kind,
        'degC' as unit
    union all
    select
        'precipitation_occurrence' as variable,
        'probability' as forecast_value_kind,
        'binary' as truth_value_kind,
        '1' as unit
    union all
    select
        'precipitation_occurrence_category' as variable,
        'categorical' as forecast_value_kind,
        'categorical' as truth_value_kind,
        'category' as unit
),

canonical_grids as (
    select
        grid_id,
        nx,
        ny
    from {{ ref('dim_weather_coverage_grid') }}
),

expected_hours as (
    select
        cast({{ window_start_date }} as timestamp(6))
            + (hour_offset * interval '1' hour) as valid_at
    from unnest(sequence(0, 23)) as offsets(hour_offset)
    where cast({{ window_end_date }} as date) = cast({{ window_start_date }} as date)

    union all

    select
        cast({{ window_start_date }} as timestamp(6))
            + (hour_offset * interval '1' hour) as valid_at
    from unnest(sequence(0, 167)) as offsets(hour_offset)
    where cast({{ window_end_date }} as date) = cast({{ window_start_date }} as date) + interval '6' day
),

expected_population as (
    select
        {{ evaluation_run_id }} as evaluation_run_id,
        cast({{ evaluation_as_of }} as timestamp(6)) as evaluation_as_of,
        cast(date(expected_hours.valid_at) as date) as evaluation_date_kst,
        coverage_grid.grid_id,
        coverage_grid.nx,
        coverage_grid.ny,
        expected_hours.valid_at,
        canonical_variables.variable,
        canonical_variables.forecast_value_kind as expected_forecast_value_kind,
        canonical_variables.truth_value_kind as expected_truth_value_kind,
        canonical_variables.unit as expected_unit,
        vintage_windows.forecast_horizon,
        vintage_windows.lower_hours_before_valid,
        vintage_windows.upper_hours_before_valid
    from expected_hours
    cross join canonical_grids as coverage_grid
    cross join canonical_variables
    cross join vintage_windows
    where expected_hours.valid_at
        < date_trunc('day', cast({{ evaluation_as_of }} as timestamp(6)) + interval '9' hour)
),

truth_candidates as (
    select
        truth.grid_id,
        truth.nx,
        truth.ny,
        truth.observed_at,
        truth.observed_at + interval '9' hour as observed_at_kst,
        truth.variable,
        truth.value_kind,
        truth.unit,
        truth.raw_value,
        truth.value_num,
        truth.value_bool,
        truth.value_category,
        truth.truth_status,
        truth.source_variable,
        truth.source_revision,
        truth.truth_revision,
        truth.source_id,
        truth.truth_source,
        truth.truth_quality,
        truth.collected_at,
        truth.truth_as_of,
        truth.dag_run_id,
        truth.manifest_key,
        truth.raw_object_key,
        truth.payload_sha256,
        row_number() over (
            partition by truth.grid_id, truth.observed_at + interval '9' hour, truth.variable
            order by truth.truth_as_of desc, truth.collected_at desc, truth.truth_revision desc, truth.dag_run_id desc
        ) as truth_rank
    from {{ ref('silver_kma_observation_truth') }} as truth
    where truth.truth_as_of <= cast({{ evaluation_as_of }} as timestamp(6))
),

selected_truth as (
    select *
    from truth_candidates
    where truth_rank = 1
),

forecast_scope as (
    select
        forecast.grid_id,
        forecast.valid_at,
        forecast.variable,
        forecast.issued_at,
        forecast.value_kind,
        forecast.unit,
        forecast.raw_value,
        forecast.raw_value_num,
        forecast.value_num,
        forecast.value_category,
        forecast.value_status,
        forecast.source_variable,
        forecast.source_revision,
        forecast.request_id,
        forecast.source_id,
        forecast.raw_object_key,
        forecast.load_date,
        forecast.collected_at,
        forecast.dag_run_id as source_run_id,
        forecast.collection_dag_id,
        forecast.manifest_event_at_utc
    from {{ ref('silver_weather_quality_forecast_vintage') }} as forecast
),

forecast_candidates as (
    select
        expected_population.evaluation_run_id,
        expected_population.grid_id,
        expected_population.valid_at,
        expected_population.variable,
        expected_population.forecast_horizon,
        forecast_scope.issued_at,
        forecast_scope.value_kind,
        forecast_scope.unit,
        forecast_scope.raw_value,
        forecast_scope.raw_value_num,
        forecast_scope.value_num,
        forecast_scope.value_category,
        forecast_scope.value_status,
        forecast_scope.source_variable,
        forecast_scope.source_revision,
        forecast_scope.request_id,
        forecast_scope.source_id,
        forecast_scope.raw_object_key,
        forecast_scope.load_date,
        forecast_scope.collected_at,
        forecast_scope.source_run_id,
        forecast_scope.collection_dag_id,
        forecast_scope.manifest_event_at_utc,
        row_number() over (
            partition by
                expected_population.evaluation_run_id,
                expected_population.grid_id,
                expected_population.valid_at,
                expected_population.variable,
                expected_population.forecast_horizon
            order by issued_at desc, source_revision desc, source_run_id desc
        ) as candidate_rank
    from expected_population
    inner join forecast_scope
        on expected_population.grid_id = forecast_scope.grid_id
       and expected_population.valid_at = forecast_scope.valid_at
       and expected_population.variable = forecast_scope.variable
       and forecast_scope.issued_at between valid_at - interval '1' hour * lower_hours_before_valid
                                      and valid_at - interval '1' hour * upper_hours_before_valid
),

selected_forecast as (
    select *
    from forecast_candidates
    where candidate_rank = 1
),

matched as (
    select
        expected_population.*,
        selected_forecast.issued_at as forecast_issued_at,
        selected_forecast.value_kind as forecast_value_kind,
        selected_forecast.unit as forecast_unit,
        selected_forecast.raw_value as forecast_raw_value,
        selected_forecast.raw_value_num as forecast_raw_value_num,
        selected_forecast.value_num as forecast_value_num,
        selected_forecast.value_category as forecast_value_category,
        selected_forecast.value_status as forecast_value_status,
        selected_forecast.source_variable as forecast_source_variable,
        selected_forecast.source_revision as forecast_source_revision,
        selected_forecast.request_id as forecast_request_id,
        selected_forecast.source_id as forecast_source_id,
        selected_forecast.raw_object_key as forecast_raw_object_key,
        selected_forecast.load_date as forecast_load_date,
        selected_forecast.collected_at as forecast_collected_at,
        selected_forecast.source_run_id as forecast_source_run_id,
        selected_forecast.collection_dag_id as forecast_collection_dag_id,
        selected_forecast.manifest_event_at_utc as forecast_manifest_event_at_utc,
        selected_truth.observed_at as truth_observed_at,
        selected_truth.value_kind as truth_value_kind,
        selected_truth.unit as truth_unit,
        selected_truth.raw_value as truth_raw_value,
        selected_truth.value_num as truth_value_num,
        selected_truth.value_bool as truth_value_bool,
        selected_truth.value_category as truth_value_category,
        selected_truth.truth_status,
        selected_truth.source_variable as truth_source_variable,
        selected_truth.source_revision as truth_source_revision,
        selected_truth.truth_revision,
        selected_truth.source_id as truth_source_id,
        selected_truth.truth_source,
        selected_truth.truth_quality,
        selected_truth.collected_at as truth_collected_at,
        selected_truth.truth_as_of,
        selected_truth.dag_run_id as truth_source_run_id,
        selected_truth.manifest_key as truth_manifest_key,
        selected_truth.raw_object_key as truth_raw_object_key,
        selected_truth.payload_sha256 as truth_payload_sha256,
        case
            when selected_truth.grid_id is null then 'missing_truth'
            when selected_forecast.grid_id is null then 'missing_vintage'
            when not (
                expected_population.variable = 'temperature_air_2m'
                and selected_forecast.value_kind = 'continuous'
                and selected_truth.value_kind = 'continuous'
                and selected_forecast.unit = 'degC'
                and selected_truth.unit = 'degC'
                or expected_population.variable = 'precipitation_occurrence'
                and selected_forecast.value_kind = 'probability'
                and selected_truth.value_kind = 'binary'
                and selected_forecast.unit = '1'
                and selected_truth.unit = '1'
                or expected_population.variable = 'precipitation_occurrence_category'
                and selected_forecast.value_kind = 'categorical'
                and selected_truth.value_kind = 'categorical'
                and selected_forecast.unit = 'category'
                and selected_truth.unit = 'category'
            ) then 'incompatible_contract'
            when selected_forecast.value_status != 'valid' then 'invalid_forecast'
            when selected_truth.truth_status = 'invalid_truth' then 'invalid_truth'
            when selected_truth.truth_status != 'provisional' then 'invalid_truth'
            when selected_forecast.value_kind = 'continuous'
                and selected_forecast.value_num is null then 'invalid_forecast'
            when selected_forecast.value_kind = 'probability'
                and selected_forecast.value_num is null then 'invalid_forecast'
            when selected_forecast.value_kind = 'categorical'
                and selected_forecast.value_category is null then 'invalid_forecast'
            when selected_truth.value_kind = 'continuous'
                and selected_truth.value_num is null then 'invalid_truth'
            when selected_truth.value_kind = 'binary'
                and selected_truth.value_bool is null then 'invalid_truth'
            when selected_truth.value_kind = 'categorical'
                and selected_truth.value_category is null then 'invalid_truth'
            when selected_forecast.value_kind = 'continuous'
                and selected_forecast.value_num is not null
                and selected_truth.value_kind = 'continuous'
                and selected_truth.value_num is not null then 'matched'
            when selected_forecast.value_kind = 'probability'
                and selected_forecast.value_num is not null
                and selected_truth.value_kind = 'binary'
                and selected_truth.value_bool is not null then 'matched'
            when selected_forecast.value_kind = 'categorical'
                and selected_forecast.value_category is not null
                and selected_truth.value_kind = 'categorical'
                and selected_truth.value_category is not null then 'matched'
            else 'incompatible_contract'
        end as match_state
    from expected_population
    left join selected_truth
        on expected_population.grid_id = selected_truth.grid_id
       and expected_population.valid_at = selected_truth.observed_at_kst
       and expected_population.variable = selected_truth.variable
    left join selected_forecast
        on expected_population.evaluation_run_id = selected_forecast.evaluation_run_id
       and expected_population.grid_id = selected_forecast.grid_id
       and expected_population.valid_at = selected_forecast.valid_at
       and expected_population.variable = selected_forecast.variable
       and expected_population.forecast_horizon = selected_forecast.forecast_horizon
)

select
    evaluation_run_id,
    evaluation_as_of,
    evaluation_date_kst,
    grid_id,
    nx,
    ny,
    valid_at,
    variable,
    forecast_horizon,
    lower_hours_before_valid,
    upper_hours_before_valid,
    match_state,
    forecast_issued_at,
    forecast_value_kind,
    truth_value_kind,
    forecast_unit,
    truth_unit,
    forecast_raw_value,
    truth_raw_value,
    forecast_raw_value_num,
    forecast_value_num,
    truth_value_num,
    forecast_value_category,
    truth_value_category,
    truth_value_bool,
    case when match_state = 'matched' and variable = 'temperature_air_2m'
        then forecast_value_num
    end as temperature_forecast_value,
    case when match_state = 'matched' and variable = 'temperature_air_2m'
        then truth_value_num
    end as temperature_observed_value,
    case when match_state = 'matched' and variable = 'temperature_air_2m'
        then forecast_value_num - truth_value_num
    end as temperature_error,
    case when match_state = 'matched' and variable = 'temperature_air_2m'
        then abs(forecast_value_num - truth_value_num)
    end as temperature_absolute_error,
    case when match_state = 'matched' and variable = 'temperature_air_2m'
        then (forecast_value_num - truth_value_num) * (forecast_value_num - truth_value_num)
    end as temperature_squared_error,
    case when match_state = 'matched' and variable = 'precipitation_occurrence'
        then forecast_value_num
    end as forecast_probability,
    case when match_state = 'matched' and variable = 'precipitation_occurrence'
        then truth_value_bool
    end as observed_occurrence,
    case when match_state = 'matched' and variable = 'precipitation_occurrence'
        then (forecast_value_num - if(truth_value_bool, 1.0, 0.0))
            * (forecast_value_num - if(truth_value_bool, 1.0, 0.0))
    end as brier_component,
    case when match_state = 'matched' and variable = 'precipitation_occurrence'
        then forecast_value_num >= 0.5
    end as predicted_occurrence,
    -- POP threshold contract: forecast_probability >= 0.5
    case when match_state = 'matched' and variable = 'precipitation_occurrence'
        then if(forecast_value_num >= 0.5 and truth_value_bool, 1, 0)
    end as true_positive,
    case when match_state = 'matched' and variable = 'precipitation_occurrence'
        then if(forecast_value_num >= 0.5 and not truth_value_bool, 1, 0)
    end as false_positive,
    case when match_state = 'matched' and variable = 'precipitation_occurrence'
        then if(forecast_value_num < 0.5 and not truth_value_bool, 1, 0)
    end as true_negative,
    case when match_state = 'matched' and variable = 'precipitation_occurrence'
        then if(forecast_value_num < 0.5 and truth_value_bool, 1, 0)
    end as false_negative,
    case when match_state = 'matched' and variable = 'precipitation_occurrence_category'
        then forecast_value_category
    end as categorical_forecast,
    case when match_state = 'matched' and variable = 'precipitation_occurrence_category'
        then truth_value_category
    end as categorical_observed,
    case when match_state = 'matched' and variable = 'precipitation_occurrence_category'
        then forecast_value_category = truth_value_category
    end as categorical_match,
    forecast_value_status,
    truth_status,
    forecast_source_variable,
    truth_source_variable,
    forecast_source_revision,
    truth_source_revision,
    truth_revision,
    forecast_source_id,
    truth_source_id,
    truth_source,
    truth_quality,
    forecast_request_id,
    forecast_raw_object_key,
    truth_raw_object_key,
    truth_payload_sha256,
    forecast_load_date,
    forecast_collected_at,
    truth_collected_at,
    truth_as_of,
    forecast_source_run_id,
    truth_source_run_id,
    forecast_collection_dag_id,
    truth_manifest_key,
    forecast_manifest_event_at_utc,
    {{ truth_policy_version }} as truth_policy_version,
    {{ vintage_policy_version }} as vintage_policy_version,
    {{ evidence_policy_version }} as evidence_policy_version,
    {{ pop_policy_version }} as pop_policy_version
from matched
