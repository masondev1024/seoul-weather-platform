with grid_score_hourly as (
    select
        evaluation_run_id,
        evaluation_date_kst,
        valid_at,
        forecast_horizon,
        sum(expected_count) as expected_count,
        sum(matched_count) as matched_count,
        sum(missing_vintage_count) as missing_vintage_count,
        sum(missing_truth_count) as missing_truth_count,
        sum(invalid_forecast_count) as invalid_forecast_count,
        sum(invalid_truth_count) as invalid_truth_count,
        sum(incompatible_contract_count) as incompatible_contract_count,
        sum(temperature_absolute_error) as temperature_absolute_error_sum,
        sum(temperature_squared_error) as temperature_squared_error_sum,
        sum(temperature_error) as temperature_error_sum,
        sum(brier_component) as precipitation_brier_sum,
        coalesce(sum(true_positive), 0) as precipitation_true_positive_count,
        coalesce(sum(false_positive), 0) as precipitation_false_positive_count,
        coalesce(sum(true_negative), 0) as precipitation_true_negative_count,
        coalesce(sum(false_negative), 0) as precipitation_false_negative_count,
        count_if(variable = 'precipitation_occurrence_category' and categorical_match) as pty_correct_count
    from {{ ref('gold_weather_forecast_quality_grid_score_history') }}
    group by
        evaluation_run_id,
        evaluation_date_kst,
        valid_at,
        forecast_horizon
),

history as (
    select *
    from {{ ref('gold_weather_forecast_quality_hourly_history') }}
),

publication_manifest as (
    select
        cast(evaluation_run_id as varchar) as evaluation_run_id,
        cast(status as varchar) as publication_status,
        cast(evaluation_as_of as timestamp(6)) as evaluation_as_of,
        cast(published_at as timestamp(6)) as published_at,
        cast(window_start_date as date) as window_start_date,
        cast(window_end_date as date) as window_end_date
    from {{ source('weather_quality_control', 'quality_publication_manifest') }}
),

successful_quality_runs_expanded_to_evaluation_dates as (
    select
        publication_manifest.evaluation_run_id,
        publication_manifest.evaluation_as_of,
        publication_manifest.published_at,
        evaluation_date_kst
    from publication_manifest
    cross join unnest(sequence(window_start_date, window_end_date)) as dates(evaluation_date_kst)
    where publication_status = 'SUCCESS'
),

successful_candidates as (
    select
        evaluation_date_kst,
        evaluation_run_id,
        row_number() over (
            partition by evaluation_date_kst
            order by evaluation_as_of desc, published_at desc, evaluation_run_id desc
        ) as publication_rank
    from successful_quality_runs_expanded_to_evaluation_dates
),

expected_view as (
    select history.*
    from history
    inner join successful_candidates as publication
        on publication.evaluation_run_id = history.evaluation_run_id
       and publication.evaluation_date_kst = history.evaluation_date_kst
       and publication.publication_rank = 1
),

view_mismatches as (
    select 'missing_from_view' as error_type, expected_view.evaluation_run_id, expected_view.valid_at, expected_view.forecast_horizon
    from expected_view
    left join {{ ref('gold_weather_forecast_quality_hourly') }} as published
        on expected_view.evaluation_run_id = published.evaluation_run_id
       and expected_view.valid_at = published.valid_at
       and expected_view.forecast_horizon = published.forecast_horizon
    where published.evaluation_run_id is null

    union all

    select 'unexpected_in_view' as error_type, published.evaluation_run_id, published.valid_at, published.forecast_horizon
    from {{ ref('gold_weather_forecast_quality_hourly') }} as published
    left join expected_view
        on expected_view.evaluation_run_id = published.evaluation_run_id
       and expected_view.valid_at = published.valid_at
       and expected_view.forecast_horizon = published.forecast_horizon
    where expected_view.evaluation_run_id is null
),

aggregate_mismatches as (
    select
        'aggregate_mismatch' as error_type,
        coalesce(grid_score_hourly.evaluation_run_id, history.evaluation_run_id) as evaluation_run_id,
        coalesce(grid_score_hourly.valid_at, history.valid_at) as valid_at,
        coalesce(grid_score_hourly.forecast_horizon, history.forecast_horizon) as forecast_horizon
    from grid_score_hourly
    full outer join history
        on grid_score_hourly.evaluation_run_id = history.evaluation_run_id
       and grid_score_hourly.valid_at = history.valid_at
       and grid_score_hourly.forecast_horizon = history.forecast_horizon
    where coalesce(grid_score_hourly.expected_count, -1) != coalesce(history.expected_count, -1)
       or coalesce(grid_score_hourly.matched_count, -1) != coalesce(history.matched_count, -1)
       or coalesce(grid_score_hourly.missing_vintage_count, -1) != coalesce(history.missing_vintage_count, -1)
       or coalesce(grid_score_hourly.missing_truth_count, -1) != coalesce(history.missing_truth_count, -1)
       or coalesce(grid_score_hourly.invalid_forecast_count, -1) != coalesce(history.invalid_forecast_count, -1)
       or coalesce(grid_score_hourly.invalid_truth_count, -1) != coalesce(history.invalid_truth_count, -1)
       or coalesce(grid_score_hourly.incompatible_contract_count, -1) != coalesce(history.incompatible_contract_count, -1)
       or coalesce(grid_score_hourly.temperature_absolute_error_sum, cast(-1 as double)) != coalesce(history.temperature_absolute_error_sum, cast(-1 as double))
       or coalesce(grid_score_hourly.temperature_squared_error_sum, cast(-1 as double)) != coalesce(history.temperature_squared_error_sum, cast(-1 as double))
       or coalesce(grid_score_hourly.temperature_error_sum, cast(-1 as double)) != coalesce(history.temperature_error_sum, cast(-1 as double))
       or coalesce(grid_score_hourly.precipitation_brier_sum, cast(-1 as double)) != coalesce(history.precipitation_brier_sum, cast(-1 as double))
       or coalesce(grid_score_hourly.precipitation_true_positive_count, -1) != coalesce(history.precipitation_true_positive_count, -1)
       or coalesce(grid_score_hourly.precipitation_false_positive_count, -1) != coalesce(history.precipitation_false_positive_count, -1)
       or coalesce(grid_score_hourly.precipitation_true_negative_count, -1) != coalesce(history.precipitation_true_negative_count, -1)
       or coalesce(grid_score_hourly.precipitation_false_negative_count, -1) != coalesce(history.precipitation_false_negative_count, -1)
       or coalesce(grid_score_hourly.pty_correct_count, -1) != coalesce(history.pty_correct_count, -1)
)

select *
from aggregate_mismatches

union all

select *
from view_mismatches
