with grid_score_daily as (
    select
        evaluation_run_id,
        evaluation_date_kst,
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
        forecast_horizon
),

hourly_rollup as (
    select
        evaluation_run_id,
        evaluation_date_kst,
        forecast_horizon,
        sum(expected_count) as expected_count,
        sum(matched_count) as matched_count,
        sum(temperature_squared_error_sum) as temperature_squared_error_sum,
        sum(precipitation_brier_sum) as precipitation_brier_sum
    from {{ ref('gold_weather_forecast_quality_hourly_history') }}
    group by
        evaluation_run_id,
        evaluation_date_kst,
        forecast_horizon
),

history as (
    select *
    from {{ ref('gold_weather_forecast_quality_daily_history') }}
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
    select 'missing_from_view' as error_type, expected_view.evaluation_run_id, expected_view.evaluation_date_kst, expected_view.forecast_horizon
    from expected_view
    left join {{ ref('gold_weather_forecast_quality_daily') }} as published
        on expected_view.evaluation_run_id = published.evaluation_run_id
       and expected_view.evaluation_date_kst = published.evaluation_date_kst
       and expected_view.forecast_horizon = published.forecast_horizon
    where published.evaluation_run_id is null

    union all

    select 'unexpected_in_view' as error_type, published.evaluation_run_id, published.evaluation_date_kst, published.forecast_horizon
    from {{ ref('gold_weather_forecast_quality_daily') }} as published
    left join expected_view
        on expected_view.evaluation_run_id = published.evaluation_run_id
       and expected_view.evaluation_date_kst = published.evaluation_date_kst
       and expected_view.forecast_horizon = published.forecast_horizon
    where expected_view.evaluation_run_id is null
),

aggregate_mismatches as (
    select
        'aggregate_mismatch' as error_type,
        coalesce(grid_score_daily.evaluation_run_id, history.evaluation_run_id) as evaluation_run_id,
        coalesce(grid_score_daily.evaluation_date_kst, history.evaluation_date_kst) as evaluation_date_kst,
        coalesce(grid_score_daily.forecast_horizon, history.forecast_horizon) as forecast_horizon
    from grid_score_daily
    full outer join history
        on grid_score_daily.evaluation_run_id = history.evaluation_run_id
       and grid_score_daily.evaluation_date_kst = history.evaluation_date_kst
       and grid_score_daily.forecast_horizon = history.forecast_horizon
    left join hourly_rollup
        on grid_score_daily.evaluation_run_id = hourly_rollup.evaluation_run_id
       and grid_score_daily.evaluation_date_kst = hourly_rollup.evaluation_date_kst
       and grid_score_daily.forecast_horizon = hourly_rollup.forecast_horizon
    where coalesce(grid_score_daily.expected_count, -1) != coalesce(history.expected_count, -1)
       or coalesce(grid_score_daily.matched_count, -1) != coalesce(history.matched_count, -1)
       or coalesce(grid_score_daily.missing_vintage_count, -1) != coalesce(history.missing_vintage_count, -1)
       or coalesce(grid_score_daily.missing_truth_count, -1) != coalesce(history.missing_truth_count, -1)
       or coalesce(grid_score_daily.invalid_forecast_count, -1) != coalesce(history.invalid_forecast_count, -1)
       or coalesce(grid_score_daily.invalid_truth_count, -1) != coalesce(history.invalid_truth_count, -1)
       or coalesce(grid_score_daily.incompatible_contract_count, -1) != coalesce(history.incompatible_contract_count, -1)
       or coalesce(grid_score_daily.temperature_absolute_error_sum, cast(-1 as double)) != coalesce(history.temperature_absolute_error_sum, cast(-1 as double))
       or coalesce(grid_score_daily.temperature_squared_error_sum, cast(-1 as double)) != coalesce(history.temperature_squared_error_sum, cast(-1 as double))
       or coalesce(grid_score_daily.temperature_error_sum, cast(-1 as double)) != coalesce(history.temperature_error_sum, cast(-1 as double))
       or coalesce(grid_score_daily.precipitation_brier_sum, cast(-1 as double)) != coalesce(history.precipitation_brier_sum, cast(-1 as double))
       or coalesce(grid_score_daily.precipitation_true_positive_count, -1) != coalesce(history.precipitation_true_positive_count, -1)
       or coalesce(grid_score_daily.precipitation_false_positive_count, -1) != coalesce(history.precipitation_false_positive_count, -1)
       or coalesce(grid_score_daily.precipitation_true_negative_count, -1) != coalesce(history.precipitation_true_negative_count, -1)
       or coalesce(grid_score_daily.precipitation_false_negative_count, -1) != coalesce(history.precipitation_false_negative_count, -1)
       or coalesce(grid_score_daily.pty_correct_count, -1) != coalesce(history.pty_correct_count, -1)
       or coalesce(hourly_rollup.expected_count, -1) != coalesce(history.expected_count, -1)
       or coalesce(hourly_rollup.matched_count, -1) != coalesce(history.matched_count, -1)
       or coalesce(hourly_rollup.temperature_squared_error_sum, cast(-1 as double)) != coalesce(history.temperature_squared_error_sum, cast(-1 as double))
       or coalesce(hourly_rollup.precipitation_brier_sum, cast(-1 as double)) != coalesce(history.precipitation_brier_sum, cast(-1 as double))
)

select *
from aggregate_mismatches

union all

select *
from view_mismatches
