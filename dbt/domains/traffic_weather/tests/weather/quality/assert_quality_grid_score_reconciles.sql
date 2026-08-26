with match_counts as (
    select
        evaluation_run_id,
        evaluation_date_kst,
        count(*) as expected_count,
        count_if(match_state = 'matched') as matched_count,
        count_if(match_state = 'missing_vintage') as missing_vintage_count,
        count_if(match_state = 'missing_truth') as missing_truth_count,
        count_if(match_state = 'invalid_forecast') as invalid_forecast_count,
        count_if(match_state = 'invalid_truth') as invalid_truth_count,
        count_if(match_state = 'incompatible_contract') as incompatible_contract_count
    from {{ ref('silver_weather_forecast_observation_match') }}
    group by evaluation_run_id, evaluation_date_kst
),

history_counts as (
    select
        evaluation_run_id,
        evaluation_date_kst,
        sum(expected_count) as expected_count,
        sum(matched_count) as matched_count,
        sum(missing_vintage_count) as missing_vintage_count,
        sum(missing_truth_count) as missing_truth_count,
        sum(invalid_forecast_count) as invalid_forecast_count,
        sum(invalid_truth_count) as invalid_truth_count,
        sum(incompatible_contract_count) as incompatible_contract_count
    from {{ ref('gold_weather_forecast_quality_grid_score_history') }}
    group by evaluation_run_id, evaluation_date_kst
),

reconciliation_errors as (
    select
        coalesce(match_counts.evaluation_run_id, history_counts.evaluation_run_id) as evaluation_run_id,
        coalesce(match_counts.evaluation_date_kst, history_counts.evaluation_date_kst) as evaluation_date_kst,
        coalesce(match_counts.expected_count, -1) as match_expected_count,
        coalesce(history_counts.expected_count, -1) as history_expected_count,
        coalesce(match_counts.matched_count, -1) as match_matched_count,
        coalesce(history_counts.matched_count, -1) as history_matched_count,
        coalesce(match_counts.missing_vintage_count, -1) as match_missing_vintage_count,
        coalesce(history_counts.missing_vintage_count, -1) as history_missing_vintage_count,
        coalesce(match_counts.missing_truth_count, -1) as match_missing_truth_count,
        coalesce(history_counts.missing_truth_count, -1) as history_missing_truth_count,
        coalesce(match_counts.invalid_forecast_count, -1) as match_invalid_forecast_count,
        coalesce(history_counts.invalid_forecast_count, -1) as history_invalid_forecast_count,
        coalesce(match_counts.invalid_truth_count, -1) as match_invalid_truth_count,
        coalesce(history_counts.invalid_truth_count, -1) as history_invalid_truth_count,
        coalesce(match_counts.incompatible_contract_count, -1) as match_incompatible_contract_count,
        coalesce(history_counts.incompatible_contract_count, -1) as history_incompatible_contract_count
    from match_counts
    full outer join history_counts
        on match_counts.evaluation_run_id = history_counts.evaluation_run_id
       and match_counts.evaluation_date_kst = history_counts.evaluation_date_kst
    where coalesce(match_counts.expected_count, -1) != coalesce(history_counts.expected_count, -1)
       or coalesce(match_counts.matched_count, -1) != coalesce(history_counts.matched_count, -1)
       or coalesce(match_counts.missing_vintage_count, -1) != coalesce(history_counts.missing_vintage_count, -1)
       or coalesce(match_counts.missing_truth_count, -1) != coalesce(history_counts.missing_truth_count, -1)
       or coalesce(match_counts.invalid_forecast_count, -1) != coalesce(history_counts.invalid_forecast_count, -1)
       or coalesce(match_counts.invalid_truth_count, -1) != coalesce(history_counts.invalid_truth_count, -1)
       or coalesce(match_counts.incompatible_contract_count, -1) != coalesce(history_counts.incompatible_contract_count, -1)
)

select *
from reconciliation_errors
