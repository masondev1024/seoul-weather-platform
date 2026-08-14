with expected as (
    select
        *,
        case
            when previous_issued_at is null then 'no_previous_issue'
            when latest_min_temp_c is null
              or previous_min_temp_c is null
              or latest_max_temp_c is null
              or previous_max_temp_c is null
              or latest_max_precip_prob_pct is null
              or previous_max_precip_prob_pct is null then 'partial_comparison'
            when latest_min_temp_c is distinct from previous_min_temp_c
              or latest_max_temp_c is distinct from previous_max_temp_c
              or latest_max_precip_prob_pct is distinct from previous_max_precip_prob_pct
              or latest_first_precipitation_at is distinct from previous_first_precipitation_at then 'changed'
            else 'unchanged'
        end as expected_change_state
    from {{ ref('gold_weather_place_forecast_change_daily') }}
),

invalid as (
    select *
    from expected
    where expected_change_state is distinct from change_state
       or min_temp_change_c is distinct from (latest_min_temp_c - previous_min_temp_c)
       or max_temp_change_c is distinct from (latest_max_temp_c - previous_max_temp_c)
       or max_precip_prob_change_pct is distinct from (
            latest_max_precip_prob_pct - previous_max_precip_prob_pct
       )
       or issue_gap_hours is distinct from date_diff('hour', previous_issued_at, latest_issued_at)
)

select *
from invalid
