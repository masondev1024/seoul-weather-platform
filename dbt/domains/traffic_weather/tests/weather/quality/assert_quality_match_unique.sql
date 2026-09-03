select
    evaluation_run_id,
    grid_id,
    valid_at,
    variable,
    forecast_horizon,
    count(*) as row_count
from {{ ref('silver_weather_forecast_observation_match') }}
group by
    evaluation_run_id,
    grid_id,
    valid_at,
    variable,
    forecast_horizon
having count(*) > 1
