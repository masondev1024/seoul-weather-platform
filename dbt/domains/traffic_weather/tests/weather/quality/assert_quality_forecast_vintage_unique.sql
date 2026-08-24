select
    grid_id,
    valid_at,
    variable,
    issued_at,
    source_revision,
    count(*) as row_count
from {{ ref('silver_weather_quality_forecast_vintage') }}
group by grid_id, valid_at, variable, issued_at, source_revision
having count(*) > 1
