with expected as (
    select grid_id
    from {{ ref('dim_weather_coverage_grid') }}
),

actual as (
    select grid_id
    from {{ ref('gold_weather_grid_current_outlook') }}
)

select expected.grid_id
from expected
left join actual
    on expected.grid_id = actual.grid_id
where actual.grid_id is null
