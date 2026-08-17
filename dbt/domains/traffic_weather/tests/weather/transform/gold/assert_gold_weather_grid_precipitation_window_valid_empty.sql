with kst_now as (
    select {{ weather_serving_as_of_hour() }} as current_hour_at
),

source_hourly as (
    select hourly.*
    from {{ ref('gold_weather_grid_hourly_outlook') }} as hourly
    cross join kst_now
    where hourly.forecast_at >= kst_now.current_hour_at
),

source_by_grid as (
    select grid_id, count(*) as source_precipitating_hour_count
    from source_hourly
    where is_precipitating
    group by grid_id
),

product_by_grid as (
    select grid_id, sum(precipitation_hour_count) as product_precipitating_hour_count
    from {{ ref('gold_weather_grid_precipitation_window') }}
    group by grid_id
)

select
    coalesce(source_by_grid.grid_id, product_by_grid.grid_id) as grid_id
from source_by_grid
full outer join product_by_grid
    on source_by_grid.grid_id = product_by_grid.grid_id
where source_by_grid.grid_id is null
   or product_by_grid.grid_id is null
   or source_by_grid.source_precipitating_hour_count
        <> product_by_grid.product_precipitating_hour_count
