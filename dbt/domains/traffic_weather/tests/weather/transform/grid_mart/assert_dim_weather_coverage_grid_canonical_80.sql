with grid as (
    select
        grid_id,
        nx,
        ny,
        coverage_scope
    from {{ ref('dim_weather_coverage_grid') }}
)

select
    'row_count' as failure_type,
    cast(count(*) as varchar) as detail
from grid
having count(*) <> 80

union all

select
    'distinct_coordinate_count' as failure_type,
    cast(count(distinct concat(cast(nx as varchar), '|', cast(ny as varchar))) as varchar) as detail
from grid
having count(distinct concat(cast(nx as varchar), '|', cast(ny as varchar))) <> 80

union all

select
    'out_of_contract_coordinate' as failure_type,
    concat(cast(nx as varchar), '|', cast(ny as varchar)) as detail
from grid
where nx not between 56 and 65
   or ny not between 123 and 130

union all

select
    'grid_id_mismatch' as failure_type,
    grid_id as detail
from grid
where grid_id <> concat('kma_', cast(nx as varchar), '_', cast(ny as varchar))

union all

select
    'coverage_scope_mismatch' as failure_type,
    coverage_scope as detail
from grid
where coverage_scope <> 'seoul_bbox'
