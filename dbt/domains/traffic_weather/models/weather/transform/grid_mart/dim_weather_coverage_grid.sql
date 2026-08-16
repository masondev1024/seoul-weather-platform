with typed as (
    select
        nullif(trim(cast(grid_id as varchar)), '') as grid_id,
        try_cast(nx as integer) as nx,
        try_cast(ny as integer) as ny,
        nullif(trim(cast(coverage_scope as varchar)), '') as coverage_scope
    from {{ ref('weather_coverage_grid') }}
)

select
    grid_id,
    nx,
    ny,
    coverage_scope
from typed
where grid_id is not null
  and nx is not null
  and ny is not null
  and coverage_scope is not null
