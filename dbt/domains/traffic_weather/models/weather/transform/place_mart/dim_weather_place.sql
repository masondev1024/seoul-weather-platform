with typed as (
    select
        cast(place_id as varchar) as place_id,
        nullif(trim(cast(place_name as varchar)), '') as place_name,
        nullif(trim(cast(alias_names as varchar)), '') as alias_names,
        nullif(trim(cast(gu as varchar)), '') as gu,
        nullif(trim(cast(admin_dong as varchar)), '') as admin_dong,
        try_cast(latitude as double) as latitude,
        try_cast(longitude as double) as longitude,
        try_cast(nx as integer) as nx,
        try_cast(ny as integer) as ny,
        nullif(trim(cast(mapping_method as varchar)), '') as mapping_method,
        try_cast(grid_distance_m as double) as grid_distance_m,
        nullif(trim(cast(source_admin_code as varchar)), '') as source_admin_code
    from {{ ref('weather_place_grid_mapping') }}
)

select
    place_id,
    place_name,
    alias_names,
    gu,
    admin_dong,
    latitude,
    longitude,
    nx,
    ny,
    mapping_method,
    grid_distance_m,
    source_admin_code,
    source_admin_code as admin_dong_code,
    substr(source_admin_code, 1, 5) as gu_code
from typed
where nullif(trim(cast(place_id as varchar)), '') is not null
