{{ config(tags=['ask_seoul_weather_transform_serving_place_mart']) }}

select
    place_id,
    issued_at,
    forecast_at,
    category,
    count(*) as duplicate_count
from {{ ref('silver_weather_forecast_by_admin_dong_serving') }}
group by 1, 2, 3, 4
having count(*) > 1
