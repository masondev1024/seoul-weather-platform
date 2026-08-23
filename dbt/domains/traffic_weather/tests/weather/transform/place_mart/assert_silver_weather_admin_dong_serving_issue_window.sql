{{ config(tags=['ask_seoul_weather_transform_serving_place_mart']) }}

select
    place_id,
    forecast_at,
    count(distinct issued_at) as issue_count
from {{ ref('silver_weather_forecast_by_admin_dong_serving') }}
group by 1, 2
having count(distinct issued_at) > 2
