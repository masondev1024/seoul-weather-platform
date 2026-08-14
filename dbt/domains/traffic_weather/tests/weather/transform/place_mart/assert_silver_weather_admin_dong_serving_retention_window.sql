{{ config(tags=['ask_seoul_weather_transform_serving_place_mart']) }}

with kst_window as (
    select cast(current_timestamp at time zone 'Asia/Seoul' as date) - interval '1' day as min_forecast_date
)

select
    serving.place_id,
    serving.issued_at,
    serving.forecast_at,
    serving.category
from {{ ref('silver_weather_forecast_by_admin_dong_serving') }} as serving
cross join kst_window
where cast(serving.forecast_at as date) < kst_window.min_forecast_date
