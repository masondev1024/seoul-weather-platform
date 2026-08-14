select *
from {{ ref('dim_weather_place') }}
where admin_dong_code is null
   or source_admin_code is null
   or admin_dong_code <> source_admin_code
   or gu_code is null
   or gu_code <> substr(admin_dong_code, 1, 5)
