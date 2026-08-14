select
    place_id,
    forecast_at,
    forecast_issued_at_min,
    forecast_issued_at_max
from {{ ref('gold_weather_place_hourly_outlook') }}
where forecast_issued_at_min is null
   or forecast_issued_at_max is null
   or forecast_issued_at_min <> forecast_issued_at_max
