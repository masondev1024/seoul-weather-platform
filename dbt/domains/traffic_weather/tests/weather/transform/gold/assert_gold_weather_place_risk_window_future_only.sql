with kst_now as (
    select {{ weather_serving_as_of_hour() }} as current_hour_at
)

select risk.product_row_id
from {{ ref('gold_weather_place_risk_window') }} as risk
cross join kst_now
where risk.forecast_at < kst_now.current_hour_at
