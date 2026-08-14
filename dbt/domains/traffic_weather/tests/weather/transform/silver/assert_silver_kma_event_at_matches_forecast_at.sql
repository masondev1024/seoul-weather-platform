select *
from {{ ref('silver_kma_vilage_fcst') }}
where event_at is null
   or event_at <> forecast_at
