select
    place_id,
    nx,
    ny,
    issued_at,
    category,
    forecast_at,
    count(*) as row_count
from {{ ref('silver_kma_vilage_fcst') }}
group by place_id, nx, ny, issued_at, category, forecast_at
having count(*) > 1
