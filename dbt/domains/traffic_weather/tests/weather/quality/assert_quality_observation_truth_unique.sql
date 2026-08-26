select
    grid_id,
    observed_at,
    variable,
    truth_revision,
    count(*) as row_count
from {{ ref('silver_kma_observation_truth') }}
group by grid_id, observed_at, variable, truth_revision
having count(*) > 1
