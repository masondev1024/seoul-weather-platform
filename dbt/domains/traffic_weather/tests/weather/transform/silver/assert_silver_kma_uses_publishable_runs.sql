with latest_manifest_state as (
    {{ latest_manifest_run_state('weather_bronze', 'collection_run_manifest', 'kma_vilage_fcst') }}
),

publishable_runs as (
    select dag_run_id
    from latest_manifest_state
    where manifest_status = 'SUCCESS'
      and is_publishable
)

select silver.dag_run_id
from {{ ref('silver_kma_vilage_fcst') }} as silver
left join publishable_runs as manifest
    on silver.dag_run_id = manifest.dag_run_id
where manifest.dag_run_id is null
limit 1
