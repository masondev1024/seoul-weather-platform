{% macro weather_delete_nonpublishable_kma_silver_runs(target_relation) -%}
{%- if is_incremental() -%}
delete from {{ target_relation }}
where dag_run_id in (
    select dag_run_id
    from (
        {{ latest_manifest_run_state(
            'weather_bronze',
            'collection_run_manifest',
            'kma_vilage_fcst'
        ) }}
    ) as latest_manifest_state
    where manifest_status <> 'SUCCESS'
       or not is_publishable
)
{%- endif -%}
{%- endmacro %}
