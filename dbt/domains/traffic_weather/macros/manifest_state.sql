{#
  Bronze manifest is an append-only state history. Consumers must choose the
  latest state for a source/run before deciding whether it is publishable;
  filtering SUCCESS first would resurrect an older state after COALESCED.
#}
{% macro latest_manifest_run_state(source_name, table_name, expected_source_id) -%}
select
    source_id,
    dag_run_id,
    collection_dag_id,
    manifest_status,
    is_publishable,
    manifest_expected_rows,
    manifest_actual_rows,
    manifest_expected_raw_objects,
    manifest_actual_raw_objects,
    manifest_failure_reason,
    manifest_event_at_utc,
    manifest_state_tie_count
from (
    select
        cast(source_id as varchar) as source_id,
        cast(dag_run_id as varchar) as dag_run_id,
        cast(dag_id as varchar) as collection_dag_id,
        cast(status as varchar) as manifest_status,
        cast(is_publishable as boolean) as is_publishable,
        cast(expected_rows as bigint) as manifest_expected_rows,
        cast(actual_rows as bigint) as manifest_actual_rows,
        cast(expected_raw_objects as bigint) as manifest_expected_raw_objects,
        cast(actual_raw_objects as bigint) as manifest_actual_raw_objects,
        cast(failure_reason as varchar) as manifest_failure_reason,
        cast(event_at as timestamp(6)) as manifest_event_at_utc,
        count(*) over (
            partition by
                cast(source_id as varchar),
                cast(dag_run_id as varchar),
                cast(event_at as timestamp(6)),
                cast(dag_id as varchar)
        ) as manifest_state_tie_count,
        row_number() over (
            partition by cast(source_id as varchar), cast(dag_run_id as varchar)
            order by cast(event_at as timestamp(6)) desc, cast(dag_id as varchar) desc
        ) as manifest_row_num
    from {{ source(source_name, table_name) }}
    where cast(source_id as varchar) = '{{ expected_source_id | replace("'", "''") }}'
) as manifest_state
where manifest_row_num = 1
  and manifest_state_tie_count = 1
{%- endmacro %}
