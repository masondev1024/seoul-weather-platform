{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['grid_id', 'observed_at', 'variable', 'truth_revision'],
    on_schema_change='fail',
    full_refresh=false,
    views_enabled=false,
    on_table_exists='drop',
    properties={
        "partitioning": "ARRAY['day(observed_at)']"
    },
    tags=['ask_seoul_weather_quality_candidate']
) }}

{% if execute %}
  {% do weather_quality_validate_runtime_contract() %}
  {% set window_start_date = weather_quality_window_start_date() %}
  {% set window_end_date = weather_quality_window_end_date() %}
  {% set evaluation_as_of = weather_quality_evaluation_as_of() %}
{% else %}
  {% set window_start_date = "cast('1970-01-01' as date)" %}
  {% set window_end_date = "cast('1970-01-01' as date)" %}
  {% set evaluation_as_of = "from_iso8601_timestamp('1970-01-01T00:00:00+00:00')" %}
{% endif %}

with coverage_grid as (
    select
        grid_id,
        nx,
        ny
    from {{ ref('dim_weather_coverage_grid') }}
),

raw_bronze_scope as (
    select
        cast(bronze.idempotency_key as varchar) as idempotency_key,
        cast(bronze.request_id as varchar) as request_id,
        cast(bronze.source_id as varchar) as source_id,
        cast(bronze.dag_run_id as varchar) as dag_run_id,
        cast(bronze.manifest_key as varchar) as manifest_key,
        cast(bronze.observed_slot as varchar) as observed_slot,
        cast(bronze.observed_at as timestamp(6)) as observed_at,
        cast(bronze.base_date as varchar) as base_date,
        cast(bronze.base_time as varchar) as base_time,
        coverage_grid.grid_id,
        cast(bronze.nx as integer) as nx,
        cast(bronze.ny as integer) as ny,
        cast(bronze.category as varchar) as category,
        cast(bronze.observed_value as double) as observed_value,
        cast(bronze.unit as varchar) as unit,
        cast(bronze.quality_status as varchar) as source_quality_status,
        cast(bronze.raw_object_key as varchar) as raw_object_key,
        cast(bronze.payload_sha256 as varchar) as payload_sha256,
        cast(bronze.source_revision as varchar) as source_revision,
        cast(bronze.source_revision as varchar) as truth_revision,
        cast(bronze.http_status as integer) as http_status,
        cast(bronze.collected_at as timestamp(6)) as collected_at
    from {{ source('weather_bronze', 'kma_ultra_srt_ncst') }} as bronze
    left join coverage_grid
        on cast(bronze.nx as integer) = coverage_grid.nx
       and cast(bronze.ny as integer) = coverage_grid.ny
    where cast(bronze.observed_at as timestamp(6)) >= cast({{ window_start_date }} as timestamp) - interval '9' hour
      and cast(bronze.observed_at as timestamp(6)) < cast({{ window_end_date }} as timestamp) + interval '1' day - interval '9' hour
      and cast(bronze.observed_at as timestamp(6)) < date_trunc('day', cast({{ evaluation_as_of }} as timestamp(6)) + interval '9' hour) - interval '9' hour
),

run_counts as (
    select
        observed_at,
        dag_run_id,
        manifest_key,
        max(collected_at) as run_collected_at,
        count(*) as row_count,
        count(distinct grid_id) as canonical_grid_count,
        count(distinct case
            when category in ('T1H', 'RN1', 'UUU', 'VVV', 'REH', 'PTY', 'VEC', 'WSD')
                then category
        end) as category_count,
        count(distinct case
            when grid_id is not null
                and category in ('T1H', 'RN1', 'UUU', 'VVV', 'REH', 'PTY', 'VEC', 'WSD')
                then grid_id || ':' || category
        end) as grid_category_count,
        count_if(source_revision is not null) as source_revision_count,
        count_if(source_id = 'kma_ultra_srt_ncst') as source_id_count,
        count_if(collected_at <= cast({{ evaluation_as_of }} as timestamp(6))) as visible_row_count,
        count_if(source_id != 'kma_ultra_srt_ncst' or source_id is null) as wrong_source_row_count,
        count_if(grid_id is null) as noncanonical_grid_row_count,
        count_if(category not in ('T1H', 'RN1', 'UUU', 'VVV', 'REH', 'PTY', 'VEC', 'WSD') or category is null) as invalid_category_row_count,
        count_if(source_revision is null) as null_source_revision_row_count,
        count_if(
            observed_at is null
            or dag_run_id is null
            or manifest_key is null
            or idempotency_key is null
            or raw_object_key is null
            or payload_sha256 is null
            or collected_at is null
        ) as null_scope_identity_row_count,
        count(*) - count(distinct case
            when grid_id is not null
                and category in ('T1H', 'RN1', 'UUU', 'VVV', 'REH', 'PTY', 'VEC', 'WSD')
                then grid_id || ':' || category
        end) as duplicate_grid_category_row_count
    from raw_bronze_scope
    group by observed_at, dag_run_id, manifest_key
),

complete_runs as (
    select
        observed_at,
        dag_run_id,
        manifest_key,
        run_collected_at
    from run_counts
    where row_count = 640
      and canonical_grid_count = 80
      and category_count = 8
      and grid_category_count = 640
      and source_revision_count = 640
      and source_id_count = 640
      and visible_row_count = 640
      and wrong_source_row_count = 0
      and noncanonical_grid_row_count = 0
      and invalid_category_row_count = 0
      and null_source_revision_row_count = 0
      and null_scope_identity_row_count = 0
      and duplicate_grid_category_row_count = 0
),

selected_runs as (
    select
        observed_at,
        dag_run_id,
        manifest_key
    from (
        select
            observed_at,
            dag_run_id,
            manifest_key,
            row_number() over (
                partition by observed_at
                order by run_collected_at desc, dag_run_id desc, manifest_key desc
            ) as row_num
        from complete_runs
    ) as ranked_complete_runs
    where row_num = 1
),

eligible_required_rows as (
    select raw_bronze_scope.*
    from raw_bronze_scope
    inner join selected_runs
        on raw_bronze_scope.observed_at = selected_runs.observed_at
       and raw_bronze_scope.dag_run_id = selected_runs.dag_run_id
       and raw_bronze_scope.manifest_key = selected_runs.manifest_key
    where raw_bronze_scope.source_id = 'kma_ultra_srt_ncst'
      and raw_bronze_scope.grid_id is not null
      and raw_bronze_scope.category in ('T1H', 'RN1', 'UUU', 'VVV', 'REH', 'PTY', 'VEC', 'WSD')
      and raw_bronze_scope.source_revision is not null
      and raw_bronze_scope.collected_at <= cast({{ evaluation_as_of }} as timestamp(6))
),

pivoted as (
    select
        grid_id,
        nx,
        ny,
        observed_at,
        dag_run_id,
        manifest_key,
        max(collected_at) as collected_at,
        max_by(
            idempotency_key,
            concat(
                cast(collected_at as varchar),
                '|', coalesce(source_revision, ''),
                '|', coalesce(raw_object_key, ''),
                '|', coalesce(request_id, '')
            )
        ) as representative_idempotency_key,
        max_by(
            request_id,
            concat(
                cast(collected_at as varchar),
                '|', coalesce(source_revision, ''),
                '|', coalesce(raw_object_key, ''),
                '|', coalesce(request_id, '')
            )
        ) as representative_request_id,
        max_by(
            raw_object_key,
            concat(
                cast(collected_at as varchar),
                '|', coalesce(source_revision, ''),
                '|', coalesce(raw_object_key, ''),
                '|', coalesce(request_id, '')
            )
        ) as representative_raw_object_key,
        max_by(
            payload_sha256,
            concat(
                cast(collected_at as varchar),
                '|', coalesce(source_revision, ''),
                '|', coalesce(raw_object_key, ''),
                '|', coalesce(request_id, '')
            )
        ) as representative_payload_sha256,
        max_by(
            source_revision,
            concat(
                cast(collected_at as varchar),
                '|', coalesce(source_revision, ''),
                '|', coalesce(raw_object_key, ''),
                '|', coalesce(request_id, '')
            )
        ) as representative_source_revision,
        max(case when category = 'T1H' then observed_value end) as t1h_value,
        max(case when category = 'T1H' then cast(observed_value as varchar) end) as t1h_raw_value,
        max(case when category = 'T1H' then source_revision end) as t1h_source_revision,
        max(case when category = 'T1H' then raw_object_key end) as t1h_raw_object_key,
        max(case when category = 'T1H' then payload_sha256 end) as t1h_payload_sha256,
        max(case when category = 'PTY' then observed_value end) as pty_value,
        max(case when category = 'PTY' then cast(observed_value as varchar) end) as pty_raw_value,
        max(case when category = 'PTY' then source_revision end) as pty_source_revision,
        max(case when category = 'PTY' then raw_object_key end) as pty_raw_object_key,
        max(case when category = 'PTY' then payload_sha256 end) as pty_payload_sha256,
        max(case when category = 'RN1' then observed_value end) as rn1_value,
        max(case when category = 'RN1' then cast(observed_value as varchar) end) as rn1_raw_value,
        max(case when category = 'RN1' then source_revision end) as rn1_source_revision,
        max(case when category = 'RN1' then raw_object_key end) as rn1_raw_object_key,
        max(case when category = 'RN1' then payload_sha256 end) as rn1_payload_sha256
    from eligible_required_rows
    group by grid_id, nx, ny, observed_at, dag_run_id, manifest_key
),

truth_rows as (
    select
        grid_id,
        nx,
        ny,
        observed_at,
        'temperature_air_2m' as variable,
        'continuous' as value_kind,
        'degC' as unit,
        t1h_raw_value as raw_value,
        t1h_value as value_num,
        cast(null as boolean) as value_bool,
        cast(null as varchar) as value_category,
        case
            when is_finite(t1h_value) then 'provisional'
            else 'invalid_truth'
        end as truth_status,
        'T1H' as source_variable,
        t1h_source_revision as source_revision,
        t1h_source_revision as truth_revision,
        collected_at,
        dag_run_id,
        manifest_key,
        t1h_raw_object_key as raw_object_key,
        t1h_payload_sha256 as payload_sha256,
        representative_idempotency_key,
        representative_request_id,
        representative_raw_object_key,
        representative_payload_sha256,
        representative_source_revision
    from pivoted

    union all

    select
        grid_id,
        nx,
        ny,
        observed_at,
        'precipitation_occurrence' as variable,
        'binary' as value_kind,
        '1' as unit,
        concat('PTY=', pty_raw_value, ';RN1=', rn1_raw_value) as raw_value,
        cast(null as double) as value_num,
        case
            when try_cast(pty_value as integer) in (0, 1, 2, 3, 4, 5, 6, 7)
                and pty_value = try_cast(pty_value as integer)
                and rn1_value is not null
                and is_finite(rn1_value)
                and rn1_value >= 0
                and try_cast(pty_value as integer) = 0
                and rn1_value = 0
                then false
            when try_cast(pty_value as integer) in (0, 1, 2, 3, 4, 5, 6, 7)
                and pty_value = try_cast(pty_value as integer)
                and rn1_value is not null
                and is_finite(rn1_value)
                and rn1_value >= 0
                and (try_cast(pty_value as integer) > 0 or rn1_value > 0)
                then true
            when pty_value is null or rn1_value is null then null
            else null
        end as value_bool,
        cast(null as varchar) as value_category,
        case
            when try_cast(pty_value as integer) in (0, 1, 2, 3, 4, 5, 6, 7)
                and pty_value = try_cast(pty_value as integer)
                and rn1_value is not null
                and is_finite(rn1_value)
                and rn1_value >= 0
                then 'provisional'
            else 'invalid_truth'
        end as truth_status,
        'PTY+RN1' as source_variable,
        pty_source_revision || '|' || rn1_source_revision as source_revision,
        pty_source_revision || '|' || rn1_source_revision as truth_revision,
        collected_at,
        dag_run_id,
        manifest_key,
        pty_raw_object_key || '|' || rn1_raw_object_key as raw_object_key,
        pty_payload_sha256 || '|' || rn1_payload_sha256 as payload_sha256,
        representative_idempotency_key,
        representative_request_id,
        representative_raw_object_key,
        representative_payload_sha256,
        representative_source_revision
    from pivoted

    union all

    select
        grid_id,
        nx,
        ny,
        observed_at,
        'precipitation_occurrence_category' as variable,
        'categorical' as value_kind,
        'category' as unit,
        concat('PTY=', pty_raw_value, ';RN1=', rn1_raw_value) as raw_value,
        cast(null as double) as value_num,
        cast(null as boolean) as value_bool,
        case
            when try_cast(pty_value as integer) in (0, 1, 2, 3, 4, 5, 6, 7)
                and pty_value = try_cast(pty_value as integer)
                and rn1_value is not null
                and is_finite(rn1_value)
                and rn1_value >= 0
                and try_cast(pty_value as integer) = 0
                and rn1_value = 0
                then 'dry'
            when try_cast(pty_value as integer) in (0, 1, 2, 3, 4, 5, 6, 7)
                and pty_value = try_cast(pty_value as integer)
                and rn1_value is not null
                and is_finite(rn1_value)
                and rn1_value >= 0
                and (try_cast(pty_value as integer) > 0 or rn1_value > 0)
                then 'wet'
            when pty_value is null or rn1_value is null then null
            else null
        end as value_category,
        case
            when try_cast(pty_value as integer) in (0, 1, 2, 3, 4, 5, 6, 7)
                and pty_value = try_cast(pty_value as integer)
                and rn1_value is not null
                and is_finite(rn1_value)
                and rn1_value >= 0
                then 'provisional'
            else 'invalid_truth'
        end as truth_status,
        'PTY+RN1' as source_variable,
        pty_source_revision || '|' || rn1_source_revision as source_revision,
        pty_source_revision || '|' || rn1_source_revision as truth_revision,
        collected_at,
        dag_run_id,
        manifest_key,
        pty_raw_object_key || '|' || rn1_raw_object_key as raw_object_key,
        pty_payload_sha256 || '|' || rn1_payload_sha256 as payload_sha256,
        representative_idempotency_key,
        representative_request_id,
        representative_raw_object_key,
        representative_payload_sha256,
        representative_source_revision
    from pivoted
),

ranked as (
    select
        *,
        row_number() over (
            partition by grid_id, observed_at, variable, truth_revision
            order by collected_at desc, source_revision desc, dag_run_id desc, manifest_key desc, raw_object_key desc
        ) as row_num
    from truth_rows
    where truth_revision is not null
)

select
    grid_id,
    nx,
    ny,
    observed_at,
    variable,
    value_kind,
    unit,
    raw_value,
    value_num,
    value_bool,
    value_category,
    truth_status,
    source_variable,
    source_revision,
    truth_revision,
    'kma_ultra_srt_ncst' as source_id,
    'kma_ultra_srt_ncst' as truth_source,
    'provisional' as truth_quality,
    collected_at,
    collected_at as truth_as_of,
    dag_run_id,
    manifest_key,
    raw_object_key,
    payload_sha256,
    representative_idempotency_key,
    representative_request_id,
    representative_raw_object_key,
    representative_payload_sha256,
    representative_source_revision
from ranked
where row_num = 1
