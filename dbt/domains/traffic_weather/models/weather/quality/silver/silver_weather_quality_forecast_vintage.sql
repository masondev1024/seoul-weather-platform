{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['grid_id', 'valid_at', 'variable', 'issued_at', 'source_revision'],
    on_schema_change='fail',
    full_refresh=false,
    views_enabled=false,
    on_table_exists='drop',
    properties={
        "partitioning": "ARRAY['day(valid_at)']"
    },
    tags=['ask_seoul_weather_quality_candidate']
) }}

{% if execute %}
  {% do weather_quality_validate_runtime_contract() %}
  {% set forecast_load_start_date = weather_quality_forecast_load_start_date() %}
  {% set forecast_load_end_date = weather_quality_forecast_load_end_date() %}
{% else %}
  {% set forecast_load_start_date = "cast('1970-01-01' as date)" %}
  {% set forecast_load_end_date = "cast('1970-01-01' as date)" %}
{% endif %}

with latest_manifest_state as (
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
        manifest_event_at_utc
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
        from {{ source('weather_bronze', 'collection_run_manifest') }}
        where cast(source_id as varchar) = 'kma_vilage_fcst'
    ) as manifest_state
    where manifest_row_num = 1
      and manifest_state_tie_count = 1
),

publishable_runs as (
    select
        dag_run_id,
        collection_dag_id,
        manifest_expected_rows,
        manifest_actual_rows,
        manifest_expected_raw_objects,
        manifest_actual_raw_objects,
        manifest_event_at_utc
    from latest_manifest_state
    where manifest_status = 'SUCCESS'
      and is_publishable
),

coverage_grid as (
    select
        grid_id,
        nx,
        ny
    from {{ ref('dim_weather_coverage_grid') }}
),

bronze as (
    select
        cast(bronze.request_id as varchar) as request_id,
        cast(bronze.source_id as varchar) as source_id,
        cast(bronze.request_params_json as varchar) as request_params_json,
        cast(bronze.place_id as varchar) as bronze_place_id,
        cast(bronze.base_date as varchar) as base_date,
        cast(bronze.base_time as varchar) as base_time,
        cast(bronze.nx as integer) as nx,
        cast(bronze.ny as integer) as ny,
        cast(bronze.category as varchar) as source_variable,
        cast(bronze.fcst_date as varchar) as fcst_date,
        cast(bronze.fcst_time as varchar) as fcst_time,
        nullif(trim(cast(bronze.fcst_value as varchar)), '') as raw_value,
        try_cast(nullif(trim(cast(bronze.fcst_value as varchar)), '') as double) as raw_value_num,
        cast(bronze.raw_object_key as varchar) as raw_object_key,
        cast(bronze.payload_hash as varchar) as source_revision,
        cast(bronze.result_code as varchar) as result_code,
        cast(bronze.collected_at as timestamp(6)) as collected_at,
        cast(bronze.load_date as varchar) as load_date,
        cast(bronze.dag_run_id as varchar) as dag_run_id,
        publishable_runs.collection_dag_id,
        publishable_runs.manifest_expected_rows,
        publishable_runs.manifest_actual_rows,
        publishable_runs.manifest_expected_raw_objects,
        publishable_runs.manifest_actual_raw_objects,
        publishable_runs.manifest_event_at_utc
    from {{ source('weather_bronze', 'kma_vilage_fcst') }} as bronze
    inner join publishable_runs
        on cast(bronze.dag_run_id as varchar) = publishable_runs.dag_run_id
    where bronze.load_date >= cast({{ forecast_load_start_date }} as varchar)
      and bronze.load_date <= cast({{ forecast_load_end_date }} as varchar)
      and cast(bronze.category as varchar) in ('TMP', 'POP', 'PTY')
      and cast(bronze.result_code as varchar) = '00'
),

standardized as (
    select
        coverage_grid.grid_id,
        bronze.nx,
        bronze.ny,
        {{ asac_axes.kst_at_from_parts('base_date', 'base_time') }} as issued_at,
        {{ asac_axes.kst_at_from_parts('fcst_date', 'fcst_time') }} as valid_at,
        case bronze.source_variable
            when 'TMP' then 'temperature_air_2m'
            when 'POP' then 'precipitation_occurrence'
            when 'PTY' then 'precipitation_occurrence_category'
        end as variable,
        case bronze.source_variable
            when 'TMP' then 'continuous'
            when 'POP' then 'probability'
            when 'PTY' then 'categorical'
        end as value_kind,
        case bronze.source_variable
            when 'TMP' then 'degC'
            when 'POP' then '1'
            when 'PTY' then 'category'
        end as unit,
        bronze.raw_value,
        bronze.raw_value_num,
        case
            when bronze.source_variable = 'TMP' and bronze.raw_value_num is not null
                then bronze.raw_value_num
            when bronze.source_variable = 'POP'
                and bronze.raw_value_num between 0.0 and 100.0
                then bronze.raw_value_num / 100.0
            else null
        end as value_num,
        case
            when bronze.source_variable = 'PTY'
                and try_cast(bronze.raw_value_num as integer) = 0
                and bronze.raw_value_num = try_cast(bronze.raw_value_num as integer)
                then 'dry'
            when bronze.source_variable = 'PTY'
                and try_cast(bronze.raw_value_num as integer) in (1, 2, 3, 4)
                and bronze.raw_value_num = try_cast(bronze.raw_value_num as integer)
                then 'wet'
            else null
        end as value_category,
        case
            when bronze.source_variable = 'TMP' and bronze.raw_value_num is not null
                then 'valid'
            when bronze.source_variable = 'POP' and bronze.raw_value_num between 0.0 and 100.0
                then 'valid'
            when bronze.source_variable = 'PTY'
                and try_cast(bronze.raw_value_num as integer) in (0, 1, 2, 3, 4)
                and bronze.raw_value_num = try_cast(bronze.raw_value_num as integer)
                then 'valid'
            else 'invalid'
        end as value_status,
        bronze.source_variable,
        bronze.source_revision,
        bronze.request_id,
        bronze.source_id,
        bronze.request_params_json,
        bronze.bronze_place_id,
        bronze.raw_object_key,
        bronze.load_date,
        bronze.collected_at,
        bronze.dag_run_id,
        bronze.collection_dag_id,
        bronze.manifest_expected_rows,
        bronze.manifest_actual_rows,
        bronze.manifest_expected_raw_objects,
        bronze.manifest_actual_raw_objects,
        bronze.manifest_event_at_utc
    from bronze
    inner join coverage_grid
        on bronze.nx = coverage_grid.nx
       and bronze.ny = coverage_grid.ny
),

ranked as (
    select
        *,
        row_number() over (
            partition by grid_id, valid_at, variable, issued_at, source_revision
            order by collected_at desc, raw_object_key desc, request_id desc, dag_run_id desc
        ) as row_num
    from standardized
    where issued_at is not null
      and valid_at is not null
      and source_revision is not null
)

select
    grid_id,
    nx,
    ny,
    valid_at,
    issued_at,
    variable,
    value_kind,
    unit,
    raw_value,
    raw_value_num,
    value_num,
    value_category,
    value_status,
    source_variable,
    source_revision,
    request_id,
    source_id,
    request_params_json,
    bronze_place_id,
    raw_object_key,
    load_date,
    collected_at,
    dag_run_id,
    collection_dag_id,
    manifest_expected_rows,
    manifest_actual_rows,
    manifest_expected_raw_objects,
    manifest_actual_raw_objects,
    manifest_event_at_utc
from ranked
where row_num = 1
