{# W1 내부 모델 공통 계약: source item identity, bounded lookback, initial-build guard. #}

{% macro weather_kma_item_key_version() -%}
'weather_kma_item_v1'
{%- endmacro %}

{% macro weather_kma_item_signature(base_date, base_time, nx, ny, category, fcst_date, fcst_time, fcst_value) -%}
{%- set tagged = [
    "case when " ~ base_date ~ " is null then 'N:<NULL>' else concat('V:', trim(cast(" ~ base_date ~ " as varchar))) end",
    "case when " ~ base_time ~ " is null then 'N:<NULL>' else concat('V:', trim(cast(" ~ base_time ~ " as varchar))) end",
    "case when " ~ nx ~ " is null then 'N:<NULL>' else concat('V:', coalesce(cast(try_cast(" ~ nx ~ " as integer) as varchar), trim(cast(" ~ nx ~ " as varchar)))) end",
    "case when " ~ ny ~ " is null then 'N:<NULL>' else concat('V:', coalesce(cast(try_cast(" ~ ny ~ " as integer) as varchar), trim(cast(" ~ ny ~ " as varchar)))) end",
    "case when " ~ category ~ " is null then 'N:<NULL>' else concat('V:', upper(trim(cast(" ~ category ~ " as varchar)))) end",
    "case when " ~ fcst_date ~ " is null then 'N:<NULL>' else concat('V:', trim(cast(" ~ fcst_date ~ " as varchar))) end",
    "case when " ~ fcst_time ~ " is null then 'N:<NULL>' else concat('V:', trim(cast(" ~ fcst_time ~ " as varchar))) end",
    "case when " ~ fcst_value ~ " is null then 'N:<NULL>' else concat('V:', trim(cast(" ~ fcst_value ~ " as varchar))) end"
] -%}
lower(to_hex(sha256(to_utf8(json_format(cast(array[
    {{ tagged | join(',\n    ') }}
] as json))))))
{%- endmacro %}

{% macro weather_w1_lookback_minutes() -%}
{%- set raw = var('weather_w1_lookback_minutes', default=30) -%}
{%- set value = raw | string -%}
{%- if not modules.re.fullmatch('^[1-9][0-9]{0,3}$', value) or (value | int) > 1440 -%}
    {{ exceptions.raise_compiler_error('weather_w1_lookback_minutes는 1~1440 정수여야 합니다.') }}
{%- endif -%}
{{ return(value) }}
{%- endmacro %}

{% macro weather_w1_prod_snapshot_bootstrap_allowed() -%}
{%- set source_schema = env_var('ASK_SEOUL_SCHEMA', 'ask_seoul') -%}
{%- set target_schema = weather_schema_name() -%}
{%- set snapshot_dag_run_id = var('weather_snapshot_dag_run_id', '') | string | trim -%}
{{ return(
    target.name == 'prod'
    and target.database == 'iceberg'
    and source_schema == 'weather_traffic_bronze'
    and target_schema == 'weather'
    and snapshot_dag_run_id != ''
) }}
{%- endmacro %}

{% macro weather_w1_assert_prod_snapshot_bootstrap_evidence() -%}
{%- if not execute or not weather_w1_prod_snapshot_bootstrap_allowed() -%}
    {{ return('') }}
{%- endif -%}
{%- set snapshot_dag_run_id = var('weather_snapshot_dag_run_id', '') | string | trim -%}
{%- set snapshot_literal = snapshot_dag_run_id | replace("'", "''") -%}
{%- set evidence_sql -%}
with latest_manifest_state as (
    {{ latest_manifest_run_state(
        'weather_bronze',
        'collection_run_manifest',
        'kma_vilage_fcst'
    ) }}
),
pinned_manifest as (
    select *
    from latest_manifest_state
    where dag_run_id = '{{ snapshot_literal }}'
),
bronze_counts as (
    select
        count(*) as bronze_row_count,
        count(distinct cast(raw_object_key as varchar)) as bronze_raw_object_count
    from {{ source('weather_bronze', 'kma_vilage_fcst') }}
    where cast(source_id as varchar) = 'kma_vilage_fcst'
      and cast(dag_run_id as varchar) = '{{ snapshot_literal }}'
)
select
    (select count(*) from pinned_manifest) as manifest_count,
    (
        select count_if(
            manifest_status is distinct from 'SUCCESS'
            or is_publishable is distinct from true
            or manifest_failure_reason is not null
            or manifest_expected_rows is null
            or manifest_actual_rows is null
            or manifest_expected_raw_objects is null
            or manifest_actual_raw_objects is null
            or not (
                manifest_expected_rows = manifest_actual_rows
                and manifest_expected_rows > 0
                and manifest_expected_raw_objects = manifest_actual_raw_objects
                and manifest_expected_raw_objects > 0
            )
        )
        from pinned_manifest
    ) as invalid_manifest_count,
    bronze_counts.bronze_row_count,
    bronze_counts.bronze_raw_object_count,
    coalesce((select max(manifest_actual_rows) from pinned_manifest), -1)
        as manifest_actual_rows,
    coalesce((select max(manifest_actual_raw_objects) from pinned_manifest), -1)
        as manifest_actual_raw_objects
from bronze_counts
{%- endset -%}
{%- set evidence = run_query(evidence_sql) -%}
{%- if evidence is none or evidence.rows | length != 1 -%}
    {{ exceptions.raise_compiler_error(
        'Weather W1 prod bootstrap evidence query가 단일 summary를 반환하지 않았습니다.'
    ) }}
{%- endif -%}
{%- set row = evidence.rows[0] -%}
{%- if (row[0] | int) != 1
    or (row[1] | int) != 0
    or (row[2] | int) <= 0
    or (row[3] | int) <= 0
    or (row[2] | int) != (row[4] | int)
    or (row[3] | int) != (row[5] | int) -%}
    {{ exceptions.raise_compiler_error(
        'Weather W1 prod bootstrap snapshot이 publishable non-empty manifest 또는 Bronze completeness 검증에 실패했습니다.'
    ) }}
{%- endif -%}
{{ return('') }}
{%- endmacro %}

{% macro weather_w1_initial_build_guard() -%}
{%- if flags.FULL_REFRESH -%}
    {{ exceptions.raise_compiler_error('Weather W1은 --full-refresh를 허용하지 않습니다.') }}
{%- endif -%}
{%- if execute and not is_incremental() -%}
    {%- set mode = var('weather_w1_initial_build_mode', '') -%}
    {%- set source_schema = env_var('ASK_SEOUL_SCHEMA', 'ask_seoul') -%}
    {%- set target_schema = weather_schema_name() -%}
    {%- set namespace_pattern = '^dev_[a-z0-9_]+_weather_contract_test_[0-9a-f]{24}$' -%}
    {%- set isolated_smoke = (
        mode == 'bounded_isolated_smoke'
        and target.database == 'iceberg_dev'
        and source_schema == target_schema
        and modules.re.fullmatch(namespace_pattern, target_schema)
    ) -%}
    {%- set prod_snapshot_bootstrap = weather_w1_prod_snapshot_bootstrap_allowed() -%}
    {%- if prod_snapshot_bootstrap -%}
        {%- do weather_w1_assert_prod_snapshot_bootstrap_evidence() -%}
    {%- endif -%}
    {%- if not isolated_smoke and not prod_snapshot_bootstrap -%}
        {%- if not weather_w2_shared_dev_build_allowed() -%}
            {{ exceptions.raise_compiler_error(
                'Weather W1 최초 빌드는 동일한 unique isolated dev source/target와 '
                ~ 'weather_w1_initial_build_mode=bounded_isolated_smoke, '
                ~ '검증된 W2 bounded DEV repair 또는 pinned canonical prod snapshot에서만 허용됩니다.'
            ) }}
        {%- endif -%}
        {%- do weather_w2_assert_repair_evidence() -%}
    {%- endif -%}
{%- endif -%}
{%- endmacro %}

{% macro weather_w1_candidate_environment_guard(candidate_name) -%}
{%- if flags.FULL_REFRESH -%}
    {{ exceptions.raise_compiler_error(candidate_name ~ '은 --full-refresh를 허용하지 않습니다.') }}
{%- endif -%}
{%- if execute -%}
    {%- set mode = var('weather_w1_initial_build_mode', '') -%}
    {%- set target_schema = weather_schema_name() -%}
    {%- set namespace_pattern = '^dev_[a-z0-9_]+_weather_contract_test_[0-9a-f]{24}$' -%}
    {%- set isolated_smoke = (
        mode == 'bounded_isolated_smoke'
        and target.database == 'iceberg_dev'
        and modules.re.fullmatch(namespace_pattern, target_schema)
    ) -%}
    {%- set prod_snapshot_bootstrap = weather_w1_prod_snapshot_bootstrap_allowed() -%}
    {%- if prod_snapshot_bootstrap -%}
        {%- do weather_w1_assert_prod_snapshot_bootstrap_evidence() -%}
    {%- endif -%}
    {%- if not isolated_smoke and not prod_snapshot_bootstrap -%}
        {%- if not weather_w2_shared_dev_build_allowed() -%}
            {{ exceptions.raise_compiler_error(
                candidate_name ~ '은 unique isolated iceberg_dev namespace와 '
                ~ 'weather_w1_initial_build_mode=bounded_isolated_smoke, '
                ~ '검증된 W2 bounded DEV repair 또는 pinned canonical prod snapshot에서만 실행할 수 있습니다.'
            ) }}
        {%- endif -%}
        {%- do weather_w2_assert_repair_evidence() -%}
    {%- endif -%}
{%- endif -%}
{{ return('') }}
{%- endmacro %}

{% macro weather_w1_candidate_seed_guard() -%}
{%- do weather_w1_candidate_environment_guard('weather_admin_dong_grid_bridge_history') -%}
select 1
{%- endmacro %}
