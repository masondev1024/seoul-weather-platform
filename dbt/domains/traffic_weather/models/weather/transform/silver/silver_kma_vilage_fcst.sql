-- incremental 전환(#145, DL-013): 풀 리빌드의 전이력 window dedup가 브론즈 선형 증가로
-- per-node 메모리 한도를 초과. 신규 publishable run 델타에만 dedup를 돌리고 배치 간
-- 중복은 merge unique_key(grain 테스트와 동일 키)로 처리한다.
-- views_enabled/on_table_exists 는 R2 카탈로그 유령 뷰 409 우회(#70 traffic 선례).
-- 주의: full-refresh는 풀 리빌드 경로라 메모리 절벽 재발 — 전체 재빌드가 필요하면
-- Trino 메모리 임시 상향 또는 base_date 배치 분할로 수행할 것.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['place_id', 'nx', 'ny', 'issued_at', 'category', 'forecast_at'],
    on_schema_change='fail',
    views_enabled=false,
    on_table_exists='drop',
    pre_hook="{{ weather_delete_nonpublishable_kma_silver_runs(this) }}",
) }}

{% set snapshot_dag_run_id = var('weather_snapshot_dag_run_id') %}
{% set snapshot_load_date = var('weather_snapshot_load_date') %}
{% set historical_transform = var('weather_historical_transform', false) %}

with latest_manifest_state as (
    {{ latest_manifest_run_state('weather_bronze', 'collection_run_manifest', 'kma_vilage_fcst') }}
),

publishable_runs as (
    select distinct dag_run_id
    from latest_manifest_state
    where manifest_status = 'SUCCESS'
      and is_publishable
      and dag_run_id = '{{ snapshot_dag_run_id | replace("'", "''") }}'
),

bronze as (
    select
        cast(bronze.request_id as varchar) as request_id,
        cast(bronze.source_id as varchar) as source_id,
        cast(bronze.request_params_json as varchar) as request_params_json,
        cast(bronze.place_id as varchar) as place_id,
        cast(bronze.base_date as varchar) as base_date,
        cast(bronze.base_time as varchar) as base_time,
        cast(bronze.nx as integer) as nx,
        cast(bronze.ny as integer) as ny,
        cast(bronze.category as varchar) as category,
        cast(bronze.fcst_date as varchar) as fcst_date,
        cast(bronze.fcst_time as varchar) as fcst_time,
        cast(bronze.fcst_value as varchar) as fcst_value_raw,
        try_cast(bronze.fcst_value as double) as fcst_value_num,
        cast(bronze.raw_object_key as varchar) as raw_object_key,
        cast(bronze.payload_hash as varchar) as payload_hash,
        cast(bronze.result_code as varchar) as result_code,
        cast(bronze.result_msg as varchar) as result_msg,
        cast(bronze.total_count as integer) as total_count,
        cast(bronze.item_count as integer) as item_count,
        cast(bronze.collected_at as timestamp(6)) as collected_at,
        cast(bronze.load_date as varchar) as load_date,
        cast(bronze.dag_run_id as varchar) as dag_run_id
    from {{ source('weather_bronze', 'kma_vilage_fcst') }} as bronze
    inner join publishable_runs
        on cast(bronze.dag_run_id as varchar) = publishable_runs.dag_run_id
    where bronze.load_date = '{{ snapshot_load_date | replace("'", "''") }}'
    {% if is_incremental() and not historical_transform %}
    -- 증분 커서는 dag_run_id 가 아니라 collected_at 워터마크. run ID 앙티조인은
    -- dedup 에서 전량 패배해 silver 에 ID 를 못 남긴 run(전량 섀도잉된 중복 수집)을
    -- 매 run 재선택하는 순환을 만든다 — 관측: 배치당 139,360행 재머지.
      and cast(bronze.collected_at as timestamp(6)) >= (
        select coalesce(max(collected_at), timestamp '1970-01-01 00:00:00')
               - interval '{{ weather_w1_lookback_minutes() }}' minute
        from {{ this }}
    )
    {% endif %}
),

standardized as (
    select
        *,
        {{ asac_axes.kst_at_from_parts('base_date', 'base_time') }} as issued_at,
        {{ asac_axes.kst_at_from_parts('fcst_date', 'fcst_time') }} as forecast_at
    from bronze
    where result_code = '00'
),

ranked as (
    select
        *,
        row_number() over (
            partition by place_id, nx, ny, base_date, base_time, category, fcst_date, fcst_time
            order by collected_at desc, raw_object_key desc, request_id desc
        ) as row_num
    from standardized
    where issued_at is not null
      and forecast_at is not null
)

select
    request_id,
    source_id,
    request_params_json,
    place_id,
    nx,
    ny,
    category,
    issued_at,
    forecast_at,
    forecast_at as event_at,
    date_trunc('hour', forecast_at) as time_bucket,
    fcst_value_raw,
    fcst_value_num,
    -- 값 의미 계층(#113): 표현 분류·정량치·범위·코드. fcst_value_num(단일 try_cast)은
    -- 호환용으로 유지 — 신규 소비자는 value_representation + value_num 을 사용할 것.
    {{ kma_value_semantics('category', 'fcst_value_raw') }},
    -- 예보 리드타임(사실값). 원구간(실측 lead>=50h)에서 PCP/SNO 가 bare_numeric
    -- 체제로 전환되는 상관이 관측됨 — 경계 불리언은 공식 문서 검증 전이라 두지 않는다.
    date_diff('hour', issued_at, forecast_at) as forecast_lead_hours,
    raw_object_key,
    payload_hash,
    total_count,
    item_count,
    load_date,
    collected_at,
    dag_run_id
from ranked
where row_num = 1
