{# dim_admin_dong.sql과 같은 조인을 재구성하되 seoul_admin_dong_crosswalk만
   FOR VERSION AS OF로 고정한다. dim_admin_dong 자체를 고치지 않는 이유: view라서
   snapshot을 박으면 그 순간에 굳어버려 평상시 용도로 못 쓴다(ASAC-DAG #480 설계
   docs/superpowers/specs/2026-07-23-shared-admin-dong-axis-version-pin-design.md).
   dim_admin_dong.sql이 바뀌면 이 매크로도 같이 갱신해야 한다. #}

{% macro admin_dong_crosswalk_pin_snapshot_id() -%}
  {%- set snapshot_id = var('admin_dong_crosswalk_pin_snapshot_id', none) -%}
  {#- parse 단계(execute=False)에서 var 가 없으면 placeholder 0 을 돌려준다.
      여기서 raise 하면 pin var 를 넘기지 않는 모든 dbt 명령의 "프로젝트 전체"
      parse 가 깨진다 — 다른 도메인 DAG, CI, dbt ls/parse 를 내부에서 돌리는
      테스트(test_gold_trigger_selectors, test_source_freshness_slo)까지 전부.
      traffic citydata snapshot pin(macros/traffic/traffic_external_snapshot.sql)
      과 같은 방식이며, 실제 compile/run/test 시점에는 아래 검증이 fail-closed 로
      동작한다. -#}
  {%- if snapshot_id is none and not execute -%}
    {{ return(0) }}
  {%- endif -%}
  {%- if snapshot_id is not integer or snapshot_id <= 0 -%}
    {{ exceptions.raise_compiler_error(
      'pinned_dim_admin_dong() requires a positive integer admin_dong_crosswalk_pin_snapshot_id.'
    ) }}
  {%- endif -%}
  {{ return(snapshot_id) }}
{%- endmacro %}

{% macro pinned_dim_admin_dong() %}
{%- set snapshot_id = asac_axes.admin_dong_crosswalk_pin_snapshot_id() -%}
(
    with latest as (
        select max(revision_date) as revision_date
        from {{ source('axes_bronze', 'admin_dong_master') }}
    ),
    dong as (
        select distinct
            admin_dong_code,
            admin_dong_nm as admin_dong,
            substr(admin_dong_code, 1, 5) as gu_code,
            sgg_nm as gu,
            stat_region_cd,
            revision_date
        from {{ source('axes_bronze', 'admin_dong_master') }}
        where sido_nm = '서울특별시'
          and revision_date = (select revision_date from latest)
          and substr(admin_dong_code, 6, 5) <> '00000'
    ),
    crosswalk as (
        select
            admin_dong_code,
            latitude,
            longitude
        from {{ ref('seoul_admin_dong_crosswalk') }} FOR VERSION AS OF {{ snapshot_id }}
    )
    select
        d.admin_dong_code,
        d.admin_dong,
        d.gu_code,
        d.gu,
        d.stat_region_cd,
        d.revision_date,
        x.latitude,
        x.longitude
    from dong d
    left join crosswalk x
        on d.admin_dong_code = x.admin_dong_code
)
{% endmacro %}
