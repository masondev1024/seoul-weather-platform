{#
  axis_tests.sql — asac_axes 공용 제네릭 테스트 (issue #48)

  dbt generic test 2종. YAML 에서는 test 접두어를 뺀 이름으로 참조한다
  (블록명 in_seoul_bbox → 컴파일 매크로 test_in_seoul_bbox).

    columns:
      - name: longitude
        tests:
          - in_seoul_bbox: {kind: lon}
          - axis_coverage: {min_ratio: 0.98}
#}

{#
  in_seoul_bbox — 좌표 컬럼이 서울 bbox 안인지. kind='lon'|'lat'.
  NULL 은 허용(커버리지는 axis_coverage 로 별도 게이트). 범위 밖 행을 반환 → 실패.
    lon: 126.6~127.3 / lat: 37.3~37.75
#}
{% test in_seoul_bbox(model, column_name, kind) %}
    {%- set bounds = {'lon': [126.6, 127.3], 'lat': [37.3, 37.75]} -%}
    {%- if kind not in bounds -%}
        {{ exceptions.raise_compiler_error("in_seoul_bbox: kind must be 'lon' or 'lat', got " ~ kind) }}
    {%- endif -%}
    {%- set lo = bounds[kind][0] -%}
    {%- set hi = bounds[kind][1] -%}
    select {{ column_name }} as axis_value
    from {{ model }}
    where {{ column_name }} is not null
      and ({{ column_name }} < {{ lo }} or {{ column_name }} > {{ hi }})
{% endtest %}

{#
  axis_coverage — non-null 비율이 min_ratio 미만이면 실패.
  행정동 좌표/코드 커버리지·정확도 게이트(kang 제안). 빈 테이블(total=0)도 실패로 본다.
#}
{% test axis_coverage(model, column_name, min_ratio) %}
    with m as (
        select
            count(*) as total_rows,
            count({{ column_name }}) as non_null_rows
        from {{ model }}
    )
    select
        total_rows,
        non_null_rows,
        case when total_rows = 0 then 0.0
             else non_null_rows * 1.0 / total_rows end as coverage_ratio
    from m
    where total_rows = 0
       or (non_null_rows * 1.0 / total_rows) < {{ min_ratio }}
{% endtest %}
