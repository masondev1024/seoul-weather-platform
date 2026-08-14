{#
  time_axes.sql — asac_axes 공용 시간축 매크로 (issue #48)

  서울시 원천 데이터의 제각각인 시각 표기를 KST timestamp(6) 하나로 모은다.
  전 도메인이 이 매크로만 호출하면 silver 의 시간축 계약이 통일된다.

  - kst_at(expr)            : 단일 문자열 시각 → 포맷 자동판별 KST timestamp(6)
  - kst_at_from_parts(d,t)  : (yyyyMMdd, HHMM|HHMMSS) 2컬럼 조합 → KST timestamp(6)
  - utc_to_kst(ts)          : UTC timestamp → +9h KST (bronze 수집시각의 silver 노출용)

  파싱 실패는 모두 NULL (try + regexp_like 가드). Trino(Iceberg) 어댑터 기준.
  traffic 의 topis_timestamp 매크로 case 스타일을 계승·확장한다.
#}

{#
  kst_at — 문자열 시각을 KST timestamp(6) 으로. 아래 3형식을 판별:
    1) 'yyyy-MM-dd HH:mm:ss'  (초 뒤 소수부 '.f+' 선택)  예: '2026-07-03 17:50:59', '2026-06-02 15:00:04.1127'
    2) 'yyyy-MM-dd HH:mm'     (초 생략)                   예: '2026-07-01 18:40'
    3) 'yyyyMMddHHmmss' 14자리 숫자                        예: '20260706132016'
  하이픈 형식(1,2)은 Trino 의 varchar→timestamp 캐스트가 소수부/초생략까지 흡수하므로
  try(cast(... as timestamp(6))) 로 처리하고, 14자리 숫자만 date_parse 를 쓴다.
  형식 불일치·비수치는 else 로 빠져 NULL.
#}
{% macro kst_at(expr) -%}
case
    when regexp_like(cast({{ expr }} as varchar), '^[0-9]{14}$')
        then try(cast(date_parse(cast({{ expr }} as varchar), '%Y%m%d%H%i%s') as timestamp(6)))
    when regexp_like(cast({{ expr }} as varchar), '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]+)?)?$')
        then try(cast(cast({{ expr }} as varchar) as timestamp(6)))
    else null
end
{%- endmacro %}

{#
  kst_at_from_parts — 날짜/시각 2컬럼 조합. topis(교통)·kma(날씨) 두 도메인 매크로의 상위 호환:
    - date_col: 'yyyyMMdd' 8자리
    - time_col: 'HHMM' 4자리(kma·topis) 또는 'HHMMSS' 6자리(topis)
  4자리는 초 '00' 을 붙여 date_parse. 형식 불일치는 NULL.
#}
{% macro kst_at_from_parts(date_col, time_col) -%}
case
    when regexp_like(cast({{ date_col }} as varchar), '^[0-9]{8}$')
         and regexp_like(cast({{ time_col }} as varchar), '^[0-9]{4}$')
        then try(cast(date_parse(concat(cast({{ date_col }} as varchar), cast({{ time_col }} as varchar), '00'), '%Y%m%d%H%i%s') as timestamp(6)))
    when regexp_like(cast({{ date_col }} as varchar), '^[0-9]{8}$')
         and regexp_like(cast({{ time_col }} as varchar), '^[0-9]{6}$')
        then try(cast(date_parse(concat(cast({{ date_col }} as varchar), cast({{ time_col }} as varchar)), '%Y%m%d%H%i%s') as timestamp(6)))
    else null
end
{%- endmacro %}

{#
  utc_to_kst — UTC timestamp 를 KST(+9h) 로. bronze 의 ingested_at(UTC) 을 silver 에서 KST 로 노출할 때.
  입력은 timestamp 타입 가정. 문자열이면 kst_at 로 먼저 파싱할 것.
#}
{% macro utc_to_kst(ts_expr) -%}
({{ ts_expr }} + interval '9' hour)
{%- endmacro %}
