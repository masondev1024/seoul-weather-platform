{#
  kma_value_semantics — KMA getVilageFcst fcstValue 의 category-aware 의미 분류 (#113).

  배경(2026-07-10 감사): fcst_value 는 카테고리·예보구간에 따라 표현이 다르다.
    - PCP/SNO 근구간: '강수없음'/'적설없음', '1mm 미만', '2.0mm', '30.0~50.0mm', '50.0mm 이상'
    - PCP/SNO 원구간(lead>=50h 실측): '0', '1', '0.2' 같은 단위 없는 맨몸 숫자
    - PTY/SKY: 정수 코드값. 나머지 카테고리: 정량 숫자.
  단일 try_cast 는 근구간 정량('2.0mm')을 NULL 로 죽이고 '강수없음'(명시적 없음)과
  구분하지 못한다 — 이 매크로는 표현 사실만 분류하고 의미를 임의 추정하지 않는다.
  (연장 구간 맨몸값의 공식 의미는 검증 전이므로 bare_numeric 으로 정직하게 표기)

  emit 컬럼: value_representation / value_num / value_lower_bound / value_upper_bound
             / qualitative_code
    - value_representation: explicit_none | quantitative_exact | quantitative_range
                            | bare_numeric | qualitative_code | missing | unparseable
    - value_num: 정량 확정치만(단위 제거). bare_numeric 은 숫자값을 채우되
                 representation 으로 구분 가능하게 남긴다. explicit_none/range 는 NULL.
    - value_lower_bound/value_upper_bound: 범위 표현만. '이상' 은 상한 NULL(무상한 보존),
      '미만' 은 하한 0. value_num 과 동시에 채워지지 않는다.
    - qualitative_code: PTY/SKY 원본 코드 문자열.
#}
{% macro kma_value_semantics(category_col, raw_col) -%}
{%- set v = "trim(cast(" ~ raw_col ~ " as varchar))" -%}
    case
        when {{ raw_col }} is null or {{ v }} = '' or {{ v }} = '-' then 'missing'
        when {{ category_col }} in ('PTY', 'SKY') then
            case
                when regexp_like({{ v }}, '^[0-9]+$') then 'qualitative_code'
                else 'unparseable'
            end
        when {{ category_col }} in ('PCP', 'SNO') then
            case
                when {{ v }} in ('강수없음', '강수 없음', '적설없음', '적설 없음', '눈날림') then 'explicit_none'
                when regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?(mm|cm) ?미만$') then 'quantitative_range'
                when regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?(mm|cm) ?이상$') then 'quantitative_range'
                when regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?~[0-9]+(\.[0-9]+)?(mm|cm)$') then 'quantitative_range'
                when regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?(mm|cm)$') then 'quantitative_exact'
                when regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?$') then 'bare_numeric'
                else 'unparseable'
            end
        else
            case
                when try_cast({{ v }} as double) is not null then 'quantitative_exact'
                else 'unparseable'
            end
    end as value_representation,
    case
        when {{ raw_col }} is null or {{ v }} in ('', '-') then cast(null as double)
        when {{ category_col }} in ('PTY', 'SKY') then cast(null as double)
        when {{ category_col }} in ('PCP', 'SNO') then
            case
                when regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?(mm|cm)$')
                    then try_cast(regexp_extract({{ v }}, '^([0-9]+(\.[0-9]+)?)', 1) as double)
                when regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?$')
                    then try_cast({{ v }} as double)
                else cast(null as double)
            end
        else try_cast({{ v }} as double)
    end as value_num,
    case
        when {{ category_col }} in ('PCP', 'SNO')
             and regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?(mm|cm) ?미만$')
            then cast(0.0 as double)
        when {{ category_col }} in ('PCP', 'SNO')
             and regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?(mm|cm) ?이상$')
            then try_cast(regexp_extract({{ v }}, '^([0-9]+(\.[0-9]+)?)', 1) as double)
        when {{ category_col }} in ('PCP', 'SNO')
             and regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?~[0-9]+(\.[0-9]+)?(mm|cm)$')
            then try_cast(regexp_extract({{ v }}, '^([0-9]+(\.[0-9]+)?)~', 1) as double)
        else cast(null as double)
    end as value_lower_bound,
    case
        when {{ category_col }} in ('PCP', 'SNO')
             and regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?(mm|cm) ?미만$')
            then try_cast(regexp_extract({{ v }}, '^([0-9]+(\.[0-9]+)?)', 1) as double)
        when {{ category_col }} in ('PCP', 'SNO')
             and regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?(mm|cm) ?이상$')
            then cast(null as double)
        when {{ category_col }} in ('PCP', 'SNO')
             and regexp_like({{ v }}, '^[0-9]+(\.[0-9]+)?~[0-9]+(\.[0-9]+)?(mm|cm)$')
            then try_cast(regexp_extract({{ v }}, '~([0-9]+(\.[0-9]+)?)', 1) as double)
        else cast(null as double)
    end as value_upper_bound,
    case
        when {{ category_col }} in ('PTY', 'SKY') and regexp_like({{ v }}, '^[0-9]+$')
            then {{ v }}
        else cast(null as varchar)
    end as qualitative_code
{%- endmacro %}
