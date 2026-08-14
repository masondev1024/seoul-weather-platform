{# KMA getVilageFcst LONG→WIDE 재사용 피벗 (#113 값 의미계층 보존).
   입력 relation grain = (admin_dong_code, forecast_at, category) = 정본
   gold_weather_forecast_by_admin_dong 의 최신 issued 1건. (admin_dong_code,
   forecast_at) 그룹당 category별로 정확히 1행이므로 max(...) filter 가 곧 그 값.
   수치형=value_num, PTY/SKY=qualitative_code, PCP/SNO=raw+representation+num+bounds
   (범위·explicit_none 손실 금지). group by (admin_dong_code, forecast_at) 문맥에서 호출. #}
{% macro weather_wide_pivot() -%}
    max(value_num) filter (where category = 'TMP') as temp_c,
    max(value_num) filter (where category = 'REH') as humidity_pct,
    max(value_num) filter (where category = 'WSD') as wind_ms,
    max(value_num) filter (where category = 'VEC') as wind_dir_deg,
    max(value_num) filter (where category = 'POP') as precip_prob_pct,
    max(qualitative_code) filter (where category = 'SKY') as sky_code,
    max(qualitative_code) filter (where category = 'PTY') as pty_code,
    max(fcst_value_raw) filter (where category = 'PCP') as pcp_raw,
    max(value_representation) filter (where category = 'PCP') as pcp_representation,
    max(value_num) filter (where category = 'PCP') as pcp_mm,
    max(value_lower_bound) filter (where category = 'PCP') as pcp_lower_mm,
    max(value_upper_bound) filter (where category = 'PCP') as pcp_upper_mm,
    max(fcst_value_raw) filter (where category = 'SNO') as sno_raw,
    max(value_representation) filter (where category = 'SNO') as sno_representation,
    max(value_num) filter (where category = 'SNO') as sno_cm
{%- endmacro %}

{# 서빙 라벨(RapidAPI/AI 카탈로그). KMA SKY: 1맑음/3구름많음/4흐림. #}
{% macro weather_sky_label(sky_col) -%}
    case {{ sky_col }}
        when '1' then '맑음'
        when '3' then '구름많음'
        when '4' then '흐림'
        else null
    end
{%- endmacro %}

{# KMA PTY: 0없음/1비/2비눈/3눈/4소나기/5빗방울/6빗방울눈날림/7눈날림. #}
{% macro weather_pty_label(pty_col) -%}
    case {{ pty_col }}
        when '0' then '없음'
        when '1' then '비'
        when '2' then '비/눈'
        when '3' then '눈'
        when '4' then '소나기'
        when '5' then '빗방울'
        when '6' then '빗방울눈날림'
        when '7' then '눈날림'
        else null
    end
{%- endmacro %}
