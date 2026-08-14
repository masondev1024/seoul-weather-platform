{#
  space_axes.sql — asac_axes 공용 공간축 매크로 (issue #48)

  좌표 정규화·투영 역변환·행정동 조인을 표준화한다. 모든 좌표 출력 매크로는
  동일 계약을 지킨다: `<lon> as longitude, <lat> as latitude` (서울범위 가드 포함,
  범위 밖/파싱실패는 NULL). 호출부는 뒤에 콤마만 붙이면 된다.

  - seoul_lonlat(lon_raw, lat_raw)  : WGS84 십진도 좌표 정규화 (culture 매크로 승격)
  - tm_to_wgs84(x_col, y_col)       : 중부원점 TM → WGS84 근사 역변환
  - admin_dong_contains(wkt, lon, lat) : 행정동 point-in-polygon 조인용 술어

  서울범위 가드: lon 126.6~127.3 / lat 37.3~37.75.
#}

{#
  seoul_lonlat — WGS84 십진도 가정. 데이터셋별 경도/위도 필드가 제각각이라
  호출부에서 lon_raw·lat_raw 를 명시 매핑해 넘긴다.
  domains/culture/macros/seoul_lonlat.sql 을 그대로 승격(원본은 culture 자체 일정으로 전환).
#}
{% macro seoul_lonlat(lon_raw, lat_raw) %}
    case when try(cast({{ lon_raw }} as double)) between 126.6 and 127.3
         then try(cast({{ lon_raw }} as double)) end as longitude,
    case when try(cast({{ lat_raw }} as double)) between 37.3 and 37.75
         then try(cast({{ lat_raw }} as double)) end as latitude
{% endmacro %}

{#
  tm_to_wgs84 — 중부원점 TM(EPSG:5186 계열) → WGS84 근사 역변환.
    중앙자오선 lon0=127°, 원점위도 lat0=38°, k0=1.0,
    false easting=200000, false northing=500000, GRS80(a=6378137, 1/f=298.257222101).
  역 Transverse Mercator 급수 전개(footprint latitude 방식, Snyder). 상수는
  scripts/build_crosswalk.py 와 같은 GRS80 파라미터로 사전 계산해 리터럴로 박았다:
    aA0 = a*(1 - e2/4 - 3e2^2/64 - 5e2^3/256)          = 6367449.1459084488
    M0  = 원점위도 38° 자오선호장                          = 4207498.0191503242
    e2  = 2f - f^2                                         = 0.0066943800229007869
    e'2 = e2/(1-e2)                                        = 0.0067394967754789573
    c1..c4 = footprint 위도 급수 계수(e1 다항)
    lon0_rad = radians(127)                               = 2.2165681500327987
  ※ 근사 변환 — 서울 범위에서 위경도 0.001°(~100m) 이내. 정밀 측지 용도가 아니라
    행정동 할당 용도. 서울범위 밖/비수치 입력은 NULL.
#}
{% macro tm_to_wgs84(x_col, y_col) %}
    {#- 좌표를 원점 기준 오프셋으로 (double 캐스트 실패 시 NULL 전파) -#}
    {%- set X   = "(try(cast(" ~ x_col ~ " as double)) - 200000.0)" -%}
    {%- set Y   = "(try(cast(" ~ y_col ~ " as double)) - 500000.0)" -%}
    {#- mu, footprint latitude fp (k0=1) -#}
    {%- set mu  = "((4207498.0191503242 + " ~ Y ~ ") / 6367449.1459084488)" -%}
    {%- set fp  = "(" ~ mu ~ " + 0.0025188265967581876*sin(2*" ~ mu ~ ")"
                  ~ " + 3.7009490719640127e-06*sin(4*" ~ mu ~ ")"
                  ~ " + 7.4478138772111321e-09*sin(6*" ~ mu ~ ")"
                  ~ " + 1.7035993573185927e-11*sin(8*" ~ mu ~ "))" -%}
    {%- set sinfp = "sin(" ~ fp ~ ")" -%}
    {%- set cosfp = "cos(" ~ fp ~ ")" -%}
    {%- set tanfp = "tan(" ~ fp ~ ")" -%}
    {%- set W   = "(1 - 0.0066943800229007869*" ~ sinfp ~ "*" ~ sinfp ~ ")" -%}
    {%- set N1  = "(6378137.0 / sqrt(" ~ W ~ "))" -%}
    {%- set R1  = "(6378137.0*(1 - 0.0066943800229007869) / power(" ~ W ~ ", 1.5))" -%}
    {%- set T1  = "(" ~ tanfp ~ "*" ~ tanfp ~ ")" -%}
    {%- set C1  = "(0.0067394967754789573*" ~ cosfp ~ "*" ~ cosfp ~ ")" -%}
    {%- set D   = "(" ~ X ~ " / " ~ N1 ~ ")" -%}
    {#- 위도(도): fp - (N1 tan/R1)(D^2/2 - (...)D^4/24 + (...)D^6/720) -#}
    {%- set lat_rad = "(" ~ fp ~ " - (" ~ N1 ~ "*" ~ tanfp ~ "/" ~ R1 ~ ")*("
                      ~ "power(" ~ D ~ ",2)/2"
                      ~ " - (5 + 3*" ~ T1 ~ " + 10*" ~ C1 ~ " - 4*" ~ C1 ~ "*" ~ C1 ~ " - 9*0.0067394967754789573)*power(" ~ D ~ ",4)/24"
                      ~ " + (61 + 90*" ~ T1 ~ " + 298*" ~ C1 ~ " + 45*" ~ T1 ~ "*" ~ T1 ~ " - 252*0.0067394967754789573 - 3*" ~ C1 ~ "*" ~ C1 ~ ")*power(" ~ D ~ ",6)/720"
                      ~ "))" -%}
    {#- 경도(도): lon0 + (D - (1+2T1+C1)D^3/6 + (...)D^5/120)/cos -#}
    {%- set lon_rad = "(2.2165681500327987 + ("
                      ~ D
                      ~ " - (1 + 2*" ~ T1 ~ " + " ~ C1 ~ ")*power(" ~ D ~ ",3)/6"
                      ~ " + (5 - 2*" ~ C1 ~ " + 28*" ~ T1 ~ " - 3*" ~ C1 ~ "*" ~ C1 ~ " + 8*0.0067394967754789573 + 24*" ~ T1 ~ "*" ~ T1 ~ ")*power(" ~ D ~ ",5)/120"
                      ~ ")/" ~ cosfp ~ ")" -%}
    {%- set lon_deg = "degrees(" ~ lon_rad ~ ")" -%}
    {%- set lat_deg = "degrees(" ~ lat_rad ~ ")" -%}
    case when {{ lon_deg }} between 126.6 and 127.3
         then {{ lon_deg }} end as longitude,
    case when {{ lat_deg }} between 37.3 and 37.75
         then {{ lat_deg }} end as latitude
{% endmacro %}

{#
  tm_to_wgs84_relation — tm_to_wgs84 와 같은 수식/상수의 **레이어드 변형** (2026-07-07 traffic 장애 대응).

  tm_to_wgs84 는 중간항(mu, fp, sin/cos/tan, W, N1, R1, T1, C1, D)을 Jinja 문자열 치환으로
  중첩 전개하기 때문에 컴파일된 표현식이 지수적으로 커진다(traffic silver 기준 컴파일 SQL 73KB).
  Trino 는 프로젝션 하나를 단일 메서드로 코드젠하므로 이 크기에서
  QUERY_EXCEEDED_COMPILER_LIMIT 로 실패한다. 이 매크로는 각 중간항을 서브쿼리 레이어의
  컬럼(심볼)으로 **한 번씩만** 계산한다 — Trino 옵티마이저는 복수 참조되는 비자명 심볼을
  재인라인하지 않으므로 표현식 크기가 선형으로 유지된다.

  계약: `select`(FROM 절 전체) 블록을 돌려준다. 출력 = `{{ relation }}.*`
  + `longitude`/`latitude`(서울범위 가드·NULL 전파는 tm_to_wgs84 와 동일)
  + `__tm_*` 스크래치 컬럼(중간항 — 최종 select 에서 명시 컬럼만 뽑으면 노출되지 않음).

      located as (
          {{ asac_axes.tm_to_wgs84_relation('standardized', 'grs80tm_x', 'grs80tm_y') }}
      )
#}
{% macro tm_to_wgs84_relation(relation, x_col, y_col) %}
select
    t8.*,
    case when t8.__tm_lon_deg between 126.6 and 127.3 then t8.__tm_lon_deg end as longitude,
    case when t8.__tm_lat_deg between 37.3 and 37.75 then t8.__tm_lat_deg end as latitude
from (
    select
        t7.*,
        degrees(2.2165681500327987 + (
            __tm_d
            - (1 + 2*__tm_t1 + __tm_c1)*power(__tm_d, 3)/6
            + (5 - 2*__tm_c1 + 28*__tm_t1 - 3*__tm_c1*__tm_c1 + 8*0.0067394967754789573 + 24*__tm_t1*__tm_t1)*power(__tm_d, 5)/120
        )/__tm_cosfp) as __tm_lon_deg,
        degrees(__tm_fp - (__tm_n1*__tm_tanfp/__tm_r1)*(
            power(__tm_d, 2)/2
            - (5 + 3*__tm_t1 + 10*__tm_c1 - 4*__tm_c1*__tm_c1 - 9*0.0067394967754789573)*power(__tm_d, 4)/24
            + (61 + 90*__tm_t1 + 298*__tm_c1 + 45*__tm_t1*__tm_t1 - 252*0.0067394967754789573 - 3*__tm_c1*__tm_c1)*power(__tm_d, 6)/720
        )) as __tm_lat_deg
    from (
        select t6.*, (__tm_x / __tm_n1) as __tm_d
        from (
            select
                t5.*,
                (6378137.0 / sqrt(__tm_w)) as __tm_n1,
                (6378137.0*(1 - 0.0066943800229007869) / power(__tm_w, 1.5)) as __tm_r1
            from (
                select
                    t4.*,
                    (1 - 0.0066943800229007869*__tm_sinfp*__tm_sinfp) as __tm_w,
                    (__tm_tanfp*__tm_tanfp) as __tm_t1,
                    (0.0067394967754789573*__tm_cosfp*__tm_cosfp) as __tm_c1
                from (
                    select
                        t3.*,
                        sin(__tm_fp) as __tm_sinfp,
                        cos(__tm_fp) as __tm_cosfp,
                        tan(__tm_fp) as __tm_tanfp
                    from (
                        select
                            t2.*,
                            (__tm_mu
                             + 0.0025188265967581876*sin(2*__tm_mu)
                             + 3.7009490719640127e-06*sin(4*__tm_mu)
                             + 7.4478138772111321e-09*sin(6*__tm_mu)
                             + 1.7035993573185927e-11*sin(8*__tm_mu)) as __tm_fp
                        from (
                            select t1.*, ((4207498.0191503242 + __tm_y) / 6367449.1459084488) as __tm_mu
                            from (
                                select
                                    base.*,
                                    (try(cast({{ x_col }} as double)) - 200000.0) as __tm_x,
                                    (try(cast({{ y_col }} as double)) - 500000.0) as __tm_y
                                from {{ relation }} as base
                            ) as t1
                        ) as t2
                    ) as t3
                ) as t4
            ) as t5
        ) as t6
    ) as t7
) as t8
{% endmacro %}

{#
  admin_dong_contains — 행정동 경계 seed 와의 point-in-polygon 술어 한 조각.
  seoul_admin_dong_boundary seed 의 boundary_wkt 컬럼과 좌표를 받아
  ST_Contains(polygon, point) 불리언을 돌려준다. 조인 ON 절/서브쿼리에서 사용.
  (전체 CTE 조인 패턴은 README '행정동 할당 표준 패턴' 참고 — 매크로는 술어만 제공.)
#}
{% macro admin_dong_contains(boundary_wkt_col, lon_expr, lat_expr) -%}
ST_Contains(ST_GeometryFromText({{ boundary_wkt_col }}), ST_Point({{ lon_expr }}, {{ lat_expr }}))
{%- endmacro %}
