{{ config(materialized='view') }}

-- 법정동 ↔ 행정동 링크(서울, 최신 revision). 법정동 주소 기반 도메인(commerce 등)이
-- 행정동 공통축으로 조인해 들어오는 다리.
--
-- grain: (beop_dong_code, admin_dong_code) 쌍 — 다대다 실증(최신 revision 743쌍,
--   법정동 467 중 134개가 복수 행정동에, 행정동 426 중 91개가 복수 법정동에 걸침).
--   따라서 단일 컬럼 unique 는 성립하지 않고 쌍 유일성만 계약한다(singular 테스트).
-- 필터: dim_admin_dong 과 동일하게 행정동 집계행(끝 5자리 '00000' = 자치구/시) 제외.
--   법정동 쪽 집계행은 행정동 집계행과 정확히 일치함을 실증(서울 최신 revision 769행 중
--   26행 양쪽 모두 집계) — 행정동 필터만으로 제거되지만 방어적으로 양쪽 모두 거른다.

with latest as (
    select max(revision_date) as revision_date
    from {{ source('axes_bronze', 'admin_dong_master') }}
)

select
    beop_dong_code,
    beop_dong_nm,
    admin_dong_code,
    admin_dong_nm as admin_dong,                -- 공통축 표기(dim_admin_dong 관례)
    substr(admin_dong_code, 1, 5) as gu_code,   -- 행안부 앞 5자리(자치구)
    revision_date
from {{ source('axes_bronze', 'admin_dong_master') }}
where sido_nm = '서울특별시'
  and revision_date = (select revision_date from latest)
  and substr(admin_dong_code, 6, 5) <> '00000'
  and substr(beop_dong_code, 6, 5) <> '00000'
