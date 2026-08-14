{{ config(materialized='view') }}

-- 서울 행정동 차원. canonical 코드·명칭은 bronze source(행안부 마스터)에서 온다.
-- crosswalk seed 는 좌표(중심 위경도)만 보조로 left join (원천에 좌표 없음).
-- grain: 서울 최신 개정 revision 의 행정동 단위 distinct (구/시 집계행 제외) → ~426행.

with latest as (
    select max(revision_date) as revision_date
    from {{ source('axes_bronze', 'admin_dong_master') }}
),

dong as (
    select distinct
        admin_dong_code,
        admin_dong_nm as admin_dong,               -- 공통축 표기
        substr(admin_dong_code, 1, 5) as gu_code,   -- 행안부 앞 5자리(자치구)
        sgg_nm as gu,
        stat_region_cd,
        revision_date
    from {{ source('axes_bronze', 'admin_dong_master') }}
    where sido_nm = '서울특별시'
      and revision_date = (select revision_date from latest)
      and substr(admin_dong_code, 6, 5) <> '00000'  -- 자치구/시 집계행 제외 → 행정동 grain
),

crosswalk as (
    select
        admin_dong_code,
        latitude,
        longitude
    from {{ ref('seoul_admin_dong_crosswalk') }}
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
