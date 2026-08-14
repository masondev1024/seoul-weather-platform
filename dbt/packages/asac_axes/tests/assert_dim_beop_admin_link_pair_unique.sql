-- dim_beop_admin_link grain (beop_dong_code, admin_dong_code) 쌍 유일성. 중복이면 실패.
-- 링크는 다대다라 단일 컬럼 unique 는 성립하지 않는다 — 쌍 단위로만 계약.
select
    beop_dong_code,
    admin_dong_code,
    count(*) as n
from {{ ref('dim_beop_admin_link') }}
group by beop_dong_code, admin_dong_code
having count(*) > 1
