-- 모든 fcst_value 가 알려진 표현 클래스로 분류돼야 한다 (#113).
-- unparseable 발생 = KMA 표현 drift 신호 — 조용히 통과시키지 않고 실패로 알린다.
select
    category,
    fcst_value_raw,
    value_representation,
    count(*) as row_count
from {{ ref('silver_kma_vilage_fcst') }}
where value_representation = 'unparseable'
group by 1, 2, 3
