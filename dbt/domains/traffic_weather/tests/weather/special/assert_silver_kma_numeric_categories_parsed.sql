-- 수치 카테고리(TMP/POP/REH/WSD/UUU/VVV/VEC/WAV/TMN/TMX)는 전량 정량 파싱돼야 한다 (#113).
-- 2026-07-10 실측: dev 전체에서 비숫자 0건. 여기 걸리면 WSD 연장 정성코드 같은
-- 신규 표현 체제가 등장했다는 신호이므로 조용히 NULL 로 흘리지 않고 실패로 알린다.
select
    category,
    fcst_value_raw,
    value_representation,
    count(*) as row_count
from {{ ref('silver_kma_vilage_fcst') }}
where category in ('TMP', 'POP', 'REH', 'WSD', 'UUU', 'VVV', 'VEC', 'WAV', 'TMN', 'TMX')
  and (value_representation <> 'quantitative_exact' or value_num is null)
group by 1, 2, 3
