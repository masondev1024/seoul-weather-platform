-- PTY/SKY 코드 도메인 (#113). 단기예보(getVilageFcst) 공식 코드:
--   PTY 0(없음)/1(비)/2(비눈)/3(눈)/4(소나기), SKY 1(맑음)/3(구름많음)/4(흐림).
-- 2026-07-10 실측: PTY {0,1,4}, SKY {1,3,4}. 도메인 밖 코드 = drift 신호.
select
    category,
    qualitative_code,
    count(*) as row_count
from {{ ref('silver_kma_vilage_fcst') }}
where (category = 'PTY' and qualitative_code not in ('0', '1', '2', '3', '4'))
   or (category = 'SKY' and qualitative_code not in ('1', '3', '4'))
   or (category in ('PTY', 'SKY') and qualitative_code is null)
group by 1, 2
