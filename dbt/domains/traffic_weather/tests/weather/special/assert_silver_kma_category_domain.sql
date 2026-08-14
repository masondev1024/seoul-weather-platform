-- category drift 감시 (#113, warn): getVilageFcst 공지 카테고리 14종 밖의 값이
-- 들어오면 알린다. bronze/silver 는 신규 카테고리를 버리지 않고 보존하므로
-- (원형 보존 계약) 차단이 아니라 warn 으로 감지만 한다.
{{ config(severity='warn') }}

select
    category,
    count(*) as row_count,
    min(load_date) as first_load_date,
    max(load_date) as last_load_date
from {{ ref('silver_kma_vilage_fcst') }}
where category not in (
    'TMP', 'POP', 'PTY', 'PCP', 'REH', 'SNO', 'SKY',
    'TMN', 'TMX', 'UUU', 'VVV', 'VEC', 'WSD', 'WAV'
)
group by 1
