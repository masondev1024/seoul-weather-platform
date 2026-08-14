-- PTY(강수형태) ↔ PCP(강수량) 논리 정합 (#113, warn):
--   같은 grid·발표·예보시각에서 PTY=0(강수 없음)인데 PCP 가 양의 정량값이거나,
--   PTY 가 강수 코드(1/2/4)인데 PCP 가 explicit_none 이면 소스 모순.
--   소스 원본의 특성일 수 있어 차단하지 않고 warn 으로만 감시한다.
{{ config(severity='warn') }}

with pty as (
    select nx, ny, issued_at, forecast_at, qualitative_code as pty_code
    from {{ ref('silver_kma_vilage_fcst') }}
    where category = 'PTY'
),

pcp as (
    select nx, ny, issued_at, forecast_at, value_representation, value_num
    from {{ ref('silver_kma_vilage_fcst') }}
    where category = 'PCP'
)

select
    pty.nx,
    pty.ny,
    pty.issued_at,
    pty.forecast_at,
    pty.pty_code,
    pcp.value_representation as pcp_representation,
    pcp.value_num as pcp_value_num
from pty
inner join pcp
    on pty.nx = pcp.nx
   and pty.ny = pcp.ny
   and pty.issued_at = pcp.issued_at
   and pty.forecast_at = pcp.forecast_at
where (pty.pty_code = '0' and pcp.value_num > 0)
   or (pty.pty_code in ('1', '2', '4') and pcp.value_representation = 'explicit_none')
