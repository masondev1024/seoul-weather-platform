-- PCP/SNO 파서 출력 자기일관성 (#113):
--   explicit_none/missing 은 수치가 없어야 하고, quantitative_exact/bare_numeric 은
--   value_num 이, quantitative_range 는 하한이 있어야 한다. 범위와 정량치는 동시에
--   채워지지 않는다 ('50.0mm 이상' 의 무상한은 upper NULL 로 보존).
select *
from {{ ref('silver_kma_vilage_fcst') }}
where category in ('PCP', 'SNO')
  and (
    (value_representation in ('explicit_none', 'missing')
        and (value_num is not null or value_lower_bound is not null or value_upper_bound is not null))
    or (value_representation in ('quantitative_exact', 'bare_numeric') and value_num is null)
    or (value_representation = 'quantitative_range' and value_lower_bound is null)
    or (value_representation = 'quantitative_range' and value_num is not null)
  )
