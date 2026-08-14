with ordered_windows as (
    select
        product_row_id,
        place_id,
        window_start_at,
        window_end_at,
        precipitation_hour_count,
        lag(window_end_at) over (
            partition by place_id
            order by window_start_at, window_end_at, product_row_id
        ) as previous_window_end_at
    from {{ ref('gold_weather_place_precipitation_window') }}
)

select *
from ordered_windows
where window_start_at > window_end_at
   or precipitation_hour_count <> date_diff('hour', window_start_at, window_end_at) + 1
   or (
       previous_window_end_at is not null
       and window_start_at <= date_add('hour', 1, previous_window_end_at)
   )
