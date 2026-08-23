{{ config(
    materialized='view',
    tags=['ask_seoul_weather_quality_published']
) }}

with successful_runs as (
    select
        cast(evaluation_run_id as varchar) as evaluation_run_id,
        cast(evaluation_as_of as timestamp(6)) as evaluation_as_of,
        cast(published_at as timestamp(6)) as published_at,
        cast(window_start_date as date) as window_start_date,
        cast(window_end_date as date) as window_end_date
    from {{ source('weather_quality_control', 'quality_publication_manifest') }}
    where cast(status as varchar) = 'SUCCESS'
),
ranked_runs as (
    select
        dates.evaluation_date_kst,
        successful_runs.evaluation_run_id,
        row_number() over (
            partition by dates.evaluation_date_kst
            order by successful_runs.evaluation_as_of desc,
                     successful_runs.published_at desc,
                     successful_runs.evaluation_run_id desc
        ) as publication_rank
    from successful_runs
    cross join unnest(sequence(window_start_date, window_end_date)) as dates(evaluation_date_kst)
)

select history.*
from {{ ref('gold_weather_forecast_quality_hourly_history') }} as history
inner join ranked_runs
    on history.evaluation_run_id = ranked_runs.evaluation_run_id
   and history.evaluation_date_kst = ranked_runs.evaluation_date_kst
   and ranked_runs.publication_rank = 1
