{{ config(
    materialized='view',
    tags=['ask_seoul_weather_quality_published']
) }}

with publication_manifest as (
    select
        cast(evaluation_run_id as varchar) as evaluation_run_id,
        cast(status as varchar) as publication_status,
        cast(evaluation_as_of as timestamp(6)) as evaluation_as_of,
        cast(published_at as timestamp(6)) as published_at,
        cast(window_start_date as date) as window_start_date,
        cast(window_end_date as date) as window_end_date
    from {{ source('weather_quality_control', 'quality_publication_manifest') }}
),

successful_quality_runs_expanded_to_evaluation_dates as (
    select
        publication_manifest.evaluation_run_id,
        publication_manifest.evaluation_as_of,
        publication_manifest.published_at,
        evaluation_date_kst
    from publication_manifest
    cross join unnest(sequence(window_start_date, window_end_date)) as dates(evaluation_date_kst)
    where publication_status = 'SUCCESS'
),

successful_candidates as (
    select
        evaluation_date_kst,
        evaluation_run_id,
        row_number() over (
            partition by evaluation_date_kst
            order by evaluation_as_of desc, published_at desc, evaluation_run_id desc
        ) as publication_rank
    from successful_quality_runs_expanded_to_evaluation_dates
)

select history.*
from {{ ref('gold_weather_forecast_quality_daily_history') }} as history
inner join successful_candidates as publication
    on publication.evaluation_run_id = history.evaluation_run_id
   and publication.evaluation_date_kst = history.evaluation_date_kst
   and publication.publication_rank = 1
