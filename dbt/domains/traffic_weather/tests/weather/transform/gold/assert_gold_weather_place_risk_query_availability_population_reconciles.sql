with population as (
    select place_id, admin_dong, gu
    from {{ ref('dim_weather_place') }}
),
expected_revision as (
    select concat(
        'kma_admin_dong_grid_20260325:',
        lower(to_hex(sha256(to_utf8(concat(
            '[',
            array_join(
                array_agg(
                    json_format(cast(array[place_id, admin_dong, gu] as json))
                    order by place_id
                ),
                ','
            ),
            ']'
        )))))
    ) as source_population_revision
    from population
),
availability as (
    select place_id, source_population_revision
    from {{ ref('gold_weather_place_risk_query_availability') }}
),
violations as (
    select
        'population_set_mismatch' as violation,
        coalesce(population.place_id, availability.place_id) as evidence
    from population
    full outer join availability
      on population.place_id = availability.place_id
    where population.place_id is null or availability.place_id is null

    union all

    select 'population_revision_mismatch', cast(availability.place_id as varchar)
    from availability
    cross join expected_revision
    where availability.source_population_revision <> expected_revision.source_population_revision
       or availability.source_population_revision is null
)
select * from violations
