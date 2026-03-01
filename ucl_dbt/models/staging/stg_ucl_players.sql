with source as (
    select * from {{ source('ucl', 'ucl_players') }}
),

staged as (
    select
        id,
        display_name,
        first_name,
        last_name,
        date_of_birth,
        CAST(age AS INT)        as age,
        height,
        weight,
        citizenship,
        team_ids
    from source
    where display_name is not null
)

select * from staged