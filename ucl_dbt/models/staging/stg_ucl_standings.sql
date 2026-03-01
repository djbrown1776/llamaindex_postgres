with source as (
    select * from {{ source('ucl', 'ucl_standings') }}
),

staged as (
    select
        season,
        rank,
        rank_change,
        group_name,
        note,
        games_played,
        wins,
        losses,
        draws,
        points,
        goals_for,
        goals_against,
        goal_differential,
        points_per_game,
        team_id,
        team_name,
        team_short_name,
        team_abbreviation,
        team_location
    from source
    where team_name is not null
)

select * from staged