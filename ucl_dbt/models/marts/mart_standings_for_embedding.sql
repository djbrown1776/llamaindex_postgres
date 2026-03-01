with staged as (
    select * from {{ ref('stg_ucl_standings') }}
)

select
    team_id,
    season,
    team_name,
    CONCAT(
        team_name, ' finished rank ', rank, ' in group ', group_name, ' ',
        'during the ', season, ' UCL season. ',
        'They played ', games_played, ' games, winning ', wins, ', ',
        'drawing ', draws, ', and losing ', losses, ', ',
        'accumulating ', points, ' points. ',
        'Goals for: ', goals_for, ', goals against: ', goals_against, ', ',
        'goal differential: ', goal_differential, '.'
    ) as embedding_text
from staged