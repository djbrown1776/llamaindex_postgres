with staged as (
    select * from {{ ref('stg_ucl_players') }}
)

select
    id,
    display_name,
    CONCAT(
        display_name, ' is a professional footballer ',
        'born on ', date_of_birth, ', aged ', age, ' years old, ',
        'with citizenship: ', citizenship, '. ',
        'They are ', height, ' tall and weigh ', weight, '.'
    ) as embedding_text
from staged