with
    staging as (
        select
            haunted_house_id,
            house_name,
            park_area,
            theme,
            fear_level,
            house_size
        from {{ source('bootcamp', 'raw_haunted_houses') }}
    )

select *
from staging