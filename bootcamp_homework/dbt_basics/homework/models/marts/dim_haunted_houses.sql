with
haunted_houses as (
    select
        haunted_house_id,
        house_name,
        park_area,
        theme,
        fear_level,
        house_size as house_size_in_ft2,
        {{ feet_to_meters('house_size') }} as house_size_in_m2
    from {{ ref('stg_haunted_houses') }}
)
select 
    *
from haunted_houses