with
    staging as (
        select
            ticket_id,
            customer_id,
            haunted_house_id,
            purchase_date,
            visit_date,
            ticket_type,
            ticket_price
        from {{ source('bootcamp', 'raw_haunted_house_tickets') }}
    )

select *
from staging