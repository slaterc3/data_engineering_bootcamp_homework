with
    staging as (
        select
            feedback_id,
            ticket_id,
            rating,
            comments
        from {{ source('bootcamp', 'raw_customer_feedbacks') }}
    )

select *
from staging