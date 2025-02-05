with
haunted_house_tickets as (
    select
        ticket_id,
        customer_id,
        haunted_house_id,
        purchase_date,
        visit_date,
        ticket_type,
        ticket_price
    from {{ ref('stg_haunted_house_tickets') }}
),
customer_feedbacks as (
    select
        ticket_id,
        rating,
        comments 
    from {{ ref('stg_customer_feedbacks') }}
),

joined as (
select 
    hht.ticket_id,
	hht.customer_id,
	hht.haunted_house_id,
	hht.purchase_date,
	hht.visit_date,
	hht.ticket_type,
	hht.ticket_price,
	cf.rating,
	cf.comments
from haunted_house_tickets hht 
left join customer_feedbacks cf 
    on hht.ticket_id = cf.ticket_id
)

select * from joined