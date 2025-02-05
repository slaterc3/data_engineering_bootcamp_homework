with customers as (
    select 
        customer_id,
        age,
        gender,
        email
    from {{ ref('stg_haunted_customers') }}
),
valid_domains as (
    select
        valid_domain
    from {{ ref('valid_domains') }}
),
final as (
    select 
        c.customer_id,
        c.age,
        c.gender,
        c.email,
        case 
            when email like '%@%.%'
            and split_part(c.email, '@', 2) in (select valid_domain from valid_domains)
            then true else false 
        end as is_valid_email_address
    from customers c 
)
select * from final