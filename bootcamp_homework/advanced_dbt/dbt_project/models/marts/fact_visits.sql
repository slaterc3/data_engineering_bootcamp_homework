{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='ticket_id'
) }}

with

    audit_table as (
        select
            ticket_id
            , customer_id
            , haunted_house_id
            , purchase_date
            , visit_date
            , ticket_type
            , ticket_price
            , rating
            , comments
        from {{ ref('audit_fact_visits') }}
    )

select *
from audit_table

