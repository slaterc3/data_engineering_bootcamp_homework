{% test valid_email(model, column_name) %}

with validation as (
    select {{ column_name }} as email 
    from {{ model }}
),
validation_errors as (
    select email 
    from validation
    where email NOT LIKE '%@%.%'
        or email LIKE '%@%@%'
        or email LIKE '@%'
        or email LIKE '%@%'
        or email LIKE '%.@%'
)

select * from validation_errors

{% endtest %}