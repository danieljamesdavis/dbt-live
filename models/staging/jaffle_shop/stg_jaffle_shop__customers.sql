with customer as(
    select * from {{ source('jaffle_shop', 'customers') }}
),

transformed as (

    select 
        *,
        customer.id as customer_id,
        customer.last_name as surname,
        customer.first_name as givenname,
        customer.first_name || ' ' || customer.last_name as full_name

    from customer

)

select * from transformed