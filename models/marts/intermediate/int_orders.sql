with orders as (

    select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments') }}
    where status != 'fail'
),

order_totals as (

    select 

        payments.orderid as order_id,
        payments.status as payment_status,
        sum(round(amount/100.0, 2)) as order_value_dollars

    from payments
    group by 1, 2
),

order_values_joined as (

    select 

        orders.*,
        order_totals.payment_status,
        order_totals.order_value_dollars,

    from orders
    join order_totals on order_totals.order_id = orders.order_id
)

select * from order_values_joined