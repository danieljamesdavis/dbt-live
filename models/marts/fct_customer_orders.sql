-- Import CTEs - Handled in referenced staging files, which are drawing from source
-- file
with orders as (
    select * from {{ ref("int_orders") }}
    ),

customers as (
    select * from {{ ref("stg_jaffle_shop__customers") }}
    ),

    -- Logical CTEs - Performing joins, having moved all filtering to intermediate
    -- model
customer_orders as (

    select

            *,
            customers.full_name,
            customers.surname,
            customers.givenname,

            min(orders.order_date) over (
                partition by orders.customer_id
            ) as customer_first_order_date,

            min(orders.valid_order_date) over (
                partition by orders.customer_id
            ) as customer_first_non_returned_order_date,

            max(orders.valid_order_date) over (
                partition by orders.customer_id
            ) as customer_most_recent_non_returned_order_date,

            count(*) over (partition by orders.customer_id) as customer_order_count,

            sum(
                (
                    select case when orders.valid_order_date is null then 1 else 0 end
                    from orders
                )
            ) as customer_non_returned_order_count,

            sum(
                (
                    select
                        case
                            when orders.valid_order_date is null
                            then order_value_dollars
                            else 0
                        end
                    from orders
                )
            ) as customer_total_lifetime_value,

            array_agg(distinct orders.order_id) over (
                partition by orders.customer_id
            ) as customer_order_ids

        from orders
        inner join customers on orders.customer_id = customers.customer_id
        group by orders.order_id, orders.customer_id, 
        orders.order_date, orders.order_status, orders.valid_order_date,
        orders.user_order_seq, orders.payment_status, orders.order_value_dollars,
        customers.id, customers.customer_id, customers.first_name, customers.last_name, 
        customers.full_name, customers.surname, customers.givenname

    ),

    add_avg_order_values as (
        select
            *,
            customer_total_lifetime_value
            / customer_non_returned_order_count as customer_avg_non_returned_order_value

        from customer_orders
    ),

    -- Final CTEs 
    final as (

        select

            order_id,
            customer_id,
            surname,
            givenname,
            customer_first_order_date as first_order_date,
            customer_order_count as order_count,
            customer_total_lifetime_value as total_lifetime_value,
            order_value_dollars,
            order_status,
            payment_status

        from add_avg_order_values

    )

-- Simple Select Statement
select * from final
