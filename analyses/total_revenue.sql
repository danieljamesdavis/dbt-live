-- with payments as (
--     select * from {{ ref('stg_payments') }}
-- )

-- select distinct(payment_method) from payments

-- select current_date