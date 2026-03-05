with payments as (
    select * from {{ ref('silver_payments') }}
    where flag_valid = 'valid'
),

orders as (
    select order_id, customer_id, order_purchase_timestamp
    from {{ ref('silver_orders') }}
),

dim_customer as (
    select customer_key, customer_id
    from {{ ref('dim_customer') }}
),

dim_payment_type as (
    select payment_type_key, payment_type
    from {{ ref('dim_payment_type') }}
),

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
)

select
    row_number() over (order by p.order_id, p.payment_sequential) as order_payment_key,
    p.order_id,
    dd.date_key,
    dc.customer_key,
    dpt.payment_type_key,
    p.payment_sequential,
    p.payment_value,
    p.payment_installments
from payments p
inner join orders o on p.order_id = o.order_id
left join dim_customer dc on o.customer_id = dc.customer_id
left join dim_payment_type dpt on p.payment_type = dpt.payment_type
left join dim_date dd on to_date(o.order_purchase_timestamp) = dd.full_date
