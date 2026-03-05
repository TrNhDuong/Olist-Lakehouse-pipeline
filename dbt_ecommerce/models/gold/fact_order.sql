with orders as (
    select * from {{ ref('silver_orders') }}
),

order_items_agg as (
    select
        order_id,
        sum(price) as total_item_value,
        sum(freight_value) as total_freight_value,
        sum(price + freight_value) as total_order_value
    from {{ ref('silver_order_items') }}
    where flag_valid = 'valid'
    group by order_id
),

reviews as (
    select
        order_id,
        review_score
    from {{ ref('silver_reviews') }}
),

invalid_payments as (
    select
        order_id,
        max(case when flag_valid = 'invalid' then true else false end) as has_invalid_payment
    from {{ ref('silver_payments') }}
    group by order_id
),

dim_customer as (
    select customer_key, customer_id
    from {{ ref('dim_customer') }}
),

dim_order_status as (
    select order_status_key, order_status
    from {{ ref('dim_order_status') }}
),

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
)

select
    row_number() over (order by o.order_id) as order_key,
    o.order_id,
    dd.date_key,
    dc.customer_key,
    dos.order_status_key,
    coalesce(oi.total_item_value, 0) as total_item_value,
    coalesce(oi.total_freight_value, 0) as total_freight_value,
    coalesce(oi.total_order_value, 0) as total_order_value,
    datediff(o.order_delivered_customer_date, o.order_purchase_timestamp) as delivery_days,
    r.review_score,
    case when r.review_score is not null then 1 else 0 end as is_reviewed,
    coalesce(ip.has_invalid_payment, false) as has_invalid_payment
from orders o
left join order_items_agg oi on o.order_id = oi.order_id
left join reviews r on o.order_id = r.order_id
left join invalid_payments ip on o.order_id = ip.order_id
left join dim_customer dc on o.customer_id = dc.customer_id
left join dim_order_status dos on o.order_status = dos.order_status
left join dim_date dd on to_date(o.order_purchase_timestamp) = dd.full_date
