with order_items as (
    select * from {{ ref('silver_order_items') }}
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

dim_product as (
    select product_key, product_id
    from {{ ref('dim_product') }}
),

dim_seller as (
    select seller_key, seller_id
    from {{ ref('dim_seller') }}
),

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
)

select
    row_number() over (order by oi.order_id, oi.order_item_id) as order_item_key,
    oi.order_id,
    dd.date_key,
    dc.customer_key,
    dp.product_key,
    ds.seller_key,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) as total_value
from order_items oi
inner join orders o on oi.order_id = o.order_id
left join dim_customer dc on o.customer_id = dc.customer_id
left join dim_product dp on oi.product_id = dp.product_id
left join dim_seller ds on oi.seller_id = ds.seller_id
left join dim_date dd on to_date(o.order_purchase_timestamp) = dd.full_date
