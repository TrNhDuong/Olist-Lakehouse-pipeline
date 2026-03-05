with bronze_orders as (
    select * 
    from {{ source('bronze', 'raw_orders') }}
)

select 
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
from bronze_orders
where 
    order_id is not null 
    and customer_id is not null
    and order_status is not null
    and order_purchase_timestamp is not null
