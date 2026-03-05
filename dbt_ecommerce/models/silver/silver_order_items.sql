with bronze_order_items as (
    select * 
    from {{ source('bronze', 'raw_order_items') }}
)

select 
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    case 
        when price < 0 or freight_value < 0 then 'invalid'
        else 'valid'
    end as flag_valid
from bronze_order_items
where 
    order_id is not null 
    and product_id is not null
    and seller_id is not null
