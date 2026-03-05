-- Kiểm tra: silver_order_items không còn key columns bị NULL

select 'silver_order_items' as source_table, order_id, 'order_id is null' as issue
from {{ ref('silver_order_items') }} where order_id is null

union all

select 'silver_order_items', order_id, 'product_id is null'
from {{ ref('silver_order_items') }} where product_id is null

union all

select 'silver_order_items', order_id, 'seller_id is null'
from {{ ref('silver_order_items') }} where seller_id is null
