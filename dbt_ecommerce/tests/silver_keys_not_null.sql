-- Kiểm tra: silver_orders không còn dòng nào có key columns bị NULL
-- Mặc dù SQL silver đã filter, test này đảm bảo kết quả thực sự clean

select 'silver_orders' as source_table, order_id, 'order_id is null' as issue
from {{ ref('silver_orders') }} where order_id is null

union all

select 'silver_orders', order_id, 'customer_id is null'
from {{ ref('silver_orders') }} where customer_id is null

union all

select 'silver_orders', order_id, 'order_status is null'
from {{ ref('silver_orders') }} where order_status is null

union all

select 'silver_orders', order_id, 'order_purchase_timestamp is null'
from {{ ref('silver_orders') }} where order_purchase_timestamp is null
