-- Kiểm tra: không có giá trị âm trong các measure của gold layer

select 'fact_order' as source_table, order_id, 'total_item_value < 0' as issue
from {{ ref('fact_order') }} where total_item_value < 0

union all

select 'fact_order', order_id, 'total_freight_value < 0'
from {{ ref('fact_order') }} where total_freight_value < 0

union all

select 'fact_order', order_id, 'total_order_value < 0'
from {{ ref('fact_order') }} where total_order_value < 0

union all

select 'fact_order_item', order_id, 'price < 0'
from {{ ref('fact_order_item') }} where price < 0

union all

select 'fact_order_item', order_id, 'freight_value < 0'
from {{ ref('fact_order_item') }} where freight_value < 0

union all

select 'fact_order_payment', order_id, 'payment_value < 0'
from {{ ref('fact_order_payment') }} where payment_value < 0
