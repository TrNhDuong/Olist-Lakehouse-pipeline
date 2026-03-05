-- Kiểm tra: tổng giá trị đơn hàng trong fact_order phải khớp với tổng line items
-- Cho phép sai số 0.01 do làm tròn

select
    fo.order_id,
    fo.total_order_value,
    coalesce(fi.items_total, 0) as items_total,
    abs(fo.total_order_value - coalesce(fi.items_total, 0)) as diff
from {{ ref('fact_order') }} fo
left join (
    select order_id, sum(total_value) as items_total
    from {{ ref('fact_order_item') }}
    group by order_id
) fi on fo.order_id = fi.order_id
where abs(fo.total_order_value - coalesce(fi.items_total, 0)) > 0.01
