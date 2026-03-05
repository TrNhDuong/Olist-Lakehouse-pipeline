with distinct_statuses as (
    select distinct order_status
    from {{ ref('silver_orders') }}
    where order_status is not null
)

select
    row_number() over (order by order_status) as order_status_key,
    order_status
from distinct_statuses
