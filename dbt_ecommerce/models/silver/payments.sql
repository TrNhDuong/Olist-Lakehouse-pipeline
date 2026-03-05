with bronze_payments as (
    select * 
    from {{ source('bronze', 'raw_payments') }}
)

select 
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
from bronze_payments
where 
    order_id is not null
