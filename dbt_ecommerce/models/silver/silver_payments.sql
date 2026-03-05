with bronze_payments as (
    select * 
    from {{ source('bronze', 'raw_payments') }}
)

select 
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    case 
        when payment_value < 0 then 'invalid'
        else 'valid'
    end as flag_valid   
from bronze_payments
where 
    order_id is not null
