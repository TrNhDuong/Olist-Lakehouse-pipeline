with bronze_customers as (
    select * 
    from {{ source('bronze', 'raw_customers') }}
)

select 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from bronze_customers
where 
    customer_id is not null
