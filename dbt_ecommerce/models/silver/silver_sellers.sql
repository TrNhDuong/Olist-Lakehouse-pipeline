with bronze_sellers as (
    select * 
    from {{ source('bronze', 'raw_sellers') }}
)

select 
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    coalesce(seller_state, 'unknown') as seller_state
from bronze_sellers
where 
    seller_id is not null
