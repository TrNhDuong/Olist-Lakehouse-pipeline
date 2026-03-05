with bronze_categories as (
    select * 
    from {{ source('bronze', 'raw_categories') }}
)

select 
    product_category_name,
    product_category_name_english
from bronze_categories
where 
    product_category_name is not null
