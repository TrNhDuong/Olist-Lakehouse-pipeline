with bronze_products as (
    select * 
    from {{ source('bronze', 'raw_products') }}
)

select 
    product_id,
    product_category_name,
    product_name_lenght,
    product_description_lenght,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
from bronze_products
where 
    product_id is not null
