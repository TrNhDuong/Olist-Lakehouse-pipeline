with products as (
    select * from {{ ref('silver_products') }}
),

categories as (
    select * from {{ ref('silver_categories') }}
)

select
    row_number() over (order by p.product_id) as product_key,
    p.product_id,
    p.product_category_name,
    c.product_category_name_english,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
from products p
left join categories c
    on p.product_category_name = c.product_category_name
