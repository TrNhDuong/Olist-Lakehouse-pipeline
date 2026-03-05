-- Kiểm tra: các bảng dimension silver còn lại không có key bị NULL

select 'silver_payments' as source_table, 'order_id is null' as issue
from {{ ref('silver_payments') }} where order_id is null

union all

select 'silver_reviews', 'review_id is null'
from {{ ref('silver_reviews') }} where review_id is null

union all

select 'silver_reviews', 'order_id is null'
from {{ ref('silver_reviews') }} where order_id is null

union all

select 'silver_products', 'product_id is null'
from {{ ref('silver_products') }} where product_id is null

union all

select 'silver_customers', 'customer_id is null'
from {{ ref('silver_customers') }} where customer_id is null

union all

select 'silver_sellers', 'seller_id is null'
from {{ ref('silver_sellers') }} where seller_id is null

union all

select 'silver_categories', 'product_category_name is null'
from {{ ref('silver_categories') }} where product_category_name is null

union all

select 'silver_geolocation', 'geolocation_zip_code_prefix is null'
from {{ ref('silver_geolocation') }} where geolocation_zip_code_prefix is null
