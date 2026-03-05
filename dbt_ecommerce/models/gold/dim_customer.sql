with customers as (
    select * from {{ ref('silver_customers') }}
),

geo as (
    select 
        geolocation_zip_code_prefix,
        avg(geolocation_lat) as geolocation_lat,
        avg(geolocation_lng) as geolocation_lng
    from {{ ref('silver_geolocation') }}
    group by geolocation_zip_code_prefix
)

select
    row_number() over (order by c.customer_id) as customer_key,
    c.customer_unique_id,
    c.customer_id,
    c.customer_city,
    c.customer_state,
    c.customer_zip_code_prefix,
    g.geolocation_lat,
    g.geolocation_lng
from customers c
left join geo g
    on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
