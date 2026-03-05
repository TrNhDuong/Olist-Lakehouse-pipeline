with sellers as (
    select * from {{ ref('silver_sellers') }}
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
    row_number() over (order by s.seller_id) as seller_key,
    s.seller_id,
    s.seller_city,
    s.seller_state,
    s.seller_zip_code_prefix,
    g.geolocation_lat,
    g.geolocation_lng
from sellers s
left join geo g
    on s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
