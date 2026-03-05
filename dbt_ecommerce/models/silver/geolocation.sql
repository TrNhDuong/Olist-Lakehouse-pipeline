with bronze_geolocation as (
    select * 
    from {{ source('bronze', 'raw_geolocation') }}
)

select 
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
from bronze_geolocation
where 
    geolocation_zip_code_prefix is not null
