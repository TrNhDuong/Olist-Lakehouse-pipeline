with order_dates as (
    select distinct
        cast(date_format(order_purchase_timestamp, 'yyyyMMdd') as int) as date_key,
        to_date(order_purchase_timestamp) as full_date
    from {{ ref('silver_orders') }}
    where order_purchase_timestamp is not null
)

select
    date_key,
    full_date,
    day(full_date) as day,
    month(full_date) as month,
    quarter(full_date) as quarter,
    year(full_date) as year,
    date_format(full_date, 'EEEE') as day_of_week,
    case 
        when dayofweek(full_date) in (1, 7) then true 
        else false 
    end as is_weekend
from order_dates
