with distinct_types as (
    select distinct payment_type
    from {{ ref('silver_payments') }}
    where payment_type is not null
)

select
    row_number() over (order by payment_type) as payment_type_key,
    payment_type
from distinct_types
