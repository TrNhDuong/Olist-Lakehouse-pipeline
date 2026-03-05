with bronze_reviews as (
    select * 
    from {{ source('bronze', 'raw_reviews') }}
)

select 
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
from bronze_reviews
where 
    review_id is not null
    and order_id is not null
