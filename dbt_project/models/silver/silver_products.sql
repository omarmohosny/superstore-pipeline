-- a few product ids carry more than one name in the source, so keep the latest
select
    product_id,
    arg_max(product_name, order_date) as product_name,
    arg_max(category, order_date)     as category,
    arg_max(sub_category, order_date) as sub_category
from {{ ref('silver_superstore') }}
group by product_id
