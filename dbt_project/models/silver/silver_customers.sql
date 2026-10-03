-- one row per customer, taking the most recent name and segment
select
    customer_id,
    arg_max(customer_name, order_date) as customer_name,
    arg_max(segment, order_date)       as segment
from {{ ref('silver_superstore') }}
group by customer_id
