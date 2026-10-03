select
    md5(customer_id) as customer_key,
    customer_id,
    customer_name,
    segment
from {{ ref('silver_customers') }}
