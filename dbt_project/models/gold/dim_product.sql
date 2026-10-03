select
    md5(product_id) as product_key,
    product_id,
    product_name,
    category,
    sub_category
from {{ ref('silver_products') }}
