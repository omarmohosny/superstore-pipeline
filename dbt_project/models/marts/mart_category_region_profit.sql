select
    p.category,
    p.sub_category,
    l.region,
    sum(f.sales)                                  as total_sales,
    sum(f.profit)                                 as total_profit,
    sum(f.profit) / nullif(sum(f.sales), 0)       as profit_margin
from {{ ref('fact_sales') }} f
join {{ ref('dim_product') }} p on p.product_key = f.product_key
join {{ ref('dim_location') }} l on l.location_key = f.location_key
group by 1, 2, 3
