select
    date_trunc('month', d.date_day)  as month_start,
    sum(f.sales)                     as total_sales,
    sum(f.profit)                    as total_profit,
    count(distinct f.order_id)       as order_count,
    sum(f.quantity)                  as units_sold
from {{ ref('fact_sales') }} f
join {{ ref('dim_date') }} d on d.date_key = f.order_date_key
group by 1
order by 1
