select
    f.ship_mode,
    l.region,
    count(*)                                          as order_lines,
    avg(date_diff('day', od.date_day, sd.date_day))   as avg_days_to_ship,
    max(date_diff('day', od.date_day, sd.date_day))   as max_days_to_ship
from {{ ref('fact_sales') }} f
join {{ ref('dim_location') }} l on l.location_key = f.location_key
join {{ ref('dim_date') }} od on od.date_key = f.order_date_key
join {{ ref('dim_date') }} sd on sd.date_key = f.ship_date_key
group by 1, 2
