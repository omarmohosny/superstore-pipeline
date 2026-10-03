with per_customer as (

    select
        f.customer_key,
        max(d.date_day)             as last_order_date,
        count(distinct f.order_id)  as frequency,
        sum(f.sales)                as monetary
    from {{ ref('fact_sales') }} f
    join {{ ref('dim_date') }} d on d.date_key = f.order_date_key
    group by 1

),

as_of as (

    -- measure recency from the newest order in the data, not from today
    select max(last_order_date) as as_of_date from per_customer

),

scored as (

    select
        c.*,
        date_diff('day', c.last_order_date, a.as_of_date)   as recency_days,
        ntile(4) over (order by date_diff('day', c.last_order_date, a.as_of_date) desc) as r_score,
        ntile(4) over (order by c.frequency)                as f_score,
        ntile(4) over (order by c.monetary)                 as m_score
    from per_customer c
    cross join as_of a

)

select
    s.customer_key,
    dc.customer_name,
    s.last_order_date,
    s.recency_days,
    s.frequency,
    round(s.monetary, 2) as monetary,
    s.r_score,
    s.f_score,
    s.m_score,
    case
        when s.r_score >= 3 and s.f_score >= 3 and s.m_score >= 3 then 'Champion'
        when s.r_score >= 3 and s.f_score >= 2                    then 'Loyal'
        when s.r_score <= 2 and s.f_score >= 3                    then 'At risk'
        when s.r_score = 1                                        then 'Lost'
        else 'Regular'
    end as rfm_segment
from scored s
join {{ ref('dim_customer') }} dc on dc.customer_key = s.customer_key
