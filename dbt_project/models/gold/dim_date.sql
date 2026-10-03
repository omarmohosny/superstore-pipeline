with bounds as (

    select min(order_date) as first_day, max(ship_date) as last_day
    from {{ ref('silver_superstore') }}

),

days as (

    select cast(unnest(generate_series(first_day, last_day, interval 1 day)) as date) as date_day
    from bounds

)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    year(date_day)                                as calendar_year,
    quarter(date_day)                             as calendar_quarter,
    month(date_day)                               as calendar_month,
    strftime(date_day, '%B')                      as month_name,
    day(date_day)                                 as day_of_month,
    dayofweek(date_day)                           as day_of_week,
    strftime(date_day, '%A')                      as day_name,
    dayofweek(date_day) in (0, 6)                 as is_weekend
from days
