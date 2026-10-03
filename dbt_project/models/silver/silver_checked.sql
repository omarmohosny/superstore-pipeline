{{ config(materialized='view') }}

with typed as (

    select
        cast(row_id as integer)            as row_id,
        trim(order_id)                     as order_id,
        cast(order_date as date)           as order_date,
        cast(ship_date as date)            as ship_date,
        trim(ship_mode)                    as ship_mode,
        trim(customer_id)                  as customer_id,
        trim(customer_name)                as customer_name,
        trim(segment)                      as segment,
        trim(country)                      as country,
        trim(city)                         as city,
        trim(state)                        as state,
        trim(postal_code)                  as postal_code,
        trim(region)                       as region,
        trim(product_id)                   as product_id,
        trim(category)                     as category,
        trim(sub_category)                 as sub_category,
        trim(product_name)                 as product_name,
        cast(sales as double)              as sales,
        cast(quantity as integer)          as quantity,
        cast(discount as double)           as discount,
        cast(profit as double)             as profit,
        _batch_id,
        _loaded_at
    from {{ source('bronze', 'raw_superstore') }}

)

select
    *,
    case
        when row_id is null or order_id is null
            or customer_id is null or product_id is null          then 'missing key'
        when order_date is null or ship_date is null              then 'missing date'
        when sales is null or sales <= 0                          then 'non-positive sales'
        when ship_date < order_date                               then 'ship date before order date'
        when discount < 0 or discount > 1                         then 'discount out of range'
    end as reject_reason
from typed
