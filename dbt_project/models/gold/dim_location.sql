with places as (

    select
        country,
        state,
        city,
        postal_code,
        arg_max(region, order_date) as region
    from {{ ref('silver_superstore') }}
    group by country, state, city, postal_code

)

select
    {{ location_key('places') }} as location_key,
    country,
    state,
    city,
    postal_code,
    region
from places
