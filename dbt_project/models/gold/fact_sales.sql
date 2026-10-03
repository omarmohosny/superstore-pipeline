{{ config(materialized='incremental', unique_key='row_id') }}

select
    s.row_id,
    s.order_id,
    c.customer_key,
    p.product_key,
    l.location_key,
    cast(strftime(s.order_date, '%Y%m%d') as integer) as order_date_key,
    cast(strftime(s.ship_date, '%Y%m%d') as integer)  as ship_date_key,
    s.ship_mode,
    s.sales,
    s.quantity,
    s.discount,
    s.profit,
    s._batch_id,
    s._loaded_at
from {{ ref('silver_superstore') }} s
join {{ ref('dim_customer') }} c on c.customer_id = s.customer_id
join {{ ref('dim_product') }} p on p.product_id = s.product_id
join {{ ref('dim_location') }} l on l.location_key = {{ location_key('s') }}

{% if is_incremental() %}
where s._loaded_at > (select max(_loaded_at) from {{ this }})
{% endif %}
