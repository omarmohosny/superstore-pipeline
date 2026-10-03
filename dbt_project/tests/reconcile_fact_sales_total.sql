{{ config(tags=['reconciliation']) }}

select f.total as fact_total, s.total as silver_total
from (select sum(sales) as total from {{ ref('fact_sales') }}) f
cross join (select sum(sales) as total from {{ ref('silver_superstore') }}) s
where abs(f.total - s.total) > 0.01
