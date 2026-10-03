{{ config(tags=['reconciliation']) }}

select f.n as fact_rows, s.n as silver_rows
from (select count(*) as n from {{ ref('fact_sales') }}) f
cross join (select count(*) as n from {{ ref('silver_superstore') }}) s
where f.n <> s.n
