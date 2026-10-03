{{ config(tags=['silver_reconciliation']) }}

-- every bronze row must end up either in silver or in the rejects table
select b.n as bronze_rows, s.n as silver_rows, r.n as reject_rows
from (select count(*) as n from {{ source('bronze', 'raw_superstore') }}) b
cross join (select count(*) as n from {{ ref('silver_superstore') }}) s
cross join (select count(*) as n from {{ ref('silver_rejects') }}) r
where b.n <> s.n + r.n
