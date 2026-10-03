select *
from {{ ref('silver_checked') }}
where reject_reason is not null
