select * exclude (reject_reason)
from {{ ref('silver_checked') }}
where reject_reason is null
