select *
from {{ ref('mart_risk_total') }}
where abs(total_mtm_eur - (physical_mtm_eur + futures_mtm_eur)) > 0.05