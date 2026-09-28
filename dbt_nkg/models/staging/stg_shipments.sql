select
    container_id,
    trade_id,
    origin,
    origin_port,
    destination_hub,
    bags_in_container,
    status,
    eta_date,
    run_id,
    ingested_at
from {{ source('raw', 'raw_shipments') }}