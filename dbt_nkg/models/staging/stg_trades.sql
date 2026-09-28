select
    trade_id,
    trade_timestamp,
    run_id,
    trader,
    counterparty,
    origin,
    warehouse_location,
    trade_type,
    quantity_bags,
    purchase_price_usd,
    live_market_price_usd,
    fx_usd_eur,
    fx_usd_brl,
    fx_source,
    eudr_status,
    ingested_at
from {{ source('raw', 'raw_coffee_trades') }}