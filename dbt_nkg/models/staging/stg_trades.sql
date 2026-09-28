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
    cast(trade_price_usd as double) as trade_price_usd,
    cast(fx_usd_eur as double) as fx_usd_eur,
    cast(fx_usd_brl as double) as fx_usd_brl,
    fx_source,
    eudr_status,
    ingested_at
from {{ source('raw', 'raw_coffee_trades') }}