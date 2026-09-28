with trades as (
    select * from {{ ref('stg_trades') }}
),

market as (
    select * from {{ ref('mart_latest_market') }}
),

enriched as (
    select
        *,
        case trade_type when 'Physical Buy' then 1 else -1 end as direction,
        case when trade_type = 'Futures Hedge' then 'Futures' else 'Physical' end as book
    from trades
)

select
    e.trade_id,
    e.trade_timestamp,
    e.trader,
    e.counterparty,
    e.origin,
    e.warehouse_location,
    e.trade_type,
    e.book,
    e.quantity_bags,
    e.direction * e.quantity_bags as signed_position_bags,
    e.trade_price_usd,
    m.futures_price_usd as market_price_usd,
    round(e.direction * (m.futures_price_usd - e.trade_price_usd) * e.quantity_bags, 2) as mtm_usd,
    round(e.direction * (m.futures_price_usd - e.trade_price_usd) * e.quantity_bags * m.fx_usd_eur, 2) as mtm_eur,
    m.fx_usd_eur as fx_usd_eur_latest,
    m.fx_source
from enriched as e
cross join market as m