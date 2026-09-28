with physical as (
    select origin, eudr_status, quantity_bags
    from {{ ref('stg_trades') }}
    where trade_type <> 'Futures Hedge'
),

market as (
    select futures_price_usd, fx_usd_eur
    from {{ ref('mart_latest_market') }}
),

agg as (
    select
        origin,
        count(*) as physical_trades,
        count(*) filter (where eudr_status = 'Verified') as verified_trades,
        count(*) filter (where eudr_status <> 'Verified') as pending_trades,
        sum(quantity_bags) as total_bags,
        sum(quantity_bags) filter (where eudr_status = 'Verified') as verified_bags,
        sum(quantity_bags) filter (where eudr_status <> 'Verified') as pending_bags
    from physical
    group by rollup (origin)
)

select
    coalesce(a.origin, 'ALL ORIGINS') as origin,
    a.physical_trades,
    a.verified_trades,
    a.pending_trades,
    a.total_bags,
    coalesce(a.verified_bags, 0) as verified_bags,
    coalesce(a.pending_bags, 0) as pending_bags,
    round(100.0 * coalesce(a.verified_bags, 0) / a.total_bags, 1) as compliance_rate_pct,
    round(coalesce(a.pending_bags, 0) * m.futures_price_usd * m.fx_usd_eur, 2) as pending_value_eur
from agg as a
cross join market as m