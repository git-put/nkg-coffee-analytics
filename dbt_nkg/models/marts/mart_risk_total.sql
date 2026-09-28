with totals as (
    select
        sum(physical_mtm_eur) as physical_mtm_eur,
        sum(futures_mtm_eur) as futures_mtm_eur,
        sum(total_mtm_eur) as total_mtm_eur,
        sum(net_physical_bags) as net_physical_bags,
        sum(futures_hedge_bags) as futures_hedge_bags,
        sum(net_unhedged_bags) as net_unhedged_bags
    from {{ ref('mart_risk_by_origin') }}
)

select
    round(t.physical_mtm_eur, 2) as physical_mtm_eur,
    round(t.futures_mtm_eur, 2) as futures_mtm_eur,
    round(t.total_mtm_eur, 2) as total_mtm_eur,
    t.net_physical_bags,
    t.futures_hedge_bags,
    t.net_unhedged_bags,
    case when t.net_physical_bags > 0
        then round(100.0 * t.futures_hedge_bags / t.net_physical_bags, 1)
    end as hedge_ratio_pct,
    round(abs(t.net_unhedged_bags) * 10 * m.fx_usd_eur, 2) as eur_impact_of_10usd_price_move,
    m.futures_price_usd,
    m.fx_usd_eur,
    m.fx_source
from totals as t
cross join {{ ref('mart_latest_market') }} as m