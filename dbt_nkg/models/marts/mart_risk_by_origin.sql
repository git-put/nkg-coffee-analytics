with by_origin as (
    select
        origin,
        sum(case when book = 'Physical' then mtm_eur else 0 end) as physical_mtm_eur,
        sum(case when book = 'Futures' then mtm_eur else 0 end) as futures_mtm_eur,
        sum(mtm_eur) as total_mtm_eur,
        sum(case when trade_type = 'Physical Buy' then quantity_bags else 0 end) as physical_bought_bags,
        sum(case when trade_type = 'Physical Sell' then quantity_bags else 0 end) as physical_sold_bags,
        sum(case when trade_type = 'Futures Hedge' then quantity_bags else 0 end) as futures_hedge_bags
    from {{ ref('mart_trade_mtm') }}
    group by origin
)

select
    origin,
    round(physical_mtm_eur, 2) as physical_mtm_eur,
    round(futures_mtm_eur, 2) as futures_mtm_eur,
    round(total_mtm_eur, 2) as total_mtm_eur,
    physical_bought_bags,
    physical_sold_bags,
    physical_bought_bags - physical_sold_bags as net_physical_bags,
    futures_hedge_bags,
    physical_bought_bags - physical_sold_bags - futures_hedge_bags as net_unhedged_bags
from by_origin