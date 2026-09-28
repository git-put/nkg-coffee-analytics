with latest_price as (
    select price_timestamp, price_usd_per_bag, price_source
    from {{ ref('stg_futures_prices') }}
    order by price_timestamp desc
    limit 1
),

latest_fx as (
    select ingested_at as fx_loaded_at, fx_usd_eur, fx_usd_brl, fx_source
    from {{ ref('stg_trades') }}
    order by ingested_at desc
    limit 1
)

select
    p.price_timestamp as price_timestamp_utc,
    p.price_usd_per_bag as futures_price_usd,
    p.price_source,
    f.fx_loaded_at as fx_timestamp_utc,
    f.fx_usd_eur,
    f.fx_usd_brl,
    f.fx_source
from latest_price as p
cross join latest_fx as f