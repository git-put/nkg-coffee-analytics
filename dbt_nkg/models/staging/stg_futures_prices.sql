select
    run_id,
    price_timestamp,
    contract,
    cast(price_usd_per_bag as double) as price_usd_per_bag,
    price_source
from {{ source('raw', 'raw_futures_prices') }}