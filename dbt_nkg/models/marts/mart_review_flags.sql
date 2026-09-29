-- mart_review_flags.sql
--
-- Purpose: surface the handful of things a trader or ops person should look
-- at, instead of making them scan every chart. Each row is one flag, with:
--   severity          'High' or 'Medium' (nothing below the threshold shows up)
--   category          'Exposure' / 'Compliance' / 'Logistics'
--   title             short headline
--   why_it_matters    the business reason, in plain language
--   suggested_action  ALWAYS phrased as review/confirm/ask - never an order
--                      to trade, hedge, or reroute. This system flags for a
--                      human to decide; it doesn't decide.
--   scope             what the flag is about (an origin, a hub, or the whole book)
--   metric_value / metric_unit   the number behind the flag, kept separate so
--                      the dashboard can format it (commas, currency) itself
--
-- Thresholds below are simple, round numbers for a student project - not a
-- real risk model. Tune them in one place, at the top of each CTE.

with exposure as (
    select
        'Exposure' as category,
        case
            when abs(net_unhedged_bags) >= 2000 then 'High'
            when abs(net_unhedged_bags) >= 500 then 'Medium'
        end as severity,
        case
            when net_unhedged_bags >= 0 then 'Unhedged long position'
            else 'Unhedged short position'
        end as title,
        case
            when net_unhedged_bags >= 0
                then 'These bags lose value if the market price falls, and nothing offsets that loss today.'
            else
                'These bags lose value if the market price rises, and nothing offsets that loss today.'
        end as why_it_matters,
        'Review whether to place an additional hedge, or confirm the desk is comfortable carrying this exposure.' as suggested_action,
        'Whole book' as scope,
        abs(net_unhedged_bags) as metric_value,
        'bags' as metric_unit
    from {{ ref('mart_risk_total') }}
),

eudr as (
    select
        'Compliance' as category,
        case
            when compliance_rate_pct < 70 then 'High'
            when compliance_rate_pct < 85 then 'Medium'
        end as severity,
        'EUDR paperwork behind for ' || origin as title,
        'These bags cannot legally be sold into the EU until deforestation-free proof is on file. A shipment can be held at customs without it.' as why_it_matters,
        'Ask the compliance team to prioritise polygon mapping for this origin before the coffee reaches an EU port.' as suggested_action,
        origin as scope,
        pending_bags as metric_value,
        'bags pending' as metric_unit
    from {{ ref('mart_eudr_compliance') }}
    where origin <> 'ALL ORIGINS'
),

logistics as (
    select
        'Logistics' as category,
        case
            when count(*) >= 10 then 'High'
            when count(*) >= 3 then 'Medium'
        end as severity,
        'Containers overdue at ' || destination_hub as title,
        'These containers were due to arrive already but are not marked delivered. The record may be stale, or the shipment may actually be delayed.' as why_it_matters,
        'Ask logistics to confirm the real status, or update the system if they have already arrived.' as suggested_action,
        destination_hub as scope,
        count(*) as metric_value,
        'containers overdue' as metric_unit
    from {{ ref('stg_shipments') }}
    where eta_date < current_date and status <> 'Delivered to Warehouse'
    group by destination_hub
),

all_flags as (
    select * from exposure where severity is not null
    union all
    select * from eudr where severity is not null
    union all
    select * from logistics where severity is not null
)

select *
from all_flags
order by
    case severity when 'High' then 1 when 'Medium' then 2 else 3 end,
    category
