with shipments as (
    select
        *,
        case status
            when 'Booked at Origin' then 1
            when 'In Transit' then 2
            when 'Arrived at Hub' then 3
            when 'Customs Clearance' then 4
            when 'Delivered to Warehouse' then 5
        end as status_order
    from {{ ref('stg_shipments') }}
)

select
    destination_hub,
    status,
    status_order,
    count(*) as containers,
    sum(bags_in_container) as bags,
    min(eta_date) as earliest_eta,
    max(eta_date) as latest_eta
from shipments
group by destination_hub, status, status_order