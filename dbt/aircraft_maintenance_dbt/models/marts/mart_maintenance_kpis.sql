select component
    , severity
    , status
    , sum(event_count) as total_events
from {{ ref('stg_maintenance_summary') }}
group by component
    , severity
    , status  