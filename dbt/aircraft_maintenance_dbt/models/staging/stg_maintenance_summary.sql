select component
    , event_count
    , event_date
    , severity
    , status
from {{ source('gold', 'DAILY_MAINTENANCE_SUMMARY') }}