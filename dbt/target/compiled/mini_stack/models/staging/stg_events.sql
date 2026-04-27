select
  cast(event_timestamp as timestamp) as event_timestamp,
  cast(user_id as varchar)          as user_id,
  lower(trim(event_type))           as event_type,
  cast(amount as double)            as amount,
  upper(trim(country))              as country
from "analytics"."raw"."raw_events"