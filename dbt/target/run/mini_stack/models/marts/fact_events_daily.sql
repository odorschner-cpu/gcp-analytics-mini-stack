
  
    
    

    create  table
      "analytics"."analytics"."fact_events_daily__dbt_tmp"
  
    as (
      select
  cast(date_trunc('day', event_timestamp) as date) as event_date,
  country,
  event_type,
  count(*) as event_count,
  sum(amount) as total_amount
from "analytics"."analytics"."stg_events"
group by 1,2,3
order by 1,2,3
    );
  
  