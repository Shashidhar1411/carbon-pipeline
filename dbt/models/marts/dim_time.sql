with hours as (
    select dateadd(hour, seq4(), '2026-01-01'::timestamp_ntz) as hour_utc from table(generator(rowcount => 24 * 800))
)
select hour_utc,
       convert_timezone('UTC', 'America/Los_Angeles', hour_utc) as hour_pt,
       date_trunc('day', convert_timezone('UTC', 'America/Los_Angeles', hour_utc))::date as date_pt,
       extract(hour from convert_timezone('UTC', 'America/Los_Angeles', hour_utc)) as hour_of_day_pt,
       dayofweekiso(convert_timezone('UTC', 'America/Los_Angeles', hour_utc)) as day_of_week,
       dayofweekiso(convert_timezone('UTC', 'America/Los_Angeles', hour_utc)) >= 6 as is_weekend
from hours
