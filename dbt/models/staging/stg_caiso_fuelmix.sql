-- one row per interval per fuel; latest revision wins
with src as (
    select {{ iso_ts('payload:interval_utc') }} as interval_utc,
           {{ iso_ts('payload:revision_ts') }}  as revision_ts,
           payload:fuels                        as fuels
    from {{ source('raw', 'caiso_fuelmix') }}
),
latest as (
    select * from src
    where interval_utc <= dateadd(minute, 10, sysdate())   -- never keep rows stamped in the future (stale-file bug)
    qualify row_number() over (partition by interval_utc order by revision_ts desc) = 1
)
select interval_utc, f.key::string as fuel, f.value::float as mw, revision_ts
from latest, lateral flatten(input => fuels) f
