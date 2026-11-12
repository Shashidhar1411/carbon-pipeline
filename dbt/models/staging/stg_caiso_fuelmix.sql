-- one row per interval per fuel; latest revision wins
with src as (
    select payload:interval_utc::timestamp_ntz as interval_utc,
           payload:revision_ts::timestamp_ntz  as revision_ts,
           payload:fuels                       as fuels
    from {{ source('raw', 'caiso_fuelmix') }}
),
latest as (
    select * from src
    qualify row_number() over (partition by interval_utc order by revision_ts desc) = 1
)
select interval_utc, f.key::string as fuel, f.value::float as mw, revision_ts
from latest, lateral flatten(input => fuels) f
