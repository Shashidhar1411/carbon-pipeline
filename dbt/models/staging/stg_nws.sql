select {{ iso_ts('payload:target_hour_utc') }} as target_hour_utc,
       payload:temperature_c::float            as temperature_c,
       payload:sky_cover_pct::float            as sky_cover_pct,
       {{ iso_ts('payload:issued_at') }}       as issued_at
from {{ source('raw', 'nws') }}
qualify row_number() over (partition by target_hour_utc order by issued_at desc) = 1
