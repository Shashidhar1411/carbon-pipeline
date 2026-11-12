select payload:period_utc::timestamp_ntz as period_utc,
       payload:respondent::string        as balancing_authority,
       payload:"respondent-name"::string as balancing_authority_name,
       payload:fueltype::string          as fuel_type,
       payload:value::float              as mwh
from {{ source('raw', 'eia_fuel') }}
qualify row_number() over (partition by period_utc, balancing_authority, fuel_type order by period_utc) = 1
