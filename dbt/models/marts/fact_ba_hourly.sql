select period_utc, balancing_authority, fuel_type, mwh from {{ ref('stg_eia_fuel') }}
