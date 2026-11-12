select distinct balancing_authority, balancing_authority_name from {{ ref('stg_eia_fuel') }}
