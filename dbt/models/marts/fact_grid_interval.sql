select interval_utc, fuel, mw from {{ ref('stg_caiso_fuelmix') }}
