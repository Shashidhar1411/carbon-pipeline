select interval_utc, ci as carbon_intensity_kg_per_mwh, total_mw, renewable_share, wind_mw, solar_mw, revision_ts, updated_at
from {{ source('raw', 'carbon_interval') }}
