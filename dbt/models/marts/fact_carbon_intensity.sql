select c.interval_utc, c.carbon_intensity_kg_per_mwh, c.renewable_share, c.total_mw as generation_mw,
       c.wind_mw + c.solar_mw as wind_solar_mw, c.revision_ts, t.hour_of_day_pt, t.is_weekend
from {{ ref('stg_carbon_interval') }} c
left join {{ ref('dim_time') }} t on date_trunc('hour', c.interval_utc) = t.hour_utc
