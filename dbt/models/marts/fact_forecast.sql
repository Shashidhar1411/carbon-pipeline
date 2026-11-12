-- scored forecasts: joins each forecast to the hourly actual
with actual as (
    select date_trunc('hour', interval_utc) as hour_utc, avg(carbon_intensity_kg_per_mwh) as actual_ci
    from {{ ref('stg_carbon_interval') }} group by 1
)
select f.issued_at, f.target_hour_utc, f.method, f.forecast_ci, a.actual_ci, abs(f.forecast_ci - a.actual_ci) as abs_error
from {{ ref('stg_forecast') }} f left join actual a on a.hour_utc = f.target_hour_utc
