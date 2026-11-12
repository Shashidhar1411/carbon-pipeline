select issued_at, target_hour as target_hour_utc, method, forecast_ci from {{ source('raw', 'forecast') }}
