# Real-Time California Grid Carbon Pipeline (DATA 226 / 228)

CAISO + EIA-930 + NWS -> Kafka -> Spark Structured Streaming -> Redis (live) + Snowflake/dbt (history) -> Airflow carbon-aware
scheduler, dashboard, forecasts, savings study. Flow diagram: `docs/flow_diagram.png`.

## What is tested and what is not (read this first)
**Unit-tested offline (`pytest tests`, 17 tests):** carbon-intensity math incl. battery handling; DST/UTC conversion; CAISO CSV parsing;
Bloom filter, DGIM, reservoir sampling; Spark carbon-intensity and revision-replacement logic (batch mode, compared against the
pure-Python reference); the Redis/warehouse sink (idempotent revisions, late data); forecasters + LSH; k-anonymity/DP; EV savings.
**Never run against the live services - first real run may need small fixes:** the CAISO/EIA/NWS/OASIS HTTP calls (endpoint paths and
column names were written from documentation/memory; check each once in a browser - see *First-run checks*), Kafka<->Spark wiring,
all Snowflake code (MERGE, PUT/COPY, dbt SQL), the Airflow DAG, the Streamlit dashboard.
Anything produced by the smoke tests used SYNTHETIC data - never put those numbers or plots in the report.

## Setup
```bash
cp .env.example .env            # add EIA_API_KEY (free), Snowflake creds; then: set -a; source .env; set +a
pip install -r requirements.txt
docker compose up -d kafka kafka-ui redis      # add `airflow` later
bash scripts/create_topics.sh
# Snowflake worksheet: run docs/snowflake_setup.sql ; then:  python -c "from src.common.store import Store; Store()"
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
```
Leave `SNOWFLAKE_ACCOUNT` empty to develop against a local SQLite file (dbt then does not apply).

## Run order
```bash
# 1  stream processor (own terminal)
bash scripts/run_spark.sh
# 2  ingestion
python -m src.producers.caiso_producer backfill --start 2026-01-01 --end 2026-09-30
python -m src.producers.eia_producer   backfill --start 2026-01-01 --end 2026-09-30     # add --respondent CISO to test small first
python -m src.producers.nws_producer &
python -m src.producers.caiso_producer live &         # keeps polling every 5 min
# 3  raw landing in the warehouse (parsed by dbt)
python -m src.loaders.kafka_to_raw --once
# 4  dbt
cd dbt && dbt seed && dbt run && dbt snapshot && dbt test && dbt docs generate && dbt docs serve
# 5  forecasts + ranking in Redis, API, dashboard
python -m src.forecasting.run_forecast --loop &
uvicorn src.serving.api:app --port 8000 &
streamlit run dashboard/app.py
# 6  scheduler demo: docker compose up -d airflow ; open :8081 (password in `docker logs airflow`); trigger DAG carbon_aware_batch
# 7  evaluation (proposal section 4.6)
python -m eval.latency_test --minutes 60
python -m eval.accuracy_check --start 2026-09-01 --end 2026-09-30
python -m eval.forecast_eval
python -m eval.savings_study
```

## First-run checks (5 minutes, saves hours)
1. Open `https://www.caiso.com/outlook/SP/History/20260601/fuelsource.csv` and `demand.csv`, and look for the emissions file
   for `accuracy_check` (set `CAISO_CO2_FILE`). Confirm column names/units match `caiso_producer.py` / `accuracy_check.py`.
2. `curl "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/?api_key=KEY&frequency=hourly&data[0]=value&length=2"`
   - confirm fields `period, respondent, fueltype, value`.
3. Replace the placeholder emission factors in `src/common/emissions.py` **and** `dbt/seeds/egrid_factors.csv` with the eGRID values
   you will cite (keep them identical).
4. Topics must stay single-partition (backfill order matters for the 30-minute watermark).

## Layout
`src/common` config, UTC handling, emissions, streaming algorithms, warehouse store | `src/producers` Kafka producers |
`src/streaming` Spark job | `src/serving` Redis + API | `src/forecasting` 3 forecasters (+LSH) | `src/privacy` | `src/simulation` EV |
`dags` Airflow | `dbt` models/seeds/snapshot | `dashboard` Streamlit | `eval` proposal-4.6 evaluations | `report` IEEE LaTeX | `docs`
