# Screenshot checklist (capture from YOUR running stack, never from sample data)

Rubric reminder: the report is checked by TurnItIn and penalised for "unnecessary screenshots" -> put only the starred
(★) ones in the report (max 2-3), use all of them in the slides. Include a visible timestamp or terminal prompt.

| # | What to capture | How | Report | Slides |
|---|---|---|---|---|
| 1 | All containers running | `docker compose ps` or Docker Desktop | | ✔ |
| 2 | Kafka topics with message counts | Kafka UI http://localhost:8080 -> Topics | | ✔ |
| 3 | Producer log showing new / revised / duplicate counts (Bloom filter) | terminal running `caiso_producer live` | | ✔ |
| 4 | Spark UI streaming/SQL tab, watermark in query progress | http://localhost:4040 | | ✔ |
| 5 | Live value + ranking in Redis | `redis-cli HGETALL carbon:current` and `redis-cli ZRANGE carbon:ranking 0 3 WITHSCORES` | | ✔ |
| 6 | Row counts for raw / staging / marts | Snowflake: `select count(*)` per table | | ✔ |
| 7 | `dbt test` all passing + dbt docs lineage graph | `dbt test`; `dbt docs serve` -> lineage | ★ (lineage) | ✔ |
| 8 | SCD Type 2 in action: two rows for one fuel after you change a factor | edit seed, `dbt seed && dbt snapshot`, `select * from marts.dim_fuel where fuel='Natural Gas'` | | ✔ |
| 9 | Airflow DAG graph; run waiting in the sensor, then released | http://localhost:8081 (Graph + task log) | | ✔ |
| 10 | ★ Dashboard (live intensity, ranking, history, carbon saved) | `streamlit run dashboard/app.py` | ★ | ✔ |
| 11 | Duck-curve plot: average intensity by Pacific hour | dashboard "Typical day" chart | ★ | ✔ |
| 12 | Accuracy result (ours vs CAISO) | `python -m eval.accuracy_check ...` output | table in report | ✔ |
| 13 | Latency result | `python -m eval.latency_test` output | number in report | ✔ |
| 14 | Forecast comparison table | `python -m eval.forecast_eval` | table in report | ✔ |
| 15 | Savings chart `data/ev_savings.png` | `python -m eval.savings_study` | figure | ✔ |
| 16 | GitHub commit history across weeks, all four authors | GitHub -> Insights / commits | | ✔ |
| 17 | Pair programming / Live Share / Copilot session (optional, 2 pts) | any editor | | ✔ |
| 18 | Sprint board / meeting minutes (agile, 2 pts) | Trello etc. -> Canvas | | |
