# DATA 228 rubric -> where it is covered

| Rubric item | Pts | Where / status |
|---|---|---|
| Use of streaming algorithms | 5 | `src/common/streaming_algos.py`: Bloom filter (producer dedupe/revision detection), DGIM (dirty-interval count on dashboard), reservoir sampling. Unit-tested. |
| Stream processing frameworks | 5 | Kafka producers, Spark Structured Streaming (`src/streaming/carbon_stream.py`) |
| Locality Sensitive Hashing | 5 | `HyperplaneLSH` in `src/forecasting/methods.py` retrieves analog days for the `lsh_analog` forecaster |
| Privacy techniques | 5 | `src/privacy/anonymize.py`: generalisation + k-anonymity, Laplace-mechanism DP |
| Other techniques | 5 | Optional: explainable forecasting / responsible-AI discussion (no ML trained, every forecast scoreable in `fact_forecast`) - mention in report |
| New tools not used in HW | 3 | dbt, Snowflake, Airflow, Redis, Streamlit - list the ones that were NOT used in your homework |
| Flow schematic diagram | 1 | `docs/flow_diagram.png` (in report as Fig. 1) |
| Innovation | 5 | open end-to-end carbon-aware pipeline; LSH analog days; revision-safe streaming; privacy-preserving release |
| Technical difficulty | 5 | revisions/watermarks, DST + UTC alignment, battery sign handling, idempotent merges, SCD2 |
| Literature survey | 7 | `report/main.tex` Section II (subsections; 22 refs). VERIFY every reference details |
| Report | 7 | `report/main.tex` - fill every red TODO; run Grammarly; avoid screenshots |
| Used LaTeX | 2 | IEEEtran template; upload the .tex |
| Lessons learned | 5 | Section V of the report - also put a slide |
| Prospects of publication | 3 | proposal targets ACM e-Energy / HotCarbon; the savings study + open pipeline is the angle |
| Code walkthrough / Demo / Presentation / Q&A / Slides / creative presentation | 5+5+5+5+3+2 | Slides not built yet - build after screenshots are in hand |
| Version control | 3 | YOU: `git init`, then commit in small pieces by each member over the remaining weeks (never one big commit). Public repo. |
| Teamwork | 5 | divide per roles in proposal; show commits by all four |
| Pair programming (optional) | 2 | YOU: do one real session (VS Code Live Share/Copilot) and screenshot it |
| Agile / scrum | 2 | `docs/sprint_template.md` - fill weekly, submit minutes on Canvas |
