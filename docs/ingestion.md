# Ingestion layer

## 1. What the ingestion layer does

* The ingestion layer collects data from three sources: CAISO, EIA and the National Weather Service. The fetched data is sent to Kafka, which works like a “mailbox” that holds the messages until Spark reads them.

## 2. Before you start


-Start Docker: docker compose up -d kafka kafka-ui redis

This starts the services the pipeline needs, such as Kafka.

* Create the .env file from .env.example and add EIA_API_KEY and NWS_USER_AGENT.

The EIA key and the weather user agent are stored here. This file must not be pushed to GitHub because it contains secret keys.

* Install Python packages: pip install -r requirements.txt

This installs all the packages listed in the requirements file.

* Create the Kafka topics: bash scripts/create_topics.sh

This creates the topics in Kafka before a producer sends data from the sources.



## 3. Running each producer

### CAISO (5-minute fuel mix and demand)



* Backfill mode loads old (past) data for the dates you choose with --start and --end:

python -m src.producers.caiso_producer backfill --start 2026-01-01 --end 2026-09-30

* Live mode keeps running and fetches the newest data from the CAISO website every 5 minutes:

python -m src.producers.caiso_producer live



### EIA-930 (hourly, all U.S. balancing authorities)



* Without --respondent, the producer pulls data for every balancing authority in the U.S. (about 65 regions, over 3 million rows):

python -m src.producers.eia_producer backfill --start 2026-01-01 --end 2026-09-30

* With --respondent CISO, it pulls only California’s data. This is useful for a quick first test, because the full run takes much longer and the dataset becomes very large:

python -m src.producers.eia_producer backfill --start 2026-01-01 --end 2026-09-30 --respondent CISO



### NWS (hourly weather forecast)



* python -m src.producers.nws_producer

This fetches the hourly temperature and sky-cover forecast from the National Weather Service. We need this continuous weather data because cloud cover and temperature affect how much clean solar energy is available, which helps predict the cleanest hours to use energy.

* Setting needed: NWS_USER_AGENT in the .env file. The weather service requires every request to include a descriptive user agent (for example, a project name and contact email), otherwise it may block the request.



















## 4. Kafka topics



| Topic | Sent by | What it contains |

| --- | --- | --- |

| `caiso.fuelmix` | `caiso_producer` | CAISO fuel mix: how many MW each fuel (solar, wind, gas, etc.) produces every 5 minutes |

| `caiso.demand` | `caiso_producer` | CAISO electricity demand: how much power California is using every 5 minutes |

| `eia930.fueltype` | `eia_producer` | EIA-930 hourly generation by fuel type for each balancing authority |

| `nws.forecast` | `nws_producer` | National Weather Service hourly temperature and sky-cover forecast |



Each topic has only one partition so that messages stay in time order. Spark reads the 5-minute data in order and uses a watermark to decide when a time window is complete. If a 10:05 message arrived after a 10:10 message, Spark could treat it as late data and drop it, or calculate the carbon intensity for that window with missing data.




## 5. Reading the producer output

At the end of each run, the CAISO producer prints three counts, for example:

    new 437, revised 421, duplicate 46195

- **New:** an interval the producer has never seen before. It is sent to Kafka.
- **Revised:** an interval that was already seen but now has different values. It is sent to Kafka again because CAISO corrected the data.
- **Duplicate:** an interval that was already seen with exactly the same values. It is dropped and not sent to Kafka.

The producer uses a Bloom filter to check quickly whether it has seen an interval before, without storing every record.

Why duplicates are high: in live mode, the producer downloads the entire day's file every 5 minutes. Intervals that were already processed appear again with the same values, so many duplicates are detected and dropped. Only the newest intervals are counted as new.



## 6. Common problems

| Problem | What happened | Fix |
| --- | --- | --- |
| Docker not running | The producer could not connect to Kafka because Docker was not started, and Docker took some time to load. | Start Docker Desktop first, wait until it is fully running, then run `docker compose up -d kafka kafka-ui redis` before starting any producer. |
| Running out of disk space | The C drive did not have enough free space for the tools and Docker images. | Free up space on C, or install tools and keep the project on another drive (we used the E drive). |
| Missing `.env` file | The producer could not find the API key and settings. | Check that `.env` exists in the project folder (copied from `.env.example`) and contains `EIA_API_KEY` and `NWS_USER_AGENT`. |
| EIA backfill too large | A full backfill for every balancing authority takes a long time and creates millions of rows. | Test first with a smaller run, for example only California (`--respondent CISO`) or a shorter date range. |









