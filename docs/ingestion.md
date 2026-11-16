# Ingestion layer

## 1. What the ingestion layer does

* The ingestion layer collects data from three sources: CAISO, EIA and the National Weather Service. The fetched data is sent to Kafka, which works like a “mailbox” that holds the messages until Spark reads them.

## 2. Before you start

→ List what must be running or set up first. Facts to include:

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

→ 

* Backfill mode loads old (past) data for the dates you choose with --start and --end:

python -m src.producers.caiso_producer backfill --start 2026-01-01 --end 2026-09-30

* Live mode keeps running and fetches the newest data from the CAISO website every 5 minutes:

python -m src.producers.caiso_producer live 

### EIA-930 (hourly, all U.S. balancing authorities)

→

* Without --respondent, the producer pulls data for every balancing authority in the U.S. (about 65 regions, over 3 million rows):

python -m src.producers.eia_producer backfill --start 2026-01-01 --end 2026-09-30

* With --respondent CISO, it pulls only California’s data. This is useful for a quick first test, because the full run takes much longer and the dataset becomes very large:

python -m src.producers.eia_producer backfill --start 2026-01-01 --end 2026-09-30 --respondent CISO

### NWS (hourly weather forecast)

→ What it fetches and the one setting it needs.

* python -m src.producers.nws_producer

This fetches the hourly temperature and sky-cover forecast from the National Weather Service. We need this continuous weather data because cloud cover and temperature affect how much clean solar energy is available, which helps predict the cleanest hours to use energy.

* Setting needed: NWS_USER_AGENT in the .env file. The weather service requires every request to include a descriptive user agent (for example, a project name and contact email), otherwise it may block the request.

