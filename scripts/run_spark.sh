#!/usr/bin/env bash
# Spark in local mode with the Kafka connector. Run from the repo root.
set -e
export PYTHONPATH=$PWD
spark-submit --master "local[2]" --driver-memory 1g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --conf spark.sql.shuffle.partitions=4 \
  src/streaming/carbon_stream.py
