#!/usr/bin/env bash
# one single-partition topic per feed (keeps event-time order for the Spark watermark)
for t in caiso.fuelmix caiso.demand eia930.fueltype nws.forecast; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $t --partitions 1 --replication-factor 1
done
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
