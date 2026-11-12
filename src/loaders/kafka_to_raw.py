"""Consume the Kafka topics and land the raw JSON in the warehouse (RAW.CAISO_FUELMIX, RAW.CAISO_DEMAND,
RAW.EIA_FUEL, RAW.NWS). dbt staging models parse the JSON from there.

  python -m src.loaders.kafka_to_raw [--once]    # --once: drain what is currently in Kafka, then exit
"""
import argparse
import json

from confluent_kafka import Consumer

from ..common import config
from ..common.store import Store

TOPIC_TO_TABLE = {config.TOPIC_CAISO_FUEL: "caiso_fuelmix", config.TOPIC_CAISO_DEMAND: "caiso_demand",
                  config.TOPIC_EIA: "eia_fuel", config.TOPIC_NWS: "nws"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--batch", type=int, default=20000)
    a = ap.parse_args()
    c = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": "raw-loader",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe(list(TOPIC_TO_TABLE))
    store, buf, idle, got_any = Store(), {t: [] for t in TOPIC_TO_TABLE.values()}, 0, False

    def flush():
        for table, rows in buf.items():
            store.insert_raw(table, rows)
            if rows:
                print(f"loaded {len(rows)} rows -> {table}", flush=True)
            rows.clear()
        c.commit()

    while True:
        msg = c.poll(1.0)
        if msg is None:
            idle += 1
            if idle >= 5 and any(buf.values()):
                flush()
            if a.once and idle >= (10 if got_any else 60):
                flush()
                break
            continue
        if msg.error():
            continue
        idle, got_any = 0, True
        buf[TOPIC_TO_TABLE[msg.topic()]].append(json.loads(msg.value()))
        if sum(len(v) for v in buf.values()) >= a.batch:
            flush()


if __name__ == "__main__":
    main()
