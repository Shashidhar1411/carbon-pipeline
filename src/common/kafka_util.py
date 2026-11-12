import json
import time
from . import config


def make_producer():
    from confluent_kafka import Producer          # lazy: parsing/testing code does not need the client
    return Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "linger.ms": 50, "compression.type": "lz4"})


def publish(p, topic: str, key: str, value: dict):
    while True:
        try:
            p.produce(topic, key=key, value=json.dumps(value))
            p.poll(0)
            return
        except BufferError:           # local queue full -> let it drain
            p.poll(0.5)


def http_get(session, url, retries=4, **kw):
    """GET with exponential backoff; returns the Response or None for 404 (e.g. a day not yet published)."""
    for i in range(retries):
        r = session.get(url, timeout=60, **kw)
        if r.status_code == 404:
            return None
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(2 ** i)
            continue
        r.raise_for_status()
        return r
    raise RuntimeError(f"giving up on {url}")
