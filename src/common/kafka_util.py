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


def http_get(session, url, retries=6, timeout=180, **kw):
    """GET with exponential backoff; returns the Response, or None for 404 (e.g. a day not yet published)."""
    import requests
    for i in range(retries):
        try:
            r = session.get(url, timeout=timeout, **kw)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            print(f"  retry {i + 1}/{retries} after {type(e).__name__}", flush=True)
            time.sleep(min(2 ** i, 30))
            continue
        if r.status_code == 404:
            return None
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(min(2 ** i, 30))
            continue
        r.raise_for_status()
        return r
    raise RuntimeError(f"giving up on {url}")
