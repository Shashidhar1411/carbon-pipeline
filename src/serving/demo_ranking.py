"""SYNTHETIC scenario for the Airflow demo (clearly not real forecast data).

Writes a separate Redis key 'carbon:ranking_demo' in which the CURRENT hour looks dirty and the NEXT hour looks clean, so the
sensor visibly waits (at most an hour) and then releases the job. The real ranking is not touched.

    python -m src.serving.demo_ranking
"""
from datetime import datetime, timedelta, timezone

from ..common import config
from . import redis_io
from .scheduler_logic import iso

if __name__ == "__main__":
    r = redis_io.client()
    hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    scores = {iso(hour): 300.0, iso(hour + timedelta(hours=1)): 40.0}
    for i in range(2, 6):
        scores[iso(hour + timedelta(hours=i))] = 250.0 + i
    key = config.KEY_RANKING + "_demo"
    r.delete(key)
    r.zadd(key, scores)
    print(f"synthetic ranking written to {key}: dirty now ({iso(hour)} = 300), clean next hour ({iso(hour + timedelta(hours=1))} = 40)")
