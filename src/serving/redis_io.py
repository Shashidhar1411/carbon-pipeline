from datetime import datetime, timezone

import redis
from ..common import config


def client():
    return redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)


def set_current(r, row: dict, extra: dict = None):
    mapping = {k: ("" if v is None else str(v)) for k, v in {**row, **(extra or {})}.items()}
    r.hset(config.KEY_CURRENT, mapping=mapping)


def set_ranking(r, hour_to_ci: dict, meta: dict = None):
    """Replace the 24-hour ranking: sorted set, lowest score (cleanest) first. meta = when/how it was made."""
    pipe = r.pipeline()
    pipe.delete(config.KEY_RANKING)
    if hour_to_ci:
        pipe.zadd(config.KEY_RANKING, {h: float(v) for h, v in hour_to_ci.items()})
    pipe.delete(config.KEY_RANKING_META)
    if meta:
        pipe.hset(config.KEY_RANKING_META, mapping={k: str(v) for k, v in meta.items()})
    pipe.execute()


def cleanest(r, n=4, future_only=True, now=None):
    """The n cleanest hours. By default only hours that have not finished yet (the current hour still counts)."""
    items = [(m, s) for m, s in r.zrange(config.KEY_RANKING, 0, -1, withscores=True)]
    if future_only:
        floor = (now or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:00:00Z")
        items = [(m, s) for m, s in items if m >= floor]          # ISO strings sort like timestamps
    return items[:n]


def ranking_meta(r):
    return r.hgetall(config.KEY_RANKING_META)


def current(r):
    return r.hgetall(config.KEY_CURRENT)
