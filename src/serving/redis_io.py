import redis
from ..common import config


def client():
    return redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, decode_responses=True)


def set_current(r, row: dict, extra: dict = None):
    mapping = {k: ("" if v is None else str(v)) for k, v in {**row, **(extra or {})}.items()}
    r.hset(config.KEY_CURRENT, mapping=mapping)


def set_ranking(r, hour_to_ci: dict):
    """Replace the 24-hour ranking: sorted set, lowest score (cleanest) first."""
    pipe = r.pipeline()
    pipe.delete(config.KEY_RANKING)
    if hour_to_ci:
        pipe.zadd(config.KEY_RANKING, {h: float(v) for h, v in hour_to_ci.items()})
    pipe.execute()


def cleanest(r, n=4):
    return [(m, s) for m, s in r.zrange(config.KEY_RANKING, 0, n - 1, withscores=True)]


def current(r):
    return r.hgetall(config.KEY_CURRENT)
