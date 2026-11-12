"""Small read-only endpoint campus systems can poll.   uvicorn src.serving.api:app --port 8000
  GET /current          -> latest intensity
  GET /cleanest?n=4     -> the n cleanest upcoming hours (from the Redis sorted set)
"""
from fastapi import FastAPI, HTTPException
from . import redis_io

app = FastAPI(title="California Grid Carbon Signal", version="1.0")
r = redis_io.client()


@app.get("/health")
def health():
    return {"redis": r.ping()}


@app.get("/current")
def current():
    cur = redis_io.current(r)
    if not cur:
        raise HTTPException(503, "no value yet")
    return cur


@app.get("/cleanest")
def cleanest(n: int = 4):
    return [{"hour_utc": h, "forecast_kg_per_mwh": round(s, 1)} for h, s in redis_io.cleanest(r, max(1, min(n, 24)))]
