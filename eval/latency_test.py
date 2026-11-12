"""Freshness/latency test (proposal 4.6, target < 10 min).

Samples the Redis 'carbon:current' hash and records lag = updated_at - interval end (interval_utc + 5 min).
The interval end is the earliest moment CAISO could have published that interval, so this is a conservative
(upper-bound) measure of 'publish to value in Redis'. Run it while the live producer + Spark job are running:

    python -m eval.latency_test --minutes 60
"""
import argparse
import csv
import time
from datetime import datetime, timedelta, timezone

import numpy as np

from src.common import config
from src.serving import redis_io


def p(s): return datetime.fromisoformat(s.replace("Z", "+00:00"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=60)
    a = ap.parse_args()
    r, seen, rows = redis_io.client(), set(), []
    end = time.time() + a.minutes * 60
    while time.time() < end:
        cur = redis_io.current(r)
        if cur and cur["interval_utc"] not in seen and cur.get("updated_at"):
            seen.add(cur["interval_utc"])
            lag = (p(cur["updated_at"]) - (p(cur["interval_utc"]) + timedelta(minutes=5))).total_seconds() / 60
            rows.append((cur["interval_utc"], cur["updated_at"], round(lag, 2)))
            print(rows[-1], flush=True)
        time.sleep(10)
    out = config.DATA_DIR / "latency_results.csv"
    with open(out, "w", newline="") as f:
        w = csv.writer(f); w.writerow(["interval_utc", "updated_at", "lag_minutes"]); w.writerows(rows)
    lags = np.array([x[2] for x in rows])
    if len(lags):
        print(f"n={len(lags)} median={np.median(lags):.1f} p95={np.percentile(lags, 95):.1f} max={lags.max():.1f} min; "
              f"target <10: {'PASS' if np.percentile(lags, 95) < 10 else 'CHECK'}  -> {out}")
