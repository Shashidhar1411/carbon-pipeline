"""Issue the next-24-hour forecast with all three methods, store them (fact_forecast) and publish the ranking of the
chosen method to Redis.

  python -m src.forecasting.run_forecast            # once
  python -m src.forecasting.run_forecast --loop     # hourly
"""
import argparse
import time

import pandas as pd

from ..common.store import Store
from ..common.timeutil import iso_utc, now_utc
from ..serving import redis_io
from . import methods
from .inputs import net_load_forecast


def issue(store, r, primary="net_load"):
    hist = methods.prepare(store.hourly())
    if len(hist) < 48:
        print("not enough history yet")
        return
    start = pd.Timestamp(now_utc()).ceil("h")
    targets = [start + pd.Timedelta(hours=i) for i in range(24)]
    nl, source = net_load_forecast(hist, targets)
    fc = {"persistence": methods.persistence(hist, targets),
          "net_load": methods.net_load_method(hist, targets, nl),
          "lsh_analog": methods.lsh_analog(hist, targets, nl)}
    issued = iso_utc(now_utc())
    store.insert_forecasts([{"issued_at": issued, "target_hour": iso_utc(t), "method": m, "forecast_ci": v}
                            for m, d in fc.items() for t, v in d.items()])
    chosen = fc[primary] or fc["persistence"]
    redis_io.set_ranking(r, {iso_utc(t): v for t, v in chosen.items()})
    print(f"{issued}: net-load inputs = {source}; cleanest hours:",
          [(iso_utc(t), round(chosen[t])) for t in methods.top_hours(chosen)])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--primary", default="net_load", choices=["persistence", "net_load", "lsh_analog"])
    a = ap.parse_args()
    st, rc = Store(), redis_io.client()
    while True:
        issue(st, rc, a.primary)
        if not a.loop:
            break
        time.sleep(3600)
