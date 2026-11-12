"""Backtest the three forecasters on the warehouse history (proposal 4.6: net-load must beat persistence).

For each held-out day D the history before D is used. Net-load inputs are the REALISED net load of D with Gaussian
noise (--noise, default 5%) standing in for day-ahead forecast error - state this clearly in the report, or replace
with stored day-ahead forecasts once enough have accumulated.

    python -m eval.forecast_eval --noise 0.05
"""
import argparse

import numpy as np
import pandas as pd

from src.common import config
from src.common.store import Store
from src.forecasting import methods

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--noise", type=float, default=0.05)
    ap.add_argument("--min-history-days", type=int, default=30)
    a = ap.parse_args()
    hist = methods.prepare(Store().hourly())
    rng = np.random.default_rng(0)
    days = sorted(hist["local_date"].unique())[a.min_history_days:]
    res = {m: {"mae": [], "top4": []} for m in ("persistence", "net_load", "lsh_analog")}
    for d in days:
        day = hist[hist["local_date"] == d]
        if day["local_hour"].nunique() < 24:
            continue
        past = hist[hist["local_date"] < d]
        targets = list(day["hour_utc"])
        truth = dict(zip(day["hour_utc"], day["ci"]))
        nl = {t: v * (1 + rng.normal(0, a.noise)) for t, v in zip(day["hour_utc"], day["net_load"]) if not np.isnan(v)}
        fc = {"persistence": methods.persistence(past, targets), "net_load": methods.net_load_method(past, targets, nl),
              "lsh_analog": methods.lsh_analog(past, targets, nl)}
        true4 = set(methods.top_hours(truth))
        for m, f in fc.items():
            common = [t for t in targets if t in f]
            if len(common) < 20:
                continue
            res[m]["mae"].append(np.mean([abs(f[t] - truth[t]) for t in common]))
            res[m]["top4"].append(len(set(methods.top_hours({t: f[t] for t in common})) & true4) / 4)
    out = pd.DataFrame({m: {"days": len(v["mae"]), "MAE kg/MWh": np.mean(v["mae"]), "top-4 overlap": np.mean(v["top4"])} for m, v in res.items()}).T
    print(out.round(3)); out.to_csv(config.DATA_DIR / "forecast_eval.csv")
