"""Accuracy check (proposal 4.6, target: within 10% on average of CAISO's own CO2 estimate).

    python -m eval.accuracy_check --start 2026-09-01 --end 2026-09-30 [--units tons_per_interval]

FIRST open https://www.caiso.com/outlook/SP/History/<yyyymmdd>/co2.csv (or whatever file Today's Outlook uses for
emissions - set CAISO_CO2_FILE) and check the column and the units. CAISO reports total emissions; we convert our
intensity to the same quantity: tons = ci[kg/MWh] * total_mw * (5/60) h / 1000 per 5-minute interval.
"""
import argparse
import csv
import io
from datetime import date, timedelta

import numpy as np
import pandas as pd
import requests

from src.common import config
from src.common.kafka_util import http_get
from src.common.store import Store
from src.producers.caiso_producer import _parse_times, _num
from src.common.timeutil import iso_utc

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True); ap.add_argument("--end", required=True)
    ap.add_argument("--units", default="tons_per_interval", choices=["tons_per_interval", "tons_per_hour"])
    a = ap.parse_args()
    s, st = requests.Session(), Store()
    theirs = {}
    d = date.fromisoformat(a.start)
    while d <= date.fromisoformat(a.end):
        resp = http_get(s, f"{config.CAISO_BASE}/{d:%Y%m%d}/{config.CAISO_CO2_FILE}")
        if resp is not None:
            rows = list(csv.DictReader(io.StringIO(resp.text)))
            col = [c for c in rows[0] if c.strip() != "Time"][0]
            if d.isoformat() == a.start:
                print("using CAISO column:", col, "| sample:", rows[0])
            for utc, r in _parse_times(d.isoformat(), rows):
                v = _num(r[col])
                if v is not None:
                    theirs[iso_utc(utc)] = v
        d += timedelta(days=1)
    ours = {r["interval_utc"]: r for r in st.query("SELECT interval_utc, ci, total_mw FROM " + st._t("carbon_interval"))}
    scale = 1 / 12 if a.units == "tons_per_hour" else 1      # tons/h -> tons per 5 min
    recs = []
    for k, v in theirs.items():
        o = ours.get(k)
        if o and o["ci"] is not None and v > 0:
            mine = o["ci"] * o["total_mw"] * (5 / 60) / 1000
            recs.append((k, mine, v * scale, abs(mine - v * scale) / (v * scale)))
    df = pd.DataFrame(recs, columns=["interval_utc", "ours_tons", "caiso_tons", "abs_pct_err"])
    df.to_csv(config.DATA_DIR / "accuracy_results.csv", index=False)
    print(f"{len(df)} intervals compared | MAPE = {df.abs_pct_err.mean():.1%} | median = {df.abs_pct_err.median():.1%} | "
          f"mean signed bias = {((df.ours_tons - df.caiso_tons).sum() / df.caiso_tons.sum()):+.1%} | target <= 10%: "
          f"{'PASS' if df.abs_pct_err.mean() <= 0.10 else 'NOT MET - report honestly and explain (factors, imports)'}")
