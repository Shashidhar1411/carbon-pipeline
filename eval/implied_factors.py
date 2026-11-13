"""Emission factors implied by CAISO's own files: per fuel, CO2 (t/h) * 1000 / generation (MW) = kg CO2 per MWh.
Median over many intervals (robust to glitches).   python -m eval.implied_factors --every 7
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
from src.producers.caiso_producer import day_url


def table(session, d, filename):
    resp = http_get(session, day_url(d, filename))
    return {r["Time"].strip(): r for r in csv.DictReader(io.StringIO(resp.text))} if resp is not None else {}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default="2026-09-30")
    ap.add_argument("--every", type=int, default=7, help="sample every Nth day")
    ap.add_argument("--min-mw", type=float, default=50.0)
    a = ap.parse_args()
    s, ratios, d = requests.Session(), {}, date.fromisoformat(a.start)
    while d <= date.fromisoformat(a.end):
        fuel, co2 = table(s, d, "fuelsource.csv"), table(s, d, "co2.csv")
        for t, crow in co2.items():
            frow = fuel.get(t)
            if not frow:
                continue
            for col, val in crow.items():
                if not col or col.strip() == "Time" or not col.strip().endswith(" CO2"):
                    continue
                name = col.strip()[:-4]
                try:
                    mw, tons = float(frow[name]), float(val)
                except (KeyError, ValueError):
                    continue
                if mw >= a.min_mw and tons > 0:
                    ratios.setdefault(name, []).append(tons * 1000.0 / mw)
        print(d, flush=True)
        d += timedelta(days=a.every)
    out = pd.DataFrame({k: {"n": len(v), "median_kg_per_mwh": np.median(v), "p10": np.percentile(v, 10), "p90": np.percentile(v, 90)}
                        for k, v in ratios.items()}).T.round(1)
    print("\nCAISO-implied emission factors (kg CO2 / MWh)\n", out.to_string())
    out.to_csv(config.DATA_DIR / "implied_factors.csv")
