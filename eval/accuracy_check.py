"""Accuracy check (proposal 4.6, target: within 10% on average of CAISO's own CO2 estimate).

    python -m eval.accuracy_check --start 2026-01-01 --end 2026-09-30 [--skip-every 7] [--tag before]

CAISO's co2.csv has one column per fuel (metric tons CO2 per hour). Its 'Imports CO2' goes NEGATIVE when California
exports power, so CAISO's total is a NET figure that can be near zero or negative on sunny middays. We treat exports
like battery charging (load, not generation) and never count negative imports. Two comparisons are therefore reported:
  net_as_published : CAISO's total exactly as published
  gross            : the same total with negative columns (export credit) set to zero  <- like-for-like with our method
--skip-every N skips every Nth day starting at --start: the days eval.implied_factors used to measure factors, so the
accuracy is measured on days that were NOT used for calibration.
"""
import argparse
import csv
import io
from datetime import date, timedelta

import pandas as pd
import requests

from src.common import config
from src.common.kafka_util import http_get
from src.common.store import Store
from src.common.timeutil import iso_utc
from src.producers.caiso_producer import _num, _parse_times, day_url


def summarise(ours, theirs, scale, label, out_csv):
    recs = []
    for k, v in theirs.items():
        o = ours.get(k)
        if o and o["ci"] is not None and v > 0:
            mine = o["ci"] * o["total_mw"] * (5 / 60) / 1000          # tons per 5-minute interval
            ref = v * scale
            recs.append((k, mine, ref, abs(mine - ref) / ref))
    df = pd.DataFrame(recs, columns=["interval_utc", "ours_tons", "caiso_tons", "abs_pct_err"])
    df.to_csv(out_csv, index=False)
    diff = df.ours_tons - df.caiso_tons
    mape, wape, bias = df.abs_pct_err.mean(), diff.abs().sum() / df.caiso_tons.sum(), diff.sum() / df.caiso_tons.sum()
    print(f"[{label}] {len(df)} intervals | MAPE = {mape:.1%} | median = {df.abs_pct_err.median():.1%} | WAPE = {wape:.1%} | "
          f"bias = {bias:+.1%} | MAPE target <= 10%: {'PASS' if mape <= 0.10 else 'NOT MET'}  -> {out_csv.name}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--units", default="tons_per_hour", choices=["tons_per_interval", "tons_per_hour"])
    ap.add_argument("--skip-every", type=int, default=0, help="skip days used for calibration (every Nth day from --start)")
    ap.add_argument("--tag", default="", help="label added to the output file names, e.g. before / after")
    a = ap.parse_args()
    s, st, net, gross = requests.Session(), Store(), {}, {}
    d, i, end = date.fromisoformat(a.start), 0, date.fromisoformat(a.end)
    while d <= end:
        if not (a.skip_every and i % a.skip_every == 0):
            resp = http_get(s, day_url(d, config.CAISO_CO2_FILE))
            if resp is not None:
                rows = list(csv.DictReader(io.StringIO(resp.text)))
                cols = [c for c in rows[0] if c and c.strip() != "Time"]
                if d.isoformat() == a.start:
                    print("summing CAISO CO2 columns:", cols)
                for utc, r in _parse_times(d.isoformat(), rows):
                    vals = [x for x in (_num(r.get(c)) for c in cols) if x is not None]
                    if vals:
                        net[iso_utc(utc)] = sum(vals)
                        gross[iso_utc(utc)] = sum(max(x, 0.0) for x in vals)
        d, i = d + timedelta(days=1), i + 1
    # Snowflake returns datetime objects, SQLite returns text: normalise both to 'YYYY-MM-DDTHH:MM:SSZ'
    key = lambda x: x if isinstance(x, str) else x.strftime("%Y-%m-%dT%H:%M:%SZ")
    ours = {key(r["interval_utc"]): r for r in st.query("SELECT interval_utc, ci, total_mw FROM " + st._t("carbon_interval"))}
    scale = 1 / 12 if a.units == "tons_per_hour" else 1                     # tons/h -> tons per 5 min
    tag = f"{a.tag}_" if a.tag else ""
    summarise(ours, net, scale, "net_as_published", config.DATA_DIR / f"accuracy_results_{tag}net_as_published.csv")
    summarise(ours, gross, scale, "gross (export credit removed)", config.DATA_DIR / f"accuracy_results_{tag}gross.csv")
