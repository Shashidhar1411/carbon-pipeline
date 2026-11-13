"""Late/revised data check (proposal 4.6: share of revised records merged into the correct window, target 100%).

Recomputes every interval of one Pacific day from CAISO's file and compares it with what the warehouse holds.

    python -m eval.revision_check --date 2026-11-13

Run it before and after `python -m src.loaders.repair_day --date ...` (the daily reconciliation job): the first number is
what the live stream got right on its own (revisions older than the 30-minute watermark are dropped by Spark), the
second shows the reconciliation merging all revisions.
"""
import argparse
from datetime import date, timedelta

import requests

from src.common.store import Store
from src.common.timeutil import iso_utc, now_utc
from src.loaders.repair_day import build_rows
from src.producers.caiso_producer import fetch_fuel_day

TOL = 0.01          # kg CO2/MWh


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    a = ap.parse_args()
    recs = fetch_fuel_day(requests.Session(), date.fromisoformat(a.date))
    if not recs or any(r["interval_utc"] > iso_utc(now_utc() + timedelta(minutes=10)) for r in recs):
        raise SystemExit("CAISO's file is missing or stale (rows from the future) - try again in a few minutes")
    truth = {r["interval_utc"]: r["ci"] for r in build_rows(recs)}
    st = Store()
    key = lambda x: x if isinstance(x, str) else x.strftime("%Y-%m-%dT%H:%M:%SZ")      # Snowflake: datetime, SQLite: text
    lo, hi = min(truth), max(truth)
    ours = {}
    for r in st.query("SELECT interval_utc, ci FROM " + st._t("carbon_interval")):
        k = key(r["interval_utc"])
        if lo <= k <= hi:
            ours[k] = r["ci"]
    missing = [k for k in truth if k not in ours]
    differs = [k for k in truth if k in ours and ours[k] is not None and abs(ours[k] - truth[k]) > TOL]
    ok = len(truth) - len(missing) - len(differs)
    print(f"{a.date}: {len(truth)} intervals in CAISO's file | warehouse matches {ok} ({ok / len(truth):.1%}) | "
          f"differs {len(differs)} | missing {len(missing)}")
    for k in sorted(differs, key=lambda k: -abs(ours[k] - truth[k]))[:5]:
        print(f"   {k}  warehouse {ours[k]:.1f}  CAISO file {truth[k]:.1f}  (diff {ours[k] - truth[k]:+.1f} kg/MWh)")
