"""Recompute carbon intensity for one Pacific day straight from CAISO's file and write it to the warehouse.
Use it after bad data was ingested for that day.

    python -m src.loaders.repair_day --date 2026-11-13

It refuses to run while CAISO's file is stale (rows from the future), so it can never write wrong data itself.
"""
import argparse
from datetime import date, timedelta

import requests

from ..common.emissions import carbon_intensity
from ..common.store import Store
from ..common.timeutil import iso_utc, now_utc
from ..producers.caiso_producer import fetch_fuel_day


def build_rows(recs):
    now, rows = iso_utc(now_utc()), []
    for r in recs:
        c = carbon_intensity(r["fuels"])
        if c["ci"] is None:
            continue
        rows.append({"interval_utc": r["interval_utc"], "ci": c["ci"], "total_mw": c["total_mw"],
                     "renewable_share": c["renewable_share"],
                     "wind_mw": max(r["fuels"].get("Wind") or 0.0, 0.0), "solar_mw": max(r["fuels"].get("Solar") or 0.0, 0.0),
                     "revision_ts": now, "updated_at": now})
    return rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    a = ap.parse_args()
    recs = fetch_fuel_day(requests.Session(), date.fromisoformat(a.date))
    limit = iso_utc(now_utc() + timedelta(minutes=10))
    if not recs or any(r["interval_utc"] > limit for r in recs):
        raise SystemExit("CAISO's file is missing or still stale (rows from the future) - try again in a few minutes")
    rows = build_rows(recs)
    Store().upsert_carbon(rows)
    print(f"repaired {len(rows)} intervals for {a.date}: {rows[0]['interval_utc']} .. {rows[-1]['interval_utc']}")
