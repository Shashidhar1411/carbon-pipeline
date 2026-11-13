"""Show CAISO's own fuel-mix and CO2 rows side by side around a time of day.
    python -m eval.inspect_interval --date 2026-05-11 --from 14:20 --to 14:50
"""
import argparse
import csv
import io
from datetime import date

import requests

from src.common.kafka_util import http_get
from src.producers.caiso_producer import day_url


def rows(session, d, filename):
    resp = http_get(session, day_url(d, filename))
    return {r["Time"].strip(): r for r in csv.DictReader(io.StringIO(resp.text))} if resp is not None else {}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--from", dest="t0", default="00:00")
    ap.add_argument("--to", dest="t1", default="23:55")
    a = ap.parse_args()
    d, s = date.fromisoformat(a.date), requests.Session()
    fuel, co2 = rows(s, d, "fuelsource.csv"), rows(s, d, "co2.csv")
    num = lambda x: float(x) if x not in (None, "") else float("nan")
    print(f"{'time':6} {'gasMW':>7} {'importsMW':>9} {'solarMW':>8} | {'CAISO gasCO2':>12} {'CAISO impCO2':>12} {'CAISO total t/h':>16}")
    for t in sorted(fuel):
        if a.t0 <= t <= a.t1 and t in co2:
            f, c = fuel[t], co2[t]
            total = sum(num(v) for k, v in c.items() if k.strip() != "Time" and k)
            print(f"{t:6} {num(f['Natural Gas']):7.0f} {num(f['Imports']):9.0f} {num(f['Solar']):8.0f} | "
                  f"{num(c['Natural Gas CO2']):12.0f} {num(c['Imports CO2']):12.0f} {total:16.1f}")
