"""EIA-930 hourly generation by fuel type (all balancing authorities by default) -> Kafka.

  export EIA_API_KEY=...   (free: https://www.eia.gov/opendata/)
  python -m src.producers.eia_producer backfill --start 2026-01-01 --end 2026-09-30 [--respondent CISO]
  python -m src.producers.eia_producer live

The API returns at most 5000 rows per request, so each month is paged with offset/length.
"""
import argparse
import time
from datetime import date, timedelta

import requests

from ..common import config
from ..common.kafka_util import http_get, make_producer, publish
from ..common.timeutil import now_utc


def month_ranges(start: date, end: date):
    d = start
    while d <= end:
        nxt = (d.replace(day=1) + timedelta(days=32)).replace(day=1)
        yield d, min(nxt - timedelta(days=1), end)
        d = nxt


def fetch_pages(session, start: str, end: str, respondent=None):
    offset = 0
    while True:
        params = {"api_key": config.EIA_API_KEY, "frequency": "hourly", "data[0]": "value",
                  "start": f"{start}T00", "end": f"{end}T23", "sort[0][column]": "period", "sort[0][direction]": "asc",
                  "offset": offset, "length": config.EIA_PAGE}
        if respondent:
            params["facets[respondent][]"] = respondent
        body = http_get(session, config.EIA_URL, params=params).json()["response"]
        rows = body["data"]
        yield rows
        offset += len(rows)
        if len(rows) < config.EIA_PAGE:
            return
        time.sleep(0.3)             # stay polite to the API


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["backfill", "live"])
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default=date.today().isoformat())
    ap.add_argument("--respondent", default=None, help="e.g. CISO for California only; omit for every BA")
    a = ap.parse_args()
    if not config.EIA_API_KEY:
        raise SystemExit("Set EIA_API_KEY first")
    s, p = requests.Session(), make_producer()
    if a.mode == "backfill":
        for m0, m1 in month_ranges(date.fromisoformat(a.start), date.fromisoformat(a.end)):
            n = 0
            for rows in fetch_pages(s, m0.isoformat(), m1.isoformat(), a.respondent):
                for r in rows:
                    r["period_utc"] = r["period"] + ":00:00Z"     # EIA 'period' is UTC, format YYYY-MM-DDTHH
                    publish(p, config.TOPIC_EIA, f"{r['respondent']}|{r['fueltype']}|{r['period']}", r)
                    n += 1
            p.flush()
            print(f"{m0:%Y-%m}: {n} rows", flush=True)
    else:
        while True:                                                # hourly refresh of the last 2 days
            y = (now_utc() - timedelta(days=2)).date().isoformat()
            for rows in fetch_pages(s, y, now_utc().date().isoformat(), a.respondent or "CISO"):
                for r in rows:
                    r["period_utc"] = r["period"] + ":00:00Z"
                    publish(p, config.TOPIC_EIA, f"{r['respondent']}|{r['fueltype']}|{r['period']}", r)
            p.flush()
            time.sleep(3600)


if __name__ == "__main__":
    main()
