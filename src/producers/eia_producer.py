"""EIA-930 hourly generation by fuel type -> Kafka.

  python -m src.producers.eia_producer backfill --start 2026-01-01 --end 2026-09-30                  # every balancing authority
  python -m src.producers.eia_producer backfill --start 2026-01-01 --end 2026-09-30 --respondent CISO,BANC,LDWP
  python -m src.producers.eia_producer live

The API returns at most 5000 rows per request and is slow for big queries, so the backfill asks for small date
windows (--chunk-days, default 3), pages each window by offset, and flushes to Kafka after every window.
"""
import argparse
import time
from datetime import date, timedelta

import requests

from ..common import config
from ..common.kafka_util import http_get, make_producer, publish
from ..common.timeutil import now_utc


def windows(start: date, end: date, days: int):
    d = start
    while d <= end:
        e = min(d + timedelta(days=days - 1), end)
        yield d, e
        d = e + timedelta(days=1)


def fetch_pages(session, start: str, end: str, respondents=None):
    offset = 0
    while True:
        params = {"api_key": config.EIA_API_KEY, "frequency": "hourly", "data[0]": "value",
                  "start": f"{start}T00", "end": f"{end}T23", "sort[0][column]": "period", "sort[0][direction]": "asc",
                  "offset": offset, "length": config.EIA_PAGE}
        if respondents:
            params["facets[respondent][]"] = respondents        # a list -> the parameter is repeated
        rows = http_get(session, config.EIA_URL, params=params).json()["response"]["data"]
        yield rows
        offset += len(rows)
        if len(rows) < config.EIA_PAGE:
            return
        time.sleep(0.3)             # stay polite to the API


def send(p, rows):
    for r in rows:
        r["period_utc"] = r["period"] + ":00:00Z"            # EIA 'period' is UTC, format YYYY-MM-DDTHH
        publish(p, config.TOPIC_EIA, f"{r['respondent']}|{r['fueltype']}|{r['period']}", r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["backfill", "live"])
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default=date.today().isoformat())
    ap.add_argument("--respondent", default=None, help="comma list, e.g. CISO,BANC ; omit for every balancing authority")
    ap.add_argument("--chunk-days", type=int, default=3)
    a = ap.parse_args()
    if not config.EIA_API_KEY:
        raise SystemExit("Set EIA_API_KEY first")
    resp = a.respondent.split(",") if a.respondent else None
    s, p, total = requests.Session(), make_producer(), 0
    if a.mode == "backfill":
        for d0, d1 in windows(date.fromisoformat(a.start), date.fromisoformat(a.end), a.chunk_days):
            n = 0
            for rows in fetch_pages(s, d0.isoformat(), d1.isoformat(), resp):
                send(p, rows)
                n += len(rows)
            p.flush()
            total += n
            print(f"{d0} .. {d1}: {n} rows (total {total})", flush=True)
    else:
        while True:                                                # hourly refresh of the last 2 days
            y = (now_utc() - timedelta(days=2)).date().isoformat()
            for rows in fetch_pages(s, y, now_utc().date().isoformat(), resp or ["CISO"]):
                send(p, rows)
            p.flush()
            time.sleep(3600)


if __name__ == "__main__":
    main()
