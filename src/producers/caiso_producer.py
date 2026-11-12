"""CAISO Today's Outlook -> Kafka.

  python -m src.producers.caiso_producer backfill --start 2026-01-01 --end 2026-09-30
  python -m src.producers.caiso_producer live            # polls every 5 minutes

One message per 5-minute interval: {"interval_utc", "fuels": {fuel: MW}, "revision_ts", "status"}.
A Bloom-filter RecordClassifier labels each interval NEW / REVISED and drops exact duplicates, so re-polling the
whole day's file every 5 minutes does not flood Kafka, while CAISO revisions still get through.
"""
import argparse
import csv
import io
import json
import time
from datetime import date, datetime, timedelta, timezone

import requests

from ..common import config
from ..common.kafka_util import http_get, make_producer, publish
from ..common.streaming_algos import RecordClassifier
from ..common.timeutil import iso_utc, now_utc, PACIFIC


def _parse_times(day: str, rows, time_col="Time"):
    """Yield (utc_datetime, row). Handles the repeated 01:xx hour on the fall DST change (fold=1 once time goes backwards)."""
    prev, fold = None, 0
    for r in rows:
        hhmm = (r.get(time_col) or "").strip()
        if not hhmm:
            continue
        naive = datetime.strptime(f"{day} {hhmm}", "%Y-%m-%d %H:%M")
        if prev is not None and naive < prev:
            fold = 1
        prev = naive
        yield naive.replace(tzinfo=PACIFIC, fold=fold).astimezone(timezone.utc), r


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def fetch_fuel_day(session, d: date):
    url = f"{config.CAISO_BASE}/{d:%Y%m%d}/{config.CAISO_FUEL_FILE}"
    resp = http_get(session, url)
    if resp is None:
        return []
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    out = []
    for utc, r in _parse_times(d.isoformat(), rows):
        fuels = {k.strip(): _num(v) for k, v in r.items() if k and k.strip() != "Time" and _num(v) is not None}
        if fuels:
            out.append({"interval_utc": iso_utc(utc), "fuels": fuels})
    return out


def fetch_demand_day(session, d: date):
    url = f"{config.CAISO_BASE}/{d:%Y%m%d}/{config.CAISO_DEMAND_FILE}"
    resp = http_get(session, url)
    if resp is None:
        return []
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    return [{"interval_utc": iso_utc(utc), "day_ahead_mw": _num(r.get("Day ahead forecast")),
             "hour_ahead_mw": _num(r.get("Hour ahead forecast")), "demand_mw": _num(r.get("Current demand"))}
            for utc, r in _parse_times(d.isoformat(), rows)]


def run_day(session, producer, clf, d: date, stats):
    for rec in fetch_fuel_day(session, d):
        status = clf.classify(rec["interval_utc"], json.dumps(rec["fuels"], sort_keys=True))
        stats[status] += 1
        if status == "duplicate":
            continue
        rec.update(status=status, revision_ts=iso_utc(now_utc()))
        publish(producer, config.TOPIC_CAISO_FUEL, rec["interval_utc"], rec)
    for rec in fetch_demand_day(session, d):
        publish(producer, config.TOPIC_CAISO_DEMAND, rec["interval_utc"], rec)   # demand is overwritten downstream, no dedupe needed
    producer.flush()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["backfill", "live"])
    ap.add_argument("--start", default="2026-01-01")
    ap.add_argument("--end", default=date.today().isoformat())
    a = ap.parse_args()
    s, p, clf = requests.Session(), make_producer(), RecordClassifier()
    stats = {"new": 0, "revised": 0, "duplicate": 0}
    if a.mode == "backfill":
        d, end = date.fromisoformat(a.start), date.fromisoformat(a.end)
        while d <= end:
            run_day(s, p, clf, d, stats)
            print(d, stats, flush=True)
            d += timedelta(days=1)
    else:
        while True:
            today = now_utc().astimezone(PACIFIC).date()
            run_day(s, p, clf, today, stats)                       # also re-checks yesterday around midnight
            if now_utc().astimezone(PACIFIC).hour == 0:
                run_day(s, p, clf, today - timedelta(days=1), stats)
            print(now_utc().isoformat(), stats, flush=True)
            time.sleep(300)


if __name__ == "__main__":
    main()
