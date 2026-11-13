"""Forecast inputs for the net-load method: CAISO day-ahead demand (Today's Outlook demand.csv) and the day-ahead
wind/solar forecast (CAISO OASIS SLD_REN_FCST, best effort). If OASIS is unreachable, wind+solar fall back to the
same hour yesterday, and the returned `source` string says so - report that honestly."""
import csv
import io
import zipfile
import pandas as pd
import requests

from ..common.kafka_util import http_get
from ..producers.caiso_producer import fetch_demand_day

OASIS = "https://oasis.caiso.com/oasisapi/SingleZip"


def day_ahead_demand(session, days) -> dict:
    acc = {}
    for d in days:
        for r in fetch_demand_day(session, d):
            if r["day_ahead_mw"] is not None:
                acc.setdefault(pd.Timestamp(r["interval_utc"]).floor("h"), []).append(r["day_ahead_mw"])
    return {h: sum(v) / len(v) for h, v in acc.items()}


def oasis_renewables(session, start: pd.Timestamp, end: pd.Timestamp) -> dict:
    """Day-ahead market wind+solar MW per hour (summed over trading hubs). Returns {} on any failure."""
    params = {"queryname": "SLD_REN_FCST", "market_run_id": "DAM", "version": 1, "resultformat": 6,
              "startdatetime": start.strftime("%Y%m%dT%H:%M-0000"), "enddatetime": end.strftime("%Y%m%dT%H:%M-0000")}
    try:
        resp = http_get(session, OASIS, params=params)
        z = zipfile.ZipFile(io.BytesIO(resp.content))
        rows = list(csv.DictReader(io.TextIOWrapper(z.open(z.namelist()[0]))))
        acc = {}
        for r in rows:
            if r.get("RENEWABLE_TYPE") in ("Solar", "Wind"):
                h = pd.Timestamp(r["INTERVALSTARTTIME_GMT"]).tz_convert("UTC") if pd.Timestamp(r["INTERVALSTARTTIME_GMT"]).tzinfo else pd.Timestamp(r["INTERVALSTARTTIME_GMT"], tz="UTC")
                acc[h] = acc.get(h, 0.0) + float(r.get("MW") or r.get("VALUE") or 0)
        return acc
    except Exception as e:                               # noqa: BLE001 - best effort by design
        print("OASIS unavailable:", e)
        return {}


def net_load_forecast(hist: pd.DataFrame, targets):
    """Returns ({hour: forecast net-load MW}, source_label)."""
    sess = requests.Session()
    days = sorted({t.tz_convert("America/Los_Angeles").date() for t in targets})
    demand = day_ahead_demand(sess, days)
    ren = oasis_renewables(sess, min(targets), max(targets) + pd.Timedelta(hours=1))
    source = "CAISO day-ahead demand minus OASIS wind/solar forecast"
    if not ren:                                              # fallback: wind+solar same hour yesterday
        by = hist.set_index("hour_utc")["ws_mw"]
        ren = {t: by.get(t - pd.Timedelta(hours=24)) for t in targets}
        source = "CAISO day-ahead demand minus wind/solar persistence (OASIS unavailable)"
    nl = {t: demand[t] - ren[t] for t in targets if t in demand and ren.get(t) is not None and not pd.isna(ren[t])}
    # Hours without a day-ahead number (e.g. tomorrow's hours before CAISO publishes it): net-load persistence,
    # i.e. the same hour 1, 2 or 7 days earlier. Keeps the ranking complete; the source label says how many.
    by_nl = hist.set_index("hour_utc")["net_load"]
    filled = 0
    for t in targets:
        if t in nl:
            continue
        for lag in (24, 48, 168):
            v = by_nl.get(t - pd.Timedelta(hours=lag))
            if v is not None and not pd.isna(v):
                nl[t] = float(v)
                filled += 1
                break
    if filled:
        source += f"; {filled} of {len(targets)} hours use net-load persistence"
    return nl, source
