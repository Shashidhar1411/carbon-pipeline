"""National Weather Service hourly temperature + sky cover forecast -> Kafka (hourly).
NWS requires a descriptive User-Agent; set NWS_USER_AGENT."""
import re
import time
from datetime import datetime, timedelta, timezone

import requests

from ..common import config
from ..common.kafka_util import http_get, make_producer, publish
from ..common.timeutil import iso_utc, now_utc


def expand(values, name):
    """NWS gridpoint series use validTime = '2026-05-01T18:00:00+00:00/PT3H'; expand to hourly points."""
    out = {}
    for v in values:
        start_s, dur = v["validTime"].split("/")
        start = datetime.fromisoformat(start_s).astimezone(timezone.utc)
        h = int(re.search(r"PT(\d+)H", dur).group(1)) if "PT" in dur and "H" in dur else 1
        for i in range(h):
            out[iso_utc(start + timedelta(hours=i))] = v["value"]
    return out


def fetch_forecast(session):
    hdr = {"User-Agent": config.NWS_USER_AGENT, "Accept": "application/geo+json"}
    pt = http_get(session, f"https://api.weather.gov/points/{config.NWS_LAT},{config.NWS_LON}", headers=hdr).json()
    grid = http_get(session, pt["properties"]["forecastGridData"], headers=hdr).json()["properties"]
    temp, sky = expand(grid["temperature"]["values"], "temp"), expand(grid["skyCover"]["values"], "sky")
    issued = iso_utc(now_utc())
    return [{"target_hour_utc": h, "temperature_c": temp.get(h), "sky_cover_pct": sky.get(h), "issued_at": issued}
            for h in sorted(set(temp) & set(sky))]


def main():
    s, p = requests.Session(), make_producer()
    while True:
        rows = fetch_forecast(s)
        for r in rows:
            publish(p, config.TOPIC_NWS, r["target_hour_utc"], r)
        p.flush()
        print(f"{now_utc().isoformat()} published {len(rows)} weather hours", flush=True)
        time.sleep(3600)


if __name__ == "__main__":
    main()
