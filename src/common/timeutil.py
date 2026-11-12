"""CAISO publishes Pacific wall-clock times (with DST); EIA-930 is UTC. Everything is normalised to UTC here."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

PACIFIC = ZoneInfo("America/Los_Angeles")


def pacific_to_utc(day: str, hhmm: str) -> datetime:
    """day='2026-03-08', hhmm='14:05' -> aware UTC datetime. Handles DST transitions via zoneinfo."""
    naive = datetime.strptime(f"{day} {hhmm}", "%Y-%m-%d %H:%M")
    return naive.replace(tzinfo=PACIFIC).astimezone(timezone.utc)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
