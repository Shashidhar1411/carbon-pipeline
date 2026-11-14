"""The carbon-aware release decision, kept free of Airflow so it can be unit-tested."""
from datetime import datetime, timedelta, timezone


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_utc(dt) -> datetime:
    """A plain Python UTC datetime. Airflow passes its own pendulum date objects, which do not compare reliably with
    ordinary datetimes; naive values are taken to be UTC."""
    if dt.tzinfo is None:
        return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, dt.microsecond, tzinfo=timezone.utc)
    return datetime.fromtimestamp(dt.timestamp(), tz=timezone.utc)


def parse(h: str) -> datetime:
    return datetime.fromisoformat(h.replace("Z", "+00:00"))


def decide(ranking, planned: datetime, now: datetime, window_hours: int, top_n: int):
    """ranking: [(hour_iso, forecast_kg_per_mwh)]. Returns (mode, best_hours).
    mode is 'clean_window' (current hour is among the top_n cleanest hours of the job's window),
    'deadline' (window about to close: run anyway so work is never starved) or None (keep waiting)."""
    planned, now = to_utc(planned), to_utc(now)
    start = planned.replace(minute=0, second=0, microsecond=0)          # the hour that contains the planned time counts
    deadline = planned + timedelta(hours=window_hours)
    in_window = [(h, s) for h, s in ranking if start <= parse(h) < deadline]
    best = [h for h, _ in sorted(in_window, key=lambda x: x[1])[:top_n]]
    if iso(now.replace(minute=0, second=0, microsecond=0)) in best:
        return "clean_window", best
    if now >= deadline - timedelta(hours=1):
        return "deadline", best
    return None, best
