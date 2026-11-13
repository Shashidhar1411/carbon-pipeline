"""The carbon-aware release decision, kept free of Airflow so it can be unit-tested."""
from datetime import datetime, timedelta, timezone


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(h: str) -> datetime:
    return datetime.fromisoformat(h.replace("Z", "+00:00"))


def decide(ranking, planned: datetime, now: datetime, window_hours: int, top_n: int):
    """ranking: [(hour_iso, forecast_kg_per_mwh)]. Returns (mode, best_hours).
    mode is 'clean_window' (current hour is among the top_n cleanest hours of the job's window),
    'deadline' (window about to close: run anyway so work is never starved) or None (keep waiting)."""
    start = planned.replace(minute=0, second=0, microsecond=0)          # the hour that contains the planned time counts
    deadline = planned + timedelta(hours=window_hours)
    in_window = [(h, s) for h, s in ranking if start <= parse(h) < deadline]
    best = [h for h, _ in sorted(in_window, key=lambda x: x[1])[:top_n]]
    if iso(now.replace(minute=0, second=0, microsecond=0)) in best:
        return "clean_window", best
    if now >= deadline - timedelta(hours=1):
        return "deadline", best
    return None, best
