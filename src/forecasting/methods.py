"""Three explainable next-day forecasts of hourly carbon intensity (no model training):

1. persistence  - tomorrow at hour h = today at hour h.
2. net_load     - forecast net load (demand - wind - solar) -> mean intensity of the k historical hours with the same
                  Pacific hour-of-day and the closest net load.
3. lsh_analog   - Locality Sensitive Hashing (random hyperplanes) over 24-hour net-load profiles finds historical days
                  that look like the forecast day; the forecast is the mean intensity profile of those analog days.
`hourly` is a DataFrame with columns hour_utc (tz-aware UTC), ci, net_load.
"""
import numpy as np
import pandas as pd

PAC = "America/Los_Angeles"


def prepare(rows) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["hour_utc", "ci", "net_load", "local_date", "local_hour"])
    df["hour_utc"] = pd.to_datetime(df["hour_utc"], utc=True)
    loc = df["hour_utc"].dt.tz_convert(PAC)
    df["local_date"], df["local_hour"] = loc.dt.date, loc.dt.hour
    return df.dropna(subset=["ci"]).sort_values("hour_utc").reset_index(drop=True)


def persistence(hist: pd.DataFrame, targets) -> dict:
    by_hour = hist.set_index("hour_utc")["ci"]
    out = {}
    for t in targets:
        for lag in (24, 48, 168):                      # fall back to 2 days / 1 week ago if yesterday is missing
            v = by_hour.get(t - pd.Timedelta(hours=lag))
            if v is not None and not np.isnan(v):
                out[t] = float(v)
                break
    return out


def net_load_method(hist: pd.DataFrame, targets, nl_forecast: dict, k: int = 15) -> dict:
    out = {}
    for t in targets:
        if t not in nl_forecast:
            continue
        h = t.tz_convert(PAC).hour
        cand = hist[hist["local_hour"] == h].dropna(subset=["net_load"])
        if cand.empty:
            continue
        near = cand.iloc[(cand["net_load"] - nl_forecast[t]).abs().argsort()[:k]]
        out[t] = float(near["ci"].mean())
    return out


class HyperplaneLSH:
    """L hash tables, each hashing a vector to a b-bit signature (sign of dot product with random hyperplanes)."""
    def __init__(self, dim, bits=6, tables=8, seed=0):
        rng = np.random.default_rng(seed)
        self.planes = [rng.standard_normal((bits, dim)) for _ in range(tables)]
        self.tables = [dict() for _ in range(tables)]

    def _sig(self, planes, v):
        return tuple((planes @ v > 0).astype(int))

    def index(self, key, v):
        for planes, tab in zip(self.planes, self.tables):
            tab.setdefault(self._sig(planes, v), []).append(key)

    def candidates(self, v):
        found = set()
        for planes, tab in zip(self.planes, self.tables):
            found.update(tab.get(self._sig(planes, v), []))
        return found


def _profile(series_by_hour: dict, fill: np.ndarray) -> np.ndarray:
    return np.array([series_by_hour.get(h, fill[h]) for h in range(24)], dtype=float)


def lsh_analog(hist: pd.DataFrame, targets, nl_forecast: dict, k: int = 7, seed: int = 0) -> dict:
    d = hist.dropna(subset=["net_load"])
    full = d.groupby("local_date").filter(lambda g: g["local_hour"].nunique() == 24)
    if full["local_date"].nunique() < k:
        return {}
    days = {}
    for date_, g in full.groupby("local_date"):
        g = g.drop_duplicates("local_hour").set_index("local_hour")
        days[date_] = (g["net_load"].reindex(range(24)).to_numpy(), g["ci"].reindex(range(24)).to_numpy())
    mean_nl = np.nanmean([v[0] for v in days.values()], axis=0)
    norm = lambda v: (v - v.mean()) / (v.std() + 1e-9)       # shape of the day, not its absolute level
    lsh = HyperplaneLSH(24, seed=seed)
    for date_, (nl, _) in days.items():
        lsh.index(date_, norm(nl))
    out = {}
    for tdate in sorted({t.tz_convert(PAC).date() for t in targets}):
        fc = {t.tz_convert(PAC).hour: nl_forecast[t] for t in targets if t.tz_convert(PAC).date() == tdate and t in nl_forecast}
        if not fc:
            continue
        q = _profile(fc, mean_nl)
        cands = lsh.candidates(norm(q)) or set(days)           # empty bucket -> fall back to scanning all days
        ranked = sorted(cands, key=lambda dd: np.linalg.norm(norm(days[dd][0]) - norm(q)))[:k]
        ci_profile = np.nanmean([days[dd][1] for dd in ranked], axis=0)
        for t in targets:
            if t.tz_convert(PAC).date() == tdate:
                out[t] = float(ci_profile[t.tz_convert(PAC).hour])
    return out


def top_hours(forecast: dict, n=4):
    return [t for t, _ in sorted(forecast.items(), key=lambda kv: kv[1])[:n]]
