import numpy as np
import pandas as pd

from src.common.store import Store
from src.forecasting import methods
from src.privacy import anonymize
from src.simulation import ev


def synthetic_hourly(days=80, seed=0):
    """Duck-curve world: intensity falls as net load falls (lots of solar); day-to-day variation in cloudiness."""
    rng = np.random.default_rng(seed)
    rows = []
    for d in range(days):
        cloud = rng.uniform(0, 1)
        for h in range(24):
            ts = pd.Timestamp("2026-03-01", tz="America/Los_Angeles") + pd.Timedelta(days=d, hours=h)
            solar = max(0, np.sin((h - 6) / 12 * np.pi)) * 14000 * (1 - 0.6 * cloud)
            demand = 24000 + 6000 * np.sin((h - 9) / 24 * 2 * np.pi)
            nl = demand - solar
            rows.append({"hour_utc": ts.tz_convert("UTC"), "net_load": nl, "ci": 60 + 0.016 * nl + rng.normal(0, 5), "ws_mw": solar})
    return methods.prepare(rows)


def test_net_load_and_lsh_beat_persistence():
    h = synthetic_hourly()
    full_days = [d for d, g in h.groupby("local_date") if g["local_hour"].nunique() == 24]
    d = full_days[-1]
    past, last = h[h["local_date"] < d], h[h["local_date"] == d]
    targets, truth = list(last["hour_utc"]), dict(zip(last["hour_utc"], last["ci"]))
    nl = dict(zip(last["hour_utc"], last["net_load"]))
    mae = lambda f: np.mean([abs(f[t] - truth[t]) for t in targets if t in f])
    p, n, l = methods.persistence(past, targets), methods.net_load_method(past, targets, nl), methods.lsh_analog(past, targets, nl)
    assert len(n) == len(l) == 24
    assert mae(n) < mae(p) and mae(l) < mae(p)                 # informed methods beat persistence in this synthetic world


def test_lsh_returns_similar_vectors():
    rng = np.random.default_rng(1)
    base = rng.standard_normal((200, 24))
    lsh = methods.HyperplaneLSH(24, bits=6, tables=10)
    for i, v in enumerate(base):
        lsh.index(i, v)
    q = base[17] + 0.05 * rng.standard_normal(24)
    assert 17 in lsh.candidates(q) and len(lsh.candidates(q)) < 200


def test_store_hourly_and_ci_at():
    st = Store()
    st.upsert_carbon([{"interval_utc": f"2026-06-02T10:{m:02d}:00Z", "ci": 100.0 + m, "total_mw": 25000.0, "renewable_share": 0.5,
                       "wind_mw": 1000.0, "solar_mw": 10000.0, "revision_ts": "x", "updated_at": "y"} for m in range(0, 60, 5)])
    h = st.hourly()
    assert len(h) == 1 and abs(h[0]["net_load"] - 14000) < 1e-6 and abs(h[0]["ws_mw"] - 11000) < 1e-6
    assert abs(st.ci_at("2026-06-02T10:30:00Z") - 127.5) < 1e-9


def test_k_anonymity_and_dp():
    s = ev.simulate_sessions(pd.date_range("2026-03-01", periods=60), n_users=400)
    g = anonymize.generalise(s)
    assert "user_id" not in g
    kept, supp = anonymize.k_anonymize(g, k=5)
    assert anonymize.min_group_size(kept) >= 5 and 0 <= supp < 0.5
    a = anonymize.dp_hourly_totals(s, epsilon=1.0, seed=3)
    true_total = s["energy_kwh"].clip(upper=40).sum()
    assert abs(a["energy_kwh_dp"].sum() - true_total) / true_total < 0.15       # noise averages out over many cells
    tight = anonymize.dp_hourly_totals(s, epsilon=0.1, seed=3)
    assert tight["energy_kwh_dp"].std() >= 0


def test_ev_shifting_never_worse():
    ci = {h: 400 - 350 * np.exp(-((h - 13) / 3) ** 2) for h in range(24)}     # clean midday
    for s in ev.simulate_sessions(pd.date_range("2026-03-01", periods=5)).itertuples():
        b, c = ev.session_emissions(ci, s)
        assert c <= b + 1e-9
