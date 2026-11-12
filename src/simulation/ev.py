"""Simulated campus EV charging sessions (no real charger data is available - see proposal risk table) and the
savings calculation: charge on arrival (baseline) vs. charge in the cleanest hours inside the parking window."""
import numpy as np
import pandas as pd


def simulate_sessions(days, n_users=200, seed=42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = []
    buildings = ["Library", "Engineering", "Student Union", "Parking Garage A", "Parking Garage B"]
    for d in days:
        for u in range(n_users):
            if rng.random() > 0.35:                    # roughly a third of users charge on a given day
                continue
            arrive = int(np.clip(rng.normal(9, 2.5), 6, 18))
            park = int(np.clip(rng.normal(8, 2), 3, 12))
            rows.append({"user_id": f"U{u:04d}", "date": pd.Timestamp(d), "building": rng.choice(buildings),
                         "arrive_hour": arrive, "depart_hour": min(arrive + park, 23),
                         "energy_kwh": float(np.clip(rng.normal(14, 6), 4, 40)), "power_kw": 7.2})
    return pd.DataFrame(rows)


def session_emissions(ci_by_local_hour: dict, s) -> tuple:
    """Returns (baseline_kg, shifted_kg). ci_by_local_hour: {hour 0-23: kg/MWh} for the session's date."""
    need = int(np.ceil(s.energy_kwh / s.power_kw))
    window = [h for h in range(s.arrive_hour, s.depart_hour) if h in ci_by_local_hour]
    if len(window) < need or need == 0:
        return 0.0, 0.0
    kwh_per_h = s.energy_kwh / need
    base = sum(ci_by_local_hour[h] for h in window[:need]) * kwh_per_h / 1000
    shifted = sum(sorted(ci_by_local_hour[h] for h in window)[:need]) * kwh_per_h / 1000
    return base, shifted
