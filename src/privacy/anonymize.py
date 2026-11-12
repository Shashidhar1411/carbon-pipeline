"""Privacy for the (simulated) campus EV-charging data: individual sessions identify people by arrival time, building
and energy use. Before anything reaches the dashboard we apply
  * generalisation + k-anonymity over the quasi-identifiers (building, arrival-time bucket, energy band), and
  * the Laplace mechanism (epsilon-differential privacy) on hourly per-building energy totals.
"""
import numpy as np
import pandas as pd


def generalise(df: pd.DataFrame, hour_bucket=4, kwh_band=10) -> pd.DataFrame:
    g = df.drop(columns=["user_id"]).copy()                     # direct identifier removed
    g["arrive_bucket"] = (g["arrive_hour"] // hour_bucket) * hour_bucket
    g["energy_band"] = (g["energy_kwh"] // kwh_band) * kwh_band
    return g


def k_anonymize(df: pd.DataFrame, k=5, quasi=("building", "arrive_bucket", "energy_band")):
    """Suppress every record whose quasi-identifier combination occurs fewer than k times. Returns (table, suppressed_fraction)."""
    quasi = list(quasi)
    size = df.groupby(quasi)["building"].transform("size")
    kept = df[size >= k]
    return kept, 1 - len(kept) / max(len(df), 1)


def min_group_size(df, quasi=("building", "arrive_bucket", "energy_band")) -> int:
    return int(df.groupby(list(quasi)).size().min()) if len(df) else 0


def dp_hourly_totals(df: pd.DataFrame, epsilon=1.0, clip_kwh=40.0, seed=None) -> pd.DataFrame:
    """Per-building, per-arrival-hour energy totals with Laplace noise. One user contributes at most one session per
    day here and sessions are clipped to clip_kwh, so the L1 sensitivity of each total is clip_kwh."""
    rng = np.random.default_rng(seed)
    clipped = df.assign(energy_kwh=df["energy_kwh"].clip(upper=clip_kwh))
    tot = clipped.groupby(["building", "arrive_hour"], as_index=False)["energy_kwh"].sum()
    tot["energy_kwh_dp"] = (tot["energy_kwh"] + rng.laplace(0, clip_kwh / epsilon, len(tot))).clip(lower=0)
    return tot.drop(columns="energy_kwh")
