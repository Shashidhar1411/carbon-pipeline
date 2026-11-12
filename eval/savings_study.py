"""Carbon-savings study: simulated EV charging (charge-on-arrival vs. cleanest hours in the parking window) using the
REAL hourly intensity history, plus the logged Airflow job runs. Applies k-anonymity / DP before producing the
shareable tables.   python -m eval.savings_study
"""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from src.common import config
from src.common.store import Store
from src.forecasting import methods
from src.privacy import anonymize
from src.simulation import ev

if __name__ == "__main__":
    st = Store()
    hist = methods.prepare(st.hourly())
    ci_days = {d: dict(zip(g["local_hour"], g["ci"])) for d, g in hist.groupby("local_date") if g["local_hour"].nunique() >= 20}
    sessions = ev.simulate_sessions(sorted(ci_days))
    rec = []
    for s in sessions.itertuples():
        b, c = ev.session_emissions(ci_days[s.date.date()], s)
        rec.append((b, c))
    sessions[["baseline_kg", "shifted_kg"]] = rec
    tot_b, tot_s = sessions.baseline_kg.sum(), sessions.shifted_kg.sum()
    print(f"EV (simulated): {len(sessions)} sessions, {sessions.energy_kwh.sum():,.0f} kWh | baseline {tot_b:,.1f} kg -> shifted {tot_s:,.1f} kg "
          f"| saved {tot_b - tot_s:,.1f} kg ({(tot_b - tot_s) / tot_b:.1%})")
    daily = sessions.groupby("date")[["baseline_kg", "shifted_kg"]].sum()
    daily.to_csv(config.DATA_DIR / "ev_savings_daily.csv")
    ax = daily.rolling(7).mean().plot(figsize=(7, 3.2)); ax.set_ylabel("kg CO2/day (7-day mean)"); ax.set_title("Simulated EV charging: on-arrival vs carbon-aware")
    plt.tight_layout(); plt.savefig(config.DATA_DIR / "ev_savings.png", dpi=200)
    # privacy-preserving release
    gen = anonymize.generalise(sessions)
    kept, supp = anonymize.k_anonymize(gen, k=5)
    print(f"k-anonymity (k=5): suppressed {supp:.1%} of records; smallest group = {anonymize.min_group_size(kept)}")
    anonymize.dp_hourly_totals(sessions, epsilon=1.0, seed=1).to_csv(config.DATA_DIR / "ev_hourly_dp.csv", index=False)
    runs = pd.DataFrame(st.query(f"SELECT * FROM {st._t('job_run')}"))
    if len(runs):
        print(f"Airflow job runs: {len(runs)} | kg avoided = {runs.kg_saved.sum():.2f} | modes: {runs['mode'].value_counts().to_dict()}")
