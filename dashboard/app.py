"""Streamlit dashboard:  streamlit run dashboard/app.py
Live intensity + 24-hour ranking come from Redis; history and carbon saved come from the warehouse."""
import pandas as pd
import streamlit as st

from src.common.store import Store
from src.forecasting import methods
from src.serving import redis_io

st.set_page_config(page_title="California Grid Carbon", layout="wide")
st.title("Real-Time California Grid Carbon Pipeline")
st.caption("Average carbon intensity of CAISO generation (kg CO2/MWh), computed from the public fuel mix every 5 minutes.")


@st.cache_resource
def get_store():
    return Store()


@st.cache_data(ttl=60)
def load_hourly():
    return methods.prepare(get_store().hourly())


@st.cache_data(ttl=300)
def load_jobs():
    s = get_store()
    return pd.DataFrame(s.query(f"SELECT * FROM {s._t('job_run')}"))


r = redis_io.client()
try:
    cur = redis_io.current(r)
    rank = redis_io.cleanest(r, 24)
except Exception as e:                                         # Redis down -> still show history
    cur, rank = {}, []
    st.warning(f"Redis not reachable ({e}); showing warehouse data only.")

c1, c2, c3, c4 = st.columns(4)
if cur:
    ci = float(cur["ci"])
    c1.metric("Carbon intensity now", f"{ci:,.0f} kg/MWh")
    c2.metric("Renewable share", f"{float(cur['renewable_share']):.0%}")
    c3.metric("Interval (Pacific)", pd.Timestamp(cur["interval_utc"]).tz_convert("America/Los_Angeles").strftime("%a %H:%M"))
    c4.metric("Dirty intervals, last 24h (DGIM)", cur.get("dirty_intervals_24h_dgim", "-"))
else:
    st.info("No live value yet - is the Spark job running?")

left, right = st.columns(2)
with left:
    st.subheader("Next 24 hours, cleanest first")
    if rank:
        df = pd.DataFrame(rank, columns=["hour_utc", "forecast_kg_per_mwh"])
        df["hour_pt"] = pd.to_datetime(df["hour_utc"]).dt.tz_convert("America/Los_Angeles").dt.strftime("%a %H:%M")
        st.dataframe(df[["hour_pt", "forecast_kg_per_mwh"]].round(0), hide_index=True, width="stretch")
        st.bar_chart(df.sort_values("hour_utc").set_index("hour_pt")["forecast_kg_per_mwh"])
    else:
        st.info("Ranking not published yet (run src.forecasting.run_forecast).")
with right:
    st.subheader("Typical day (average by Pacific hour)")
    h = load_hourly()
    if len(h):
        st.line_chart(h.groupby("local_hour")["ci"].mean())

st.subheader("History")
h = load_hourly()
if len(h):
    days = st.slider("Days to show", 3, 60, 14)
    recent = h[h["hour_utc"] >= h["hour_utc"].max() - pd.Timedelta(days=days)]
    st.line_chart(recent.set_index("hour_utc")["ci"])

st.subheader("Carbon saved by shifting load")
jobs = load_jobs()
if len(jobs):
    a, b = st.columns(2)
    a.metric("Scheduled job runs", len(jobs))
    b.metric("kg CO2 avoided (jobs)", f"{jobs['kg_saved'].dropna().sum():.2f}")
    st.dataframe(jobs.tail(20), hide_index=True, width="stretch")
else:
    st.info("No scheduled job runs logged yet.")
try:
    ev = pd.read_csv("data/ev_savings_daily.csv", parse_dates=["date"]).set_index("date")
    st.caption("Simulated EV charging: on-arrival vs. carbon-aware (kg CO2/day)")
    st.line_chart(ev.rolling(7).mean())
except FileNotFoundError:
    st.caption("Run `python -m eval.savings_study` to add the simulated EV chart.")
