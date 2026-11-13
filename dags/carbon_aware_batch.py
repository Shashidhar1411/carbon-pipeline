"""Carbon-aware batch job (proposal section 4.5).

A sensor reads the Redis 24-hour ranking and releases the job when the current hour is among the `top_n` cleanest hours
inside the job's allowed window (window_hours from the planned start). If the window is about to close the job runs anyway
('deadline'), so work is never starved. Every run is logged to fact_job_run with the kg CO2 avoided versus running at the
planned time.

Trigger it manually with "Trigger DAG w/ config". Parameters (defaults below):
  window_hours 12 | top_n 3 | ranking_key carbon:ranking | job_id nightly_gpu_job | energy_kwh 20
For a nightly run set schedule="0 1 * * *" below.
"""
import sys
import time
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator

sys.path.append("/opt/project")                                  # repo is mounted here by docker-compose
from src.common import config                                    # noqa: E402
from src.common.store import Store                               # noqa: E402
from src.serving import redis_io                                 # noqa: E402
from src.serving.scheduler_logic import decide, iso              # noqa: E402


class CleanWindowSensor(BaseSensorOperator):
    def poke(self, context):
        p = context["params"]
        ranking = redis_io.client().zrange(p["ranking_key"], 0, -1, withscores=True)
        planned = context["data_interval_end"].astimezone(timezone.utc)
        mode, best = decide(ranking, planned, datetime.now(timezone.utc), int(p["window_hours"]), int(p["top_n"]))
        if mode is None:
            self.log.info("WAITING - this hour is not among the %s cleanest hours of the window; cleanest: %s", p["top_n"], best)
            return False
        context["ti"].xcom_push(key="mode", value=mode)
        self.log.info("RELEASING job (%s); cleanest hours in window: %s", mode, best)
        return True


def run_job(**ctx):
    time.sleep(5)                      # stand-in for the real work (GPU training, lab data crunching, ...)
    return "done"


def log_run(**ctx):
    p = ctx["params"]
    r, store = redis_io.client(), Store()
    now = datetime.now(timezone.utc)
    planned = ctx["data_interval_end"].astimezone(timezone.utc)
    ci_run = store.ci_at(iso(now))                                 # mean intensity of the current hour so far
    if ci_run is None:
        ci_run = float(redis_io.current(r).get("ci") or 0)
    ci_base = store.ci_at(iso(planned))                            # what running at the planned time would have cost
    energy = float(p["energy_kwh"])
    saved = energy * (ci_base - ci_run) / 1000 if ci_base is not None else None
    store.insert_job_run({"job_id": p["job_id"], "run_id": ctx["run_id"], "planned_start": iso(planned), "actual_start": iso(now),
                          "energy_kwh": energy, "ci_at_run": ci_run, "ci_at_baseline": ci_base, "kg_saved": saved,
                          "mode": ctx["ti"].xcom_pull(task_ids="wait_for_clean_window", key="mode")})


with DAG("carbon_aware_batch", start_date=datetime(2026, 11, 1), schedule=None, catchup=False,
         default_args={"retries": 0}, tags=["carbon", "data226"],
         params={"window_hours": 12, "top_n": 3, "ranking_key": config.KEY_RANKING,
                 "job_id": "nightly_gpu_job", "energy_kwh": 20.0}) as dag:
    wait = CleanWindowSensor(task_id="wait_for_clean_window", mode="reschedule", poke_interval=60, timeout=13 * 3600)
    work = PythonOperator(task_id="run_batch_job", python_callable=run_job)
    log = PythonOperator(task_id="log_run", python_callable=log_run)
    wait >> work >> log
