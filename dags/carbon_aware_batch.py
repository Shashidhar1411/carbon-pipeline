"""Carbon-aware batch job (proposal section 4.5).

A Sensor reads the Redis 24-hour ranking and only releases the job when the current hour is among the `top_n`
cleanest hours inside the job's allowed window. If the window is about to close the job runs anyway ('deadline'),
so work is never starved. Each run is logged to fact_job_run with the kg CO2 avoided vs. running at planned time.
"""
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator

import sys
sys.path.append("/opt/project")                                  # repo is mounted here by docker-compose
from src.common import config                                    # noqa: E402
from src.common.store import Store                               # noqa: E402
from src.serving import redis_io                                 # noqa: E402


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


class CleanWindowSensor(BaseSensorOperator):
    def __init__(self, window_hours=12, top_n=3, **kw):
        super().__init__(**kw)
        self.window_hours, self.top_n = window_hours, top_n

    def poke(self, context):
        now = datetime.now(timezone.utc)
        planned = context["data_interval_end"]
        deadline = planned + timedelta(hours=self.window_hours)
        r = redis_io.client()
        ranking = r.zrange(config.KEY_RANKING, 0, -1, withscores=True)
        in_window = [(h, s) for h, s in ranking if planned <= datetime.fromisoformat(h.replace("Z", "+00:00")) < deadline]
        this_hour = iso(now.replace(minute=0, second=0, microsecond=0))
        best = [h for h, _ in sorted(in_window, key=lambda x: x[1])[: self.top_n]]
        if this_hour in best:
            mode = "clean_window"
        elif now >= deadline - timedelta(hours=1):
            mode = "deadline"
        else:
            self.log.info("waiting; cleanest hours in window: %s", best)
            return False
        context["ti"].xcom_push(key="mode", value=mode)
        self.log.info("releasing job (%s)", mode)
        return True


def run_job(**ctx):
    import time
    time.sleep(5)                      # stand-in for the real work (GPU training, lab data crunching, ...)
    return "done"


def log_run(energy_kwh=20.0, job_id="nightly_gpu_job", **ctx):
    r, store = redis_io.client(), Store()
    now = datetime.now(timezone.utc)
    planned = ctx["data_interval_end"].astimezone(timezone.utc)
    ci_run = float(redis_io.current(r).get("ci") or store.ci_at(iso(now)) or 0)
    ci_base = store.ci_at(iso(planned))
    saved = energy_kwh * (ci_base - ci_run) / 1000 if ci_base is not None else None
    store.insert_job_run({"job_id": job_id, "run_id": ctx["run_id"], "planned_start": iso(planned), "actual_start": iso(now),
                          "energy_kwh": energy_kwh, "ci_at_run": ci_run, "ci_at_baseline": ci_base, "kg_saved": saved,
                          "mode": ctx["ti"].xcom_pull(task_ids="wait_for_clean_window", key="mode")})


with DAG("carbon_aware_batch", start_date=datetime(2026, 11, 1), schedule="0 1 * * *", catchup=False,
         default_args={"retries": 0}, tags=["carbon", "data226"]) as dag:
    wait = CleanWindowSensor(task_id="wait_for_clean_window", mode="reschedule", poke_interval=300,
                             timeout=13 * 3600, window_hours=12, top_n=3)
    work = PythonOperator(task_id="run_batch_job", python_callable=run_job)
    log = PythonOperator(task_id="log_run", python_callable=log_run)
    wait >> work >> log
