"""Warehouse access. Uses Snowflake when SNOWFLAKE_ACCOUNT is set, otherwise a local SQLite file so the
pipeline can be developed and unit-tested without credentials. dbt only runs against Snowflake."""
import json
import sqlite3
from . import config

CARBON_COLS = ["interval_utc", "ci", "total_mw", "renewable_share", "wind_mw", "solar_mw", "revision_ts", "updated_at"]

SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS raw_carbon_interval(interval_utc TEXT PRIMARY KEY, ci REAL, total_mw REAL,
  renewable_share REAL, wind_mw REAL, solar_mw REAL, revision_ts TEXT, updated_at TEXT);
CREATE TABLE IF NOT EXISTS raw_caiso_fuelmix(payload TEXT);
CREATE TABLE IF NOT EXISTS raw_caiso_demand(payload TEXT);
CREATE TABLE IF NOT EXISTS raw_eia_fuel(payload TEXT);
CREATE TABLE IF NOT EXISTS raw_nws(payload TEXT);
CREATE TABLE IF NOT EXISTS raw_forecast(issued_at TEXT, target_hour TEXT, method TEXT, forecast_ci REAL);
CREATE TABLE IF NOT EXISTS raw_job_run(job_id TEXT, run_id TEXT, planned_start TEXT, actual_start TEXT,
  energy_kwh REAL, ci_at_run REAL, ci_at_baseline REAL, kg_saved REAL, mode TEXT);
"""
SNOWFLAKE_DDL = [
    "CREATE SCHEMA IF NOT EXISTS RAW",
    """CREATE TABLE IF NOT EXISTS RAW.CARBON_INTERVAL(INTERVAL_UTC TIMESTAMP_NTZ PRIMARY KEY, CI FLOAT, TOTAL_MW FLOAT,
       RENEWABLE_SHARE FLOAT, WIND_MW FLOAT, SOLAR_MW FLOAT, REVISION_TS TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ)""",
    "CREATE TABLE IF NOT EXISTS RAW.CAISO_FUELMIX(PAYLOAD VARIANT)",
    "CREATE TABLE IF NOT EXISTS RAW.CAISO_DEMAND(PAYLOAD VARIANT)",
    "CREATE TABLE IF NOT EXISTS RAW.EIA_FUEL(PAYLOAD VARIANT)",
    "CREATE TABLE IF NOT EXISTS RAW.NWS(PAYLOAD VARIANT)",
    "CREATE TABLE IF NOT EXISTS RAW.FORECAST(ISSUED_AT TIMESTAMP_NTZ, TARGET_HOUR TIMESTAMP_NTZ, METHOD STRING, FORECAST_CI FLOAT)",
    """CREATE TABLE IF NOT EXISTS RAW.JOB_RUN(JOB_ID STRING, RUN_ID STRING, PLANNED_START TIMESTAMP_NTZ, ACTUAL_START TIMESTAMP_NTZ,
       ENERGY_KWH FLOAT, CI_AT_RUN FLOAT, CI_AT_BASELINE FLOAT, KG_SAVED FLOAT, MODE STRING)""",
]
TS = lambda s: s.replace("T", " ").replace("Z", "") if isinstance(s, str) else s   # Snowflake NTZ-friendly


class Store:
    def __init__(self):
        self.snow = bool(config.SF["account"])
        if self.snow:
            import snowflake.connector
            kw = {k: v for k, v in config.SF.items() if v}
            self.conn = snowflake.connector.connect(**kw)
            cur = self.conn.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {config.SF['database']}")
            cur.execute(f"USE DATABASE {config.SF['database']}")
            for stmt in SNOWFLAKE_DDL:
                cur.execute(stmt)
        else:
            self.conn = sqlite3.connect(config.SQLITE_PATH)
            self.conn.executescript(SQLITE_DDL)

    # ---- generic helpers -------------------------------------------------------------------------------
    def _t(self, name):                 # logical name -> physical table
        return f"RAW.{name.upper()}" if self.snow else f"raw_{name.lower()}"

    def query(self, sql, params=()):
        cur = self.conn.cursor()
        cur.execute(sql, params)
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def execute(self, sql, params=()):
        cur = self.conn.cursor()
        cur.execute(sql, params)
        if not self.snow:
            self.conn.commit()

    # ---- writes ----------------------------------------------------------------------------------------
    def upsert_carbon(self, rows):
        """Idempotent: a revised interval replaces the earlier row (matched on interval_utc)."""
        for r in rows:
            vals = [r.get(c) for c in CARBON_COLS]
            if self.snow:
                vals = [TS(v) if c in ("interval_utc", "revision_ts", "updated_at") else v for c, v in zip(CARBON_COLS, vals)]
                self.execute(
                    f"""MERGE INTO {self._t('carbon_interval')} t USING (SELECT %s INTERVAL_UTC,%s CI,%s TOTAL_MW,%s RENEWABLE_SHARE,
                        %s WIND_MW,%s SOLAR_MW,%s REVISION_TS,%s UPDATED_AT) s ON t.INTERVAL_UTC=s.INTERVAL_UTC
                        WHEN MATCHED THEN UPDATE SET CI=s.CI,TOTAL_MW=s.TOTAL_MW,RENEWABLE_SHARE=s.RENEWABLE_SHARE,
                        WIND_MW=s.WIND_MW,SOLAR_MW=s.SOLAR_MW,REVISION_TS=s.REVISION_TS,UPDATED_AT=s.UPDATED_AT
                        WHEN NOT MATCHED THEN INSERT VALUES (s.INTERVAL_UTC,s.CI,s.TOTAL_MW,s.RENEWABLE_SHARE,s.WIND_MW,s.SOLAR_MW,
                        s.REVISION_TS,s.UPDATED_AT)""", vals)
            else:
                self.execute(f"INSERT OR REPLACE INTO raw_carbon_interval VALUES ({','.join('?'*len(CARBON_COLS))})", vals)

    def insert_raw(self, name, rows):
        """Bulk-append JSON documents to a payload-only raw table. Snowflake: PUT to the table stage + COPY INTO
        (fast for hundreds of thousands of rows); SQLite: executemany."""
        if not rows:
            return
        t = self._t(name)
        if not self.snow:
            self.conn.executemany(f"INSERT INTO {t} VALUES (?)", [(json.dumps(r),) for r in rows])
            self.conn.commit()
            return
        import os, tempfile, uuid
        path = os.path.join(tempfile.gettempdir(), f"{name}_{uuid.uuid4().hex}.json")
        with open(path, "w") as f:
            f.write("\n".join(json.dumps(r) for r in rows))
        cur = self.conn.cursor()
        cur.execute(f"PUT file://{path} @%{t.split('.')[-1]} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        cur.execute(f"COPY INTO {t} FROM @%{t.split('.')[-1]} FILE_FORMAT=(TYPE=JSON) PURGE=TRUE")
        os.remove(path)

    def insert_forecasts(self, rows):
        for r in rows:
            a = (TS(r["issued_at"]), TS(r["target_hour"]), r["method"], r["forecast_ci"])
            self.execute(f"INSERT INTO {self._t('forecast')} VALUES ({','.join(['%s' if self.snow else '?']*4)})", a)

    def insert_job_run(self, r):
        cols = ["job_id", "run_id", "planned_start", "actual_start", "energy_kwh", "ci_at_run", "ci_at_baseline", "kg_saved", "mode"]
        a = [TS(r[c]) if c.endswith("_start") else r[c] for c in cols]
        self.execute(f"INSERT INTO {self._t('job_run')} VALUES ({','.join(['%s' if self.snow else '?']*len(cols))})", a)

    # ---- reads -----------------------------------------------------------------------------------------
    def hourly(self, since=None):
        """Hourly mean intensity plus the net-load proxy (non-wind/solar generation, MW)."""
        t = self._t("carbon_interval")
        hour = "DATE_TRUNC('hour', INTERVAL_UTC)" if self.snow else "substr(interval_utc,1,13)||':00:00Z'"
        ph = "%s" if self.snow else "?"
        where = f"WHERE CI IS NOT NULL {'AND INTERVAL_UTC >= ' + ph if since else ''}"
        sql = (f"SELECT {hour} hour_utc, AVG(ci) ci, AVG(total_mw - COALESCE(wind_mw,0) - COALESCE(solar_mw,0)) net_load, "
               f"AVG(COALESCE(wind_mw,0) + COALESCE(solar_mw,0)) ws_mw, "
               f"AVG(renewable_share) renewable_share, COUNT(*) n FROM {t} {where} GROUP BY 1 ORDER BY 1")
        return self.query(sql, (TS(since),) if since else ())

    def ci_at(self, iso_ts):
        """Mean intensity of the hour containing iso_ts (used by the scheduler savings calculation)."""
        t = self._t("carbon_interval")
        if self.snow:
            rows = self.query(f"SELECT AVG(ci) ci FROM {t} WHERE DATE_TRUNC('hour', INTERVAL_UTC)=DATE_TRUNC('hour', %s::TIMESTAMP_NTZ)", (TS(iso_ts),))
        else:
            rows = self.query(f"SELECT AVG(ci) ci FROM {t} WHERE substr(interval_utc,1,13)=substr(?,1,13)", (iso_ts,))
        return rows[0]["ci"] if rows and rows[0]["ci"] is not None else None
