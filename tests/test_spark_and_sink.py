import pytest
fakeredis = pytest.importorskip("fakeredis")
pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession, functions as F

from src.common.emissions import carbon_intensity
from src.common.store import Store
from src.serving import redis_io
from src.streaming.carbon_stream import BatchSink, SCHEMA, latest_per_window, with_intensity

ROWS = [
    ("2026-06-01T20:00:00Z", {"Solar": 0.0, "Natural Gas": 9000.0, "Imports": 5000.0, "Batteries": 2500.0, "Large hydro": 1500.0}, "2026-06-01T20:06:00Z", "new"),
    ("2026-06-01T20:05:00Z", {"Solar": 0.0, "Natural Gas": 9100.0, "Imports": 5000.0, "Batteries": -300.0, "Large hydro": 1500.0}, "2026-06-01T20:11:00Z", "new"),
    ("2026-06-01T20:05:00Z", {"Solar": 0.0, "Natural Gas": 8000.0, "Imports": 5000.0, "Batteries": -300.0, "Large hydro": 1500.0}, "2026-06-01T20:20:00Z", "revised"),
]


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").appName("t").config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate()
    yield s
    s.stop()


def df_of(spark):
    return spark.createDataFrame(ROWS, SCHEMA).withColumn("event_time", F.to_timestamp("interval_utc"))


def test_spark_matches_python_reference(spark):
    out = {r["interval_utc"] + r["revision_ts"]: r for r in with_intensity(df_of(spark)).collect()}
    for iv, fuels, rev, _ in ROWS:
        ref = carbon_intensity(fuels)
        got = out[iv + rev]
        assert got["ci"] == pytest.approx(ref["ci"], rel=1e-9)
        assert got["renewable_share"] == pytest.approx(ref["renewable_share"], rel=1e-9)


def test_revision_replaces_first_publication(spark):
    res = {r["interval_utc"]: r for r in latest_per_window(with_intensity(df_of(spark))).collect()}
    assert len(res) == 2                                                   # two 5-minute windows, not three rows
    assert res["2026-06-01T20:05:00Z"]["ci"] == pytest.approx(carbon_intensity(ROWS[2][1])["ci"])


def test_sink_updates_redis_and_warehouse_idempotently():
    st, r = Store(), fakeredis.FakeRedis(decode_responses=True)
    sink = BatchSink(st, r)
    mk = lambda iv, ci, rev: {"interval_utc": iv, "ci": ci, "total_mw": 20000.0, "renewable_share": 0.3, "wind_mw": 100.0, "solar_mw": 0.0, "revision_ts": rev}
    sink([mk("2026-06-01T20:00:00Z", 300.0, "a"), mk("2026-06-01T20:05:00Z", 280.0, "b")])
    sink([mk("2026-06-01T20:05:00Z", 250.0, "c")])                          # revision of the latest interval
    cur = redis_io.current(r)
    assert float(cur["ci"]) == 250.0 and cur["interval_utc"] == "2026-06-01T20:05:00Z" and "updated_at" in cur
    rows = st.query("SELECT * FROM raw_carbon_interval WHERE interval_utc LIKE '2026-06-01%' ORDER BY interval_utc")
    assert [x["ci"] for x in rows] == [300.0, 250.0]                        # revision overwrote, no duplicate row
    sink([mk("2026-06-01T19:55:00Z", 999.0, "d")])                          # late/old interval must not overwrite 'current'
    assert float(redis_io.current(r)["ci"]) == 250.0
