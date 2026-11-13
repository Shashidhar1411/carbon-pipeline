"""Spark Structured Streaming: Kafka(caiso.fuelmix) -> carbon intensity per 5-minute event-time window ->
Redis (live value) + warehouse (history).

Run (inside the spark container or locally with Java 17):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 -m src.streaming.carbon_stream
  (or: python -m src.streaming.carbon_stream  with PYSPARK_SUBMIT_ARGS set, see README)

Design notes
* 30-minute watermark: CAISO revises recent intervals; a revised record arriving within the watermark replaces the
  earlier value because each window keeps the record with the greatest revision_ts (max_by).
* Output mode 'update' + idempotent MERGE into the warehouse means a revision overwrites, never duplicates.
* Batteries: negative output = charging = load; clipped to zero so it does not distort the denominator.
"""
import os
from datetime import datetime, timezone
from itertools import chain

from pyspark.sql import SparkSession, functions as F, types as T

from src.common import config                      # absolute imports: spark-submit runs this file as a script
from src.common.emissions import EMISSION_FACTORS, RENEWABLES
from src.common.streaming_algos import DGIM, ReservoirSample

DIRTY_THRESHOLD = float(os.getenv("DIRTY_THRESHOLD", "250"))   # kg CO2/MWh; interval counted 'dirty' above this

SCHEMA = T.StructType([
    T.StructField("interval_utc", T.StringType()),
    T.StructField("fuels", T.MapType(T.StringType(), T.DoubleType())),
    T.StructField("revision_ts", T.StringType()),
    T.StructField("status", T.StringType()),
])


def with_intensity(df):
    """Adds ci, total_mw, renewable_share, wind_mw, solar_mw columns computed from the `fuels` map column."""
    ef = F.create_map(*chain.from_iterable((F.lit(k), F.lit(float(v))) for k, v in EMISSION_FACTORS.items()))
    gen = F.transform_values(F.col("fuels"), lambda k, v: F.greatest(v, F.lit(0.0)))     # drop battery charging
    total = F.aggregate(F.map_values(gen), F.lit(0.0), lambda a, x: a + x)
    emis = F.aggregate(F.map_entries(gen), F.lit(0.0),
                       lambda a, e: a + e["value"] * F.coalesce(F.element_at(ef, e["key"]), F.lit(0.0)))
    renew = F.aggregate(F.map_entries(gen), F.lit(0.0),
                        lambda a, e: a + F.when(e["key"].isin(*RENEWABLES), e["value"]).otherwise(F.lit(0.0)))
    return (df.withColumn("gen", gen).withColumn("total_mw", total)
              .withColumn("ci", F.when(F.col("total_mw") > 0, emis / F.col("total_mw")))
              .withColumn("renewable_share", F.when(F.col("total_mw") > 0, renew / F.col("total_mw")))
              .withColumn("wind_mw", F.coalesce(F.col("gen")["Wind"], F.lit(0.0)))
              .withColumn("solar_mw", F.coalesce(F.col("gen")["Solar"], F.lit(0.0))).drop("gen"))


class BatchSink:
    """Runs on the driver for every micro-batch. Kept separate from Spark so it can be unit-tested."""
    def __init__(self, store, redis_client, reservoir_path=None):
        self.store, self.r = store, redis_client
        self.dgim, self.last_interval = DGIM(288), None
        self.reservoir = ReservoirSample(500)
        self.reservoir_path = reservoir_path or (config.DATA_DIR / "reservoir_sample.json")

    def __call__(self, rows):
        from src.serving import redis_io
        import json
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows = sorted(rows, key=lambda r: r["interval_utc"])
        out = [dict(r, updated_at=now) for r in rows if r["ci"] is not None]
        self.store.upsert_carbon(out)
        for r in out:
            if self.last_interval is None or r["interval_utc"] > self.last_interval:     # revisions are not re-counted
                self.dgim.update(1 if r["ci"] > DIRTY_THRESHOLD else 0)
                self.last_interval = r["interval_utc"]
                self.reservoir.add({"interval_utc": r["interval_utc"], "ci": r["ci"]})
        if out:
            latest = max(out, key=lambda r: r["interval_utc"])
            cur = redis_io.current(self.r)
            if not cur or latest["interval_utc"] >= cur.get("interval_utc", ""):
                redis_io.set_current(self.r, {k: latest[k] for k in ("interval_utc", "ci", "total_mw", "renewable_share")},
                                     {"updated_at": now, "dirty_intervals_24h_dgim": self.dgim.count()})
            with open(self.reservoir_path, "w") as f:
                json.dump(self.reservoir.items, f)


def drop_future(df, minutes=15):
    """Events stamped in the future are bad data. Left in, they push the watermark ahead and Spark then silently
    discards every real event until that time arrives."""
    return df.filter(F.col("event_time") <= F.current_timestamp() + F.expr(f"INTERVAL {minutes} MINUTES"))


def latest_per_window(scored):
    """Tumbling 5-minute event-time windows; within a window keep the record with the greatest revision_ts, so a
    CAISO revision replaces the first publication. Works on batch and streaming DataFrames."""
    return (scored.groupBy(F.window("event_time", config.WINDOW))
            .agg(F.max_by(F.struct("interval_utc", "ci", "total_mw", "renewable_share", "wind_mw", "solar_mw", "revision_ts"),
                          "revision_ts").alias("r"))
            .select("r.*"))


def build_stream(spark):
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
           .option("subscribe", config.TOPIC_CAISO_FUEL).option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", 3000).load())
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("m")).select("m.*")
              .withColumn("event_time", F.to_timestamp("interval_utc")))
    scored = drop_future(with_intensity(parsed))
    return latest_per_window(scored.withWatermark("event_time", config.WATERMARK))


def main():
    from src.common.store import Store
    from src.serving import redis_io
    spark = SparkSession.builder.appName("carbon-stream").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sink = BatchSink(Store(), redis_io.client())
    q = (build_stream(spark).writeStream.outputMode("update")
         .foreachBatch(lambda df, _id: sink([r.asDict() for r in df.collect()]))
         .option("checkpointLocation", os.getenv("CHECKPOINT_DIR", "/tmp/carbon-checkpoint")).start())
    q.awaitTermination()


if __name__ == "__main__":
    main()
