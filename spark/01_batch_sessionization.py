"""
================================================================================
SPARK INTERVIEW SCENARIO #1 — Batch Clickstream Sessionization (PySpark)
================================================================================
THE ASK
    "Given clickstream events (user, timestamp, page), deduplicate them and
     build sessions that break on 30 minutes of inactivity. Then produce
     per-session stats and the top-2 pages per session."

HOW TO RUN (fully self-contained, no cluster needed)
    pip install pyspark
    python spark/01_batch_sessionization.py
    (production source: spark.read.schema(...).json("gs://bucket/events/"))

IMPLEMENTATION WALKTHROUGH
    1. dedupe() — classic pattern: row_number() over a window partitioned by
       event_id, keep rn = 1. Why not df.dropDuplicates(["event_id"])?
       dropDuplicates picks an ARBITRARY row; the ordered window is
       deterministic (event_time desc, event_id as tie-breaker). Both cost
       the same shuffle.
    2. sessionize() — the heart of the scenario, two window passes:
         a) lag("event_time") over (partitionBy user, orderBy time) gives
            gap to the previous event; gap > 30 min (or NULL = first event)
            flags a session START.
         b) sum(starts) over the same window with an UNBOUNDED frame turns
            flags into a running session index → session_id.
       Because both windows use the SAME partitioning + ordering, Spark
       reuses one exchange+sort (check .explain() — one Exchange here).
    3. session_stats() — plain groupBy aggregation per session.
    4. top_pages_per_session() — top-N-per-group: aggregate →
       row_number() over (partitionBy session, orderBy hits desc) → filter
       rank <= N. Say those three words in this order and you pass.
    5. sessions.cache() BEFORE two downstream consumers — pay the shuffle
       once; without cache the lineage recomputes everything twice.
    6. AQE on + explicit repartition before write: fewer, bigger output
       files; partitionBy("dt") enables partition pruning on read.

CONCEPTS THE INTERVIEWER IS PROBING
    * Count the shuffles: dedupe window (1), sessionize windows (1 —
      shared), stats groupBy (1), page-count groupBy (1), rank window (1)
      ≈ 5 Exchanges. Run .explain() and point at them.
    * Narrow vs wide transformations: map/filter never shuffle; anything
      keyed (groupBy / join / window / distinct) does.
    * Skew: one power user → one gigantic partition. Salting: append a
      random 0..N suffix to the key, aggregate twice (per salt, then sum).
    * Window functions vs UDFs: stay in Catalyst (codegen, no
      serialization to Python workers). UDFs are a black box.
    * dropDuplicates vs row_number window — determinism story above.
    * cache() vs persist(DISK_ONLY/MEMORY_AND_DISK) for bigger-than-memory.

LIKELY FOLLOW-UP QUESTIONS
    Q: A duplicate arrives with the same event_time — which survives?
    A: With only event_time ordering it's arbitrary; the event_id
       tie-breaker in the ORDER BY makes it deterministic. Mentioning this
       unprompted scores points.
    Q: How would you do this on truly huge data without a full sort?
    A: Approximations (sessionization via flatMapGroupsWithState in
      Structured Streaming) or bucketed tables to pre-cluster by user.
    Q: Why did you set spark.sql.shuffle.partitions to 8?
    A: Defaults to 200 → 200 tiny files + task overhead on small data; AQE
       coalesces post-shuffle, but starting closer to right-sized helps.
    Q: Where does the driver OOM risk live here?
    A: .collect() / toPandas() of large results (this demo is tiny);
      sessionization itself is distributed.
================================================================================
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

SAMPLE = [  # stand-in for a day of GCS event logs
    ("u1", "2026-10-01 10:00:00", "/home", "e1"),
    ("u1", "2026-10-01 10:05:00", "/search", "e2"),
    ("u1", "2026-10-01 11:00:00", "/checkout", "e3"),   # 55-min gap → NEW session
    ("u2", "2026-10-01 10:02:00", "/home", "e4"),
    ("u2", "2026-10-01 10:02:00", "/home", "e4"),       # at-least-once duplicate
    ("u2", "2026-10-01 10:20:00", "/item/42", "e5"),
]


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("sessionization")
        .config("spark.sql.shuffle.partitions", 8)      # right-sized for local data
        .config("spark.sql.adaptive.enabled", "true")   # AQE coalesces shuffle partitions
        .getOrCreate()
    )


def read_events(spark: SparkSession) -> DataFrame:
    schema = "user_id STRING, event_time TIMESTAMP, page STRING, event_id STRING"
    return spark.createDataFrame(SAMPLE, schema=schema)
    # production: spark.read.schema(schema).json("gs://bucket/events/dt=2026-10-01/")


def dedupe(df: DataFrame) -> DataFrame:
    """One shuffle: keep exactly one row per event_id — latest, deterministically."""
    w = Window.partitionBy("event_id").orderBy(F.col("event_time").desc(), F.col("event_id"))
    return df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")


def sessionize(df: DataFrame, gap_minutes: int = 30) -> DataFrame:
    """Session = events with <= gap since the previous event, per user."""
    per_user = Window.partitionBy("user_id").orderBy("event_time")
    unbounded = per_user.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    gap_sec = F.col("event_time").cast("long") - F.lag("event_time").over(per_user).cast("long")
    starts = F.coalesce(gap_sec > F.lit(gap_minutes * 60), F.lit(True)).cast("int")

    return (
        df.withColumn("starts_session", starts)
        .withColumn("session_seq", F.sum("starts_session").over(unbounded))
        .withColumn("session_id", F.concat_ws(":", "user_id", "session_seq"))
        .drop("starts_session", "session_seq")
    )


def session_stats(df: DataFrame) -> DataFrame:
    return df.groupBy("session_id").agg(
        F.min("event_time").alias("session_start"),
        F.max("event_time").alias("session_end"),
        F.count("*").alias("events"),
    )


def top_pages_per_session(df: DataFrame, n: int = 2) -> DataFrame:
    """Top-N per group = aggregate + row_number window + rank filter."""
    counts = df.groupBy("session_id", "page").agg(F.count("*").alias("hits"))
    ranked = counts.withColumn(
        "rank", F.row_number().over(Window.partitionBy("session_id").orderBy(F.desc("hits")))
    )
    return ranked.where(f"rank <= {n}").drop("rank")


def main() -> None:
    spark = get_spark()
    sessions = sessionize(dedupe(read_events(spark)))
    sessions.cache()                    # two consumers below → compute sessions once
    session_stats(sessions).show()
    top_pages_per_session(sessions).show()
    sessions.explain(True)              # read the Exchange nodes; count the shuffles
    # production write:
    # stats.repartition(16).write.mode("overwrite").partitionBy("dt") \
    #     .parquet("gs://bucket/curated/sessions/")
    spark.stop()


if __name__ == "__main__":
    main()
