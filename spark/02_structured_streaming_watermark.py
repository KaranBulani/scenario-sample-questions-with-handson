"""
================================================================================
SPARK INTERVIEW SCENARIO #2 — Structured Streaming: dedup + windows + watermark
================================================================================
THE ASK
    "Build a streaming job over the clickstream topic: drop duplicates,
     compute 5-minute per-page counts, and land results in BigQuery so a
     task restart or replay never double-counts. Handle late data."

HOW TO RUN
    pip install pyspark google-cloud-bigquery
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \
      spark/02_structured_streaming_watermark.py
    Local no-Kafka variant: swap read_kafka() for
      spark.readStream.format("socket").option("host","localhost").option("port",9999)
    and stream NDJSON rows with `nc -lk 9999`.

IMPLEMENTATION WALKTHROUGH
    1. withWatermark("event_time", "10 minutes") — the watermark = max event
       time seen minus 10 min. It bounds EVERYTHING stateful downstream:
       dedup state, window state. State older than the watermark is evicted
       → memory doesn't grow forever. Anchor the whole interview here.
    2. dropDuplicatesWithinWatermark (Spark 3.5+) — duplicate memory is
       dropped once past the watermark. Plain dropDuplicates(["event_id"])
       keeps duplicate keys in state FOREVER — the classic production
       memory leak; calling that out is the point.
    3. windowed() — groupBy(window("event_time", "5 minutes"), page).
       Aggregation state per (window, page) is also evicted after the
       watermark passes the window's END.
    4. append output mode — a window's row is emitted exactly ONCE, after
       the watermark closes it. Perfect fit for an upsert sink.
       update mode would emit partial counts every micro-batch (good for a
       dashboard cache, wrong for a mart table); complete mode never
       evicts state (must re-output ALL history).
    5. upsert_to_bigquery via foreachBatch — foreachBatch is AT-LEAST-ONCE
       (a failed batch replays), so the sink MUST be idempotent: stage the
       micro-batch, then MERGE on the deterministic key (page,
       window_start). That one sentence is the exactly-once story.
    6. checkpointLocation — holds Kafka offsets, state, and commit log.
       Restart resumes from it; change the query/shape → NEW checkpoint
       path, or you get corrupt state.

CONCEPTS THE INTERVIEWER IS PROBING
    * Event-time vs processing-time; watermark mechanics and idle sources
      (an idle Kafka partition can stall the watermark —
       spark.sql.streaming.metricsEnabled to observe).
    * The three output modes and their constraints.
    * foreachBatch trade-offs: not structured-output-friendly, but the
       ONLY way to upsert; gets batch_id for idempotency keying.
    * trigger(processingTime="1 minute") vs once=True vs availableNow=True
      (availableNow = today's answer for incremental-ish replayable jobs).
    * Exactly-once end-to-end = replayable source (Kafka offsets via
      checkpoint) + idempotent/transactional sink (MERGE). BigQuery native
      streaming insert (Storage Write API) with insertId is the other
      idempotent sink option.

LIKELY FOLLOW-UP QUESTIONS
    Q: An event arrives 15 minutes late — what happens?
    A: Later than the 10-min watermark → DROPPED (it would need evicted
       state). Tune the watermark to your observed lateness; too large =
       more state held. Optionally capture drops by watching numDropped*
       metrics / a side output pattern.
    Q: How do you change the aggregation without losing data?
    A: You can't reuse the checkpoint for a changed plan — run a new query
       with a new checkpoint dir from earliest offsets, or batch-backfill
       from the source of truth.
    Q: Duplicate arrives AFTER its window closed?
    A: It's either dropped by dedup (if within watermark) or dropped as
       too-late — either way the MERGE key guarantees no double count.
    Q: Why not write straight to the BigQuery connector sink?
    A: Native streaming sink appends rows; retried micro-batches then
       duplicate. MERGE in foreachBatch absorbs the replay.
================================================================================
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PROJECT = os.environ.get("GCP_PROJECT", "my-project")
CHECKPOINT = "gs://de-interview-checkpoints/page-stats"   # offsets + state + commits
BQ_STAGE = f"{PROJECT}:streaming._stage_page_stats"
BQ_TARGET = f"{PROJECT}:streaming.page_stats"

MERGE_SQL = f"""
MERGE `{BQ_TARGET}` t
USING `{BQ_STAGE}` s
ON t.page = s.page AND t.window_start = s.window_start
WHEN MATCHED THEN UPDATE SET hits = t.hits + s.hits, uniques = s.uniques
WHEN NOT MATCHED THEN INSERT (page, window_start, window_end, hits, uniques)
                      VALUES (s.page, s.window_start, s.window_end, s.hits, s.uniques)
"""


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("streaming-page-stats").getOrCreate()


def read_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "clickstream")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "true")   # die loudly rather than silently skip
        .load()
    )


def parse(raw: DataFrame) -> DataFrame:
    schema = "event_id STRING, user_id STRING, page STRING, event_time TIMESTAMP"
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), schema).alias("e")
    ).select("e.*")
    return parsed.withWatermark("event_time", "10 minutes")   # bounds all state below


def dedupe(events: DataFrame) -> DataFrame:
    # Spark 3.5+; older releases: .dropDuplicates(["event_id"]) — unbounded state.
    return events.dropDuplicatesWithinWatermark(["event_id"])


def windowed(events: DataFrame) -> DataFrame:
    return (
        events.groupBy(F.window("event_time", "5 minutes"), F.col("page"))
        .agg(F.count("*").alias("hits"), F.approx_count_distinct("user_id").alias("uniques"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "page", "hits", "uniques",
        )
    )


def upsert_to_bigquery(batch_df: DataFrame, batch_id: int) -> None:
    """Idempotent sink: same (page, window) replayed → MERGE, never double count."""
    (
        batch_df.write.format("bigquery")
        .mode("overwrite")
        .option("temporaryGcsBucket", "de-interview-tmp")
        .save(BQ_STAGE)
    )
    from google.cloud import bigquery  # import here: shipper-side dep on executors

    bigquery.Client().query(MERGE_SQL).result()


def main() -> None:
    spark = get_spark()
    stats = windowed(dedupe(parse(read_kafka(spark))))

    query = (
        stats.writeStream
        .outputMode("append")   # each closed window emitted exactly once
        .foreachBatch(upsert_to_bigquery)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="1 minute")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
