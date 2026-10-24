"""
================================================================================
DATAFLOW INTERVIEW SCENARIO #1 — Streaming: Pub/Sub → windows → BigQuery + DLQ
================================================================================
THE ASK
    "Build a Dataflow streaming pipeline: consume JSON events from Pub/Sub,
     quarantine malformed records to a dead-letter bucket (pipeline must
     survive poison pills), aggregate spend per user in 5-minute windows
     with speculative updates and 30 minutes of lateness tolerance, and
     append to BigQuery."

HOW TO RUN
    pip install "apache-beam[gcp]"
    Local (DirectRunner, no project needed — swap SUBSCRIPTION to a local one):
        python dataflow/01_streaming_pubsub_windows.py --streaming \
            --subscription=projects/<prj>/subscriptions/clickstream-sub
    On Dataflow:
        append --runner=DataflowRunner --project=<prj> --region=<rg> \
               --temp_location=gs://<bucket>/tmp --experiments=use_runner_v2

IMPLEMENTATION WALKTHROUGH
    1. ParseEvent with a SIDE OUTPUT — the #1 production pattern. A poison
       record yields TaggedOutput(BAD, {...}) instead of raising; the
       pipeline keeps running, humans get a reviewable DLQ file. try/except
       + log-and-drop loses data silently — say that out loud.
    2. assign_event_time — Beam defaults to PROCESSING time (arrival).
       TimestampedValue re-stamps elements with EVENT time; the watermark
       is computed from these timestamps, so skipping this silently breaks
       every window below. (Pub/Sub alternative: attach a timestamp
       attribute and pass timestamp_attribute to ReadFromPubSub.)
    3. WindowInto(FixedWindows(300)) with the triple that gets probed:
         trigger = AfterWatermark(early=AfterProcessingTime(60),
         late=AfterCount(1)) → one on-time result, speculative results
         every ~60 s while the window is open, and a fire per late arrival.
         accumulation_mode = ACCUMULATING → each fire re-emits the running
         total (BQ gets repeated growing rows — dedupe via insert IDs; or
         use DISCARDING + deltas). allowed_lateness = 30 min → after that,
         late elements are dropped.
    4. CombinePerKey(sum) — windowed: per user PER WINDOW. CombinePerKey
       (not GroupByKey) means partial sums are pre-aggregated on workers =
       less data through the shuffle ("combiner lifting").
    5. ToBqRow via DoFn.WindowParam — the only way to put window bounds
       into the output row.
    6. WriteToBigQuery WRITE_APPEND — retried bundles make streaming
       at-least-once; BQ insert-ID de-duplication absorbs the replays.

CONCEPTS THE INTERVIEWER IS PROBING
    * Event time vs processing time; what the watermark is and what
      advances it (Pub/Sub source: ~10 s data-freshness heuristic over the
      timestamp attribute; idle sources can stall it).
    * Trigger × accumulation × lateness interplay — draw the timeline.
    * Side output vs side input (output = a second output PCollection from
      one DoFn; input = read-only broadcast data).
    * Exactly-once semantics on Dataflow: source offsets + sink dedup.
    * Streaming Engine + Streaming Autoscaler (Dataflow Shuffle moves
      group-by-key work out of workers).

LIKELY FOLLOW-UP QUESTIONS
    Q: Event arrives 45 minutes late (lateness allowance is 30)?
    A: Dropped at the window. Mitigate: raise allowed_lateness (costs
       state), or capture via Beam 2.5x+ onWindowExpiration / a second
       pipeline on the raw topic for manual replay.
    Q: How do you reprocess the DLQ?
    A: It's GCS text — run a BATCH pipeline over it: fix the transform,
       write to a side table, then MERGE into the target.
    Q: ACCUMULATING appends the same user-window repeatedly — no double
       counting?
    A: Row carries (user, window_start, spend); downstream treats latest
       per key as truth (MERGE), or rely on BQ insert IDs deduping exact
       re-deliveries. Name the trade-off and you're through.
    Q: Why CombinePerKey over GroupByKey?
    A: Lifted combiners shrink shuffle volume — partial merges happen
       before network transfer.
================================================================================
"""
import json
from datetime import datetime

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.utils.timestamp import Duration

SUBSCRIPTION = "projects/my-project/subscriptions/clickstream-sub"
TABLE = "my-project:streaming.user_spend"
DLQ_PREFIX = "gs://de-interview-dlq/events"
BQ_SCHEMA = "user_id:STRING,spend:FLOAT,window_start:TIMESTAMP,window_end:TIMESTAMP"


class ParseEvent(beam.DoFn):
    """Route poison pills to a dead-letter side output; never crash the pipe."""

    BAD = "bad"

    def process(self, element: bytes):
        try:
            event = json.loads(element)
            missing = {"user_id", "event_time", "amount"} - event.keys()
            if missing:
                raise ValueError(f"missing fields: {sorted(missing)}")
            yield event
        except Exception as err:  # noqa: BLE001 — quarantine, don't drop
            yield beam.pvalue.TaggedOutput(
                self.BAD,
                {"payload": element.decode("utf-8", "replace"), "error": str(err)},
            )


class ToBqRow(beam.DoFn):
    """DoFn.WindowParam reads the element's window — how bounds reach the row."""

    def process(self, elem, window=beam.DoFn.WindowParam):
        return [{
            "user_id": elem[0],
            "spend": float(elem[1]),
            "window_start": window.start.to_utc_datetime().isoformat(),
            "window_end": window.end.to_utc_datetime().isoformat(),
        }]


def assign_event_time(event: dict):
    """Re-stamp with EVENT time — the watermark is computed from these."""
    ts = datetime.fromisoformat(event["event_time"]).timestamp()
    return beam.window.TimestampedValue(event, ts)


def build_pipeline(argv=None) -> beam.Pipeline:
    options = PipelineOptions(argv, save_main_session=True)
    options.view_as(StandardOptions).streaming = True   # required for Pub/Sub

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "read-pubsub" >> ReadFromPubSub(subscription=SUBSCRIPTION)
            | "parse" >> beam.ParDo(ParseEvent()).with_outputs(ParseEvent.BAD, main="ok")
        )

        # Dead-letter: reviewable GCS files, replayable by a batch pipeline.
        (
            parsed.bad
            | "dlq-json" >> beam.Map(json.dumps)
            | "dlq-write" >> WriteToText(DLQ_PREFIX, file_name_suffix=".json")
        )

        (
            parsed.ok
            | "assign-event-time" >> beam.Map(assign_event_time)
            | "window-5m" >> beam.WindowInto(
                beam.window.FixedWindows(size=300),
                trigger=beam.trigger.AfterWatermark(
                    early=beam.trigger.AfterProcessingTime(60),  # speculative fires
                    late=beam.trigger.AfterCount(1),             # fire per late arrival
                ),
                accumulation_mode=beam.trigger.AccumulationMode.ACCUMULATING,
                allowed_lateness=Duration(seconds=1800),        # 30 min
            )
            | "key-by-user" >> beam.Map(lambda e: (e["user_id"], float(e["amount"])))
            | "spend-per-user" >> beam.CombinePerKey(sum)
            | "to-bq-row" >> beam.ParDo(ToBqRow())
            | "write-bq" >> WriteToBigQuery(
                TABLE,
                schema=BQ_SCHEMA,
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED",
            )
        )
    return p


if __name__ == "__main__":
    build_pipeline()
