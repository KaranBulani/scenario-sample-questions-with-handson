"""
================================================================================
DATAFLOW INTERVIEW SCENARIO #2 — Batch: CoGroupByKey join + side-input enrich
================================================================================
THE ASK
    "Batch job: enrich orders with user tier (join two GCS files) and with a
     small country-code lookup (side input). Orders without a matching user
     must still flow. Write to a partitioned, clustered BigQuery table so
     the job can be re-run safely."

HOW TO RUN
    pip install "apache-beam[gcp]"
    Local:  python dataflow/02_batch_join_side_inputs.py \
                --orders=gs://.../orders.csv --users=gs://.../users.csv \
                --codes=gs://.../codes.csv
    On Dataflow: append --runner=DataflowRunner --project=... --region=... \
                 --temp_location=gs://<bucket>/tmp
    Unit test (show this proactively):
        with beam.Pipeline() as p:
            rows = p | beam.Create([...]) | your_transform
            assert_that(rows, equal_to([...]))

IMPLEMENTATION WALKTHROUGH
    1. ParseCsv DoFn shows the DoFn LIFECYCLE interview question: setup()
       once per WORKER (load models, compile regexes — never in process()),
       start_bundle() per bundle, process() per element. Moving heavy init
       out of process() is exactly what they want to hear.
    2. CoGroupByKey — Beam's join. Both sides keyed by user_id; output is
       (key, {"orders": [...], "users": [...]}). LEFT JOIN semantics are
       implemented in the enrich fn: iterate orders, fall back when
       grouped["users"] is empty. Key appears if EITHER side has it — you
       decide join type in the consumer, which is the point of CoGBK.
    3. Side input: codes is small (a few hundred rows) → pvalue.AsDict
       turns the PCollection into a VIEW broadcast to every worker. The
       FlatMap receives a plain dict — Beam materializes it per bundle.
    4. Rule of thumb they probe: small static lookup → side input;
       big/unbounded data → CoGBK join (a side input is materialized
       wholesale — a huge one eats worker memory).
    5. WriteToBigQuery with WRITE_TRUNCATE + time partitioning + clustering:
       re-running the job replaces just that day's partition → idempotent
       batch loads (companion to the streaming file's WRITE_APPEND).

CONCEPTS THE INTERVIEWER IS PROBING
    * PCollection immutability — transforms produce new PCollections;
      enrich "mutates" by yielding new dicts.
    * Side input flavors: AsDict (unique keys — duplicate key = error!),
      AsList, AsSingleton; main input vs side input windows (a global
      window side input serves all main windows).
    * CoGBK is a SHUFFLE both sides pay; hot keys (one whale user) skew
      workers — mitigation: salting or pre-aggregation.
    * What happens if users.csv is empty → CoGBK still emits every order
      with users=[] → enrich falls back to tier="guest". Reason it through.
    * Fusion / combiner lifting: Beam fuses adjacent stages; combiners
      pre-aggregate before the shuffle. Explain why CombinePerKey > GBK.

LIKELY FOLLOW-UP QUESTIONS
    Q: Side input has duplicate keys?
    A: AsDict raises at runtime — de-dupe upstream or use AsList and index.
    Q: Lookup table is 50 GB?
    A: Not a side input anymore — CoGBK join (or broadcast join in Spark).
    Q: Why FlatMap not Map in enrich?
    A: One joined key yields N output rows (fan-out per order).
    Q: How is this different from a Spark join?
    A: Same shuffle idea, different shape: Beam joins are keyed-PCollection
      transforms (CoGBK), Spark offers sort-merge/broadcast strategies
      with a cost-based optimizer.
================================================================================
"""
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

ORDERS = "gs://de-interview/orders-2026-10-24.csv"      # user_id,order_id,amount,country_code
USERS = "gs://de-interview/users.csv"                   # user_id,tier
CODES = "gs://de-interview/country_codes.csv"           # code,name  (small → side input)
TABLE = "my-project:mart.enriched_orders"
BQ_SCHEMA = "order_id:STRING,user_id:STRING,amount:FLOAT,tier:STRING,country:STRING"


class ParseCsv(beam.DoFn):
    """Lifecycle demo: setup per worker, start_bundle per bundle, process per line."""

    def __init__(self, columns: list[str]) -> None:
        self._columns = columns
        self._parse = None                       # built once per worker

    def setup(self) -> None:                     # per WORKER: heavy init goes here
        self._parse = lambda line: dict(zip(self._columns, line.split(",")))

    def start_bundle(self) -> None:              # per BUNDLE: cheap per-batch state
        self._seen_header = False

    def process(self, line: str):                # per ELEMENT: keep it lean
        line = line.strip()
        if not line or line.startswith("user_id,") or line.startswith("code,"):
            return                               # skip blanks and CSV headers
        yield self._parse(line)


def build_pipeline(argv=None) -> beam.Pipeline:
    options = PipelineOptions(argv, save_main_session=True)

    with beam.Pipeline(options=options) as p:
        keyed_orders = (
            p
            | "read-orders" >> ReadFromText(ORDERS)
            | "parse-orders" >> beam.ParDo(ParseCsv(["user_id", "order_id", "amount", "country_code"]))
            | "key-orders" >> beam.Map(lambda o: (o["user_id"], o))
        )
        keyed_users = (
            p
            | "read-users" >> ReadFromText(USERS)
            | "parse-users" >> beam.ParDo(ParseCsv(["user_id", "tier"]))
            | "key-users" >> beam.Map(lambda u: (u["user_id"], u))
        )
        codes = (
            p
            | "read-codes" >> ReadFromText(CODES)
            | "parse-codes" >> beam.ParDo(ParseCsv(["code", "name"]))
            | "key-codes" >> beam.Map(lambda c: (c["code"], c["name"]))
        )
        code_names = pvalue.AsDict(codes)   # VIEW: materialized, broadcast to workers

        def enrich(kv, lookup):
            user_id, grouped = kv          # CoGBK → (key, {"orders": [...], "users": [...]})
            user = grouped["users"][0] if grouped["users"] else {"tier": "guest"}
            for order in grouped["orders"]:   # LEFT JOIN: orders drive the fan-out
                yield {
                    "order_id": order["order_id"],
                    "user_id": user_id,
                    "amount": float(order["amount"]),
                    "tier": user["tier"],
                    "country": lookup.get(order["country_code"], "unknown"),
                }

        (
            {"orders": keyed_orders, "users": keyed_users}
            | "co-group-by-key" >> beam.CoGroupByKey()
            | "enrich" >> beam.FlatMap(enrich, lookup=code_names)
            | "write-bq" >> WriteToBigQuery(
                TABLE,
                schema=BQ_SCHEMA,
                write_disposition="WRITE_TRUNCATE",       # re-run = replace, idempotent
                additional_bq_parameters={
                    "timePartitioning": {"type": "DAY"},   # partitioned mart table
                    "clustering": {"fields": ["user_id"]},
                },
            )
        )
    return p


if __name__ == "__main__":
    build_pipeline()
