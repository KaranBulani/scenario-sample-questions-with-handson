"""
================================================================================
AIRFLOW INTERVIEW SCENARIO #1 — Daily incremental ELT DAG (TaskFlow API)
================================================================================
THE ASK (how the interviewer phrases it)
    "Write a production-grade DAG that waits for an upstream extract file,
     loads each partition it finds, applies a data-quality gate, and only
     publishes to the mart if the row counts pass a threshold. Make it safe
     to backfill."

HOW TO RUN
    Not a standalone script — it is a DAG file parsed by Airflow.
        pip install apache-airflow apache-airflow-providers-google
        airflow standalone                      # local webserver+scheduler
    Validate this file:  python -c "import airflow.utils.dag_test"  (or simply
    import the module)  python airflow/01_elt_dag_taskflow.py  -- only parses.
    Best local check: call sales_daily_elt().test() (see file #2 for that).

IMPLEMENTATION WALKTHROUGH
    1. notify_on_failure — `on_failure_callback` receives the full task
       `context` dict (ti, dag_run, exception ...). Alerting lives here so
       every task inherits it via default_args.
    2. wait_for_extract — @task.sensor with mode="reschedule": between pokes
       the task RELEASES its worker slot (poke mode would hold it). Uses
       data_interval_start (NOT datetime.now()) so backfilled runs look at
       the right file — this is the idempotency trick.
    3. TaskFlow returns become XComs automatically — return metadata only
       (URIs, row counts). Never row data: XComs default to ~48 KB and are
       stored in the metadata DB.
    4. load_partition.partial(...).expand(partition=...) — dynamic task
       mapping. .partial() holds the constant arg; .expand() maps over the
       list. The scheduler creates one task INSTANCE per partition at
       runtime; each retries independently. Perfect for "N partitions
       discovered at runtime".
    5. quality_gate — @task.branch returns the task_id(s) to run; every other
       direct downstream gets SKIPPED. An empty extract raises
       AirflowSkipException (skip ≠ fail → no pager alert).
    6. summarize — TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS lets it run when
       exactly one of the two gate branches succeeded and the other was
       skipped. XComArg of a skipped task resolves to None.

CONCEPTS THE INTERVIEWER IS PROBING
    * catchup/backfill: with catchup=True the scheduler creates a run for
      every missed data interval; correctness therefore depends on deriving
      paths from data_interval_start/end, never on "now".
    * logical_date == data_interval START (why the 03:00 run on Oct-2
      processes Oct-1's data interval).
    * Idempotency: partition-keyed writes / MERGE, so re-running a backfill
      can never double-load.
    * max_active_runs=1 — serialise overlapping intervals for ordered loads.
    * retries + exponential_backoff on flaky external APIs.
    * TaskFlow vs classic operators: plain Python, typed IO, auto-XCom.

LIKELY FOLLOW-UP QUESTIONS
    Q: logical_date vs data_interval_start/end?
    A: For `schedule="0 3 * * *"` a run "executes" at 03:00 but its logical
       date is 03:00 the PREVIOUS day — the run processes [start, end) of
       the interval that just closed. Read data_interval_start in tasks.
    Q: A mapped task instance fails mid-run — what happens?
    A: Only that instance burns its retries; others finish. The downstream
       reduce task (quality_gate) waits for ALL mapped instances, so it
       stays upstream_failed unless you give it a different trigger rule.
    Q: Why not pass the DataFrame through XCom?
    A: XCom = small metadata in the metadata DB (48 KB default, custom XCom
       backends exist for larger). Data flows through external systems:
       GCS/BigQuery; tasks exchange pointers, not payloads.
    Q: How do you test this without a scheduler?
    A: DAG structure: `dag.test()` (Airflow 2.5+) or import + assert; task
       logic: unit-test the plain Python functions by extracting them.
================================================================================
"""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)
NOTIFY_EMAIL = "data-oncall@example.com"
BUCKET = "de-interview-data"


def notify_on_failure(context: dict) -> None:
    """on_failure_callback: every task gets the run context dict."""
    ti = context["task_instance"]
    log.error(
        "ALERT (%s): task=%s run=%s failed after retries",
        NOTIFY_EMAIL, ti.task_id, context["dag_run"].run_id,
    )


@task.sensor(poke_interval=30, timeout=600, mode="reschedule")
def wait_for_extract() -> str:
    """Poke until the extract for THIS data interval lands in GCS."""
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    ctx = get_current_context()
    ds = ctx["data_interval_start"].strftime("%Y-%m-%d")   # backfill-safe
    obj = f"incoming/sales/{ds}.jsonl"
    if GCSHook(gcp_conn_id="google_cloud_default").exists(BUCKET, obj):
        return f"gs://{BUCKET}/{obj}"
    return False                                            # keep poking


@task
def list_partitions(file_uri: str) -> list[str]:
    """Discover partitions in the extract (stand-in: GCS listing / manifest)."""
    log.info("inspecting %s", file_uri)
    return ["region=emea/country=de", "region=emea/country=fr", "region=apac/country=in"]


@task(retries=3, retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=10))
def load_partition(file_uri: str, partition: str) -> dict:
    """Load ONE partition into BigQuery. Idempotent: partition-keyed MERGE."""
    rows = 1_234                                             # stand-in: load-job count
    log.info("loaded %s rows for %s from %s", rows, partition, file_uri)
    return {"partition": partition, "rows": rows}


@task.branch(retries=1)
def quality_gate(metrics: list[dict]) -> str:
    """Return the task_id to run; the other branch will be SKIPPED."""
    total = sum(m["rows"] for m in metrics)
    if total == 0:
        raise AirflowSkipException("empty extract — nothing to publish")
    return "publish_to_mart" if total >= 3_000 else "quarantine_and_alert"


@task
def publish_to_mart(metrics: list[dict]) -> str:
    return f"published {len(metrics)} partitions"


@task
def quarantine_and_alert(metrics: list[dict]) -> str:
    log.warning("row count below threshold: %s", metrics)
    return "quarantined"


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def summarize(published: str | None, quarantined: str | None) -> None:
    """Runs when one branch succeeded and the other was skipped."""
    log.info("pipeline finished: %s", published or quarantined)


@dag(
    dag_id="sales_daily_elt",
    description="Interview scenario: incremental ELT, dynamic mapping, quality gate",
    schedule="0 3 * * *",                    # run at 03:00 over the PREVIOUS interval
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=True,                            # unpausing backfills every missed interval
    max_active_runs=1,                       # never overlap two days of this pipeline
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notify_on_failure,
    },
    tags=["interview", "elt"],
)
def sales_daily_elt():
    file_uri = wait_for_extract()
    partitions = list_partitions(file_uri)

    # .partial() = constant arg, .expand() = the arg we map over (runtime list).
    metrics = load_partition.partial(file_uri=file_uri).expand(partition=partitions)

    gate = quality_gate(metrics)                 # auto-waits for ALL mapped instances
    published = publish_to_mart(metrics)
    quarantined = quarantine_and_alert(metrics)
    gate >> [published, quarantined]             # branch picks exactly one path
    summarize(published, quarantined)            # implicit deps on both branches


sales_daily_elt()
