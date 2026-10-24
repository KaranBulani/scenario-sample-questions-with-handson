"""
================================================================================
AIRFLOW INTERVIEW SCENARIO #2 — Custom Hook + Operator + Deferrable Sensor
================================================================================
THE ASK
    "We call an internal audit API from many DAGs. Build a reusable custom
     operator and a sensor that waits for a report — without burning worker
     slots while polling. Add SLA, and show how you'd run this DAG locally."

HOW TO RUN
    pip install apache-airflow requests
    Local end-to-end run (Airflow 2.5+, no scheduler/webserver needed):
        python airflow/02_custom_operator_deferrable.py
    (this executes every task in-process via dag.test())

IMPLEMENTATION WALKTHROUGH
    1. AuditHook — a HOOK is the reusable client: it reads an Airflow
       Connection (host/port/password) and exposes small methods
       (get_report, is_ready). Operators stay thin; ten DAGs reuse one hook.
    2. RunAuditOperator — an OPERATOR is one DAG step. template_fields
       makes report_id Jinja-renderable ("{{ ds_nodash }}"); whatever
       execute() returns lands in XCom automatically.
    3. ReportReadyTrigger — the deferral workhorse. Triggers are ASYNC
       coroutines that run inside the single Triggerer process, thousands
       at a time, holding NO worker slot. asyncio.to_thread keeps the
       blocking requests call off the event loop. Must stay picklable
       (no live clients in __init__).
    4. ReportReadySensorAsync.execute() calls self.defer(...) and returns;
       the task moves to `deferred` state. When the trigger fires, Airflow
       reschedules a worker to run execute_complete with the event payload.
    5. pool="audit_api_pool" — a pool with N slots caps global concurrency
       against the API no matter how many DAGs call it; priority_weight
       decides who wins when the pool is contended.
    6. sla vs execution_timeout: SLA = business expectation (misses show in
       the SLA view / callback, task keeps running); execution_timeout
       actually kills the task.
    7. dag_instance.test() — runs all tasks in-process; the go-to answer
       for "how do you verify a DAG before deploying?"

CONCEPTS THE INTERVIEWER IS PROBING
    * Hook vs Operator vs Sensor vs Trigger responsibilities.
    * Why deferrable: poke mode = 1 worker slot held the whole time;
      reschedule = slot released but a scheduler slot per poke + DB churn;
      deferrable = near-zero cost while waiting. Many providers expose
      `deferrable=True` — writing one yourself is the senior version.
    * Where triggers run: a separate triggerer process (airflow triggerer),
      serialized via pickle; results are pushed back through the DB/redis.
    * Pools & priority_weight for downstream-system protection.

LIKELY FOLLOW-UP QUESTIONS
    Q: Task stuck in `queued` — debug?
    A: Check pool slots, then executor slots/workers, then
       parallelism/max_active_tis_per_dag, then scheduler heartbeat.
       Stuck in `deferred` → check the triggerer process is alive.
    Q: How do you unit-test a custom operator?
    A: Instantiate it, call execute(context) with a mocked hook; or run the
       whole DAG with dag.test() against a stubbed connection.
    Q: What runs on retries of execute_complete?
    A: execute_complete runs ONCE per trigger event; failures there follow
       normal task retry semantics.
================================================================================
"""
from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.base import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

log = logging.getLogger(__name__)


class AuditHook(BaseHook):
    """HOOK = reusable client built from an Airflow Connection."""

    def __init__(self, audit_conn_id: str = "audit_api_default") -> None:
        self.audit_conn_id = audit_conn_id

    def _base_url(self) -> str:
        conn = self.get_connection(self.audit_conn_id)  # host/login/password from the conn
        return f"https://{conn.host}:{conn.port or 443}"

    def get_report(self, report_id: str) -> dict:
        resp = requests.get(f"{self._base_url()}/reports/{report_id}", timeout=30)
        resp.raise_for_status()
        return resp.json()

    def is_ready(self, report_id: str) -> bool:
        return self.get_report(report_id).get("status") == "READY"


class RunAuditOperator(BaseOperator):
    """OPERATOR = one pipeline step; IO delegated to the hook."""

    template_fields = ("report_id",)          # Jinja-rendered at runtime

    def __init__(self, *, report_id: str, audit_conn_id: str = "audit_api_default", **kwargs):
        super().__init__(**kwargs)
        self.report_id = report_id
        self.audit_conn_id = audit_conn_id

    def execute(self, context: dict) -> str:
        report = AuditHook(self.audit_conn_id).get_report(self.report_id)
        self.log.info("audit %s status=%s", self.report_id, report["status"])
        return report["gcs_path"]              # return value → XCom


class ReportReadyTrigger(BaseTrigger):
    """Runs on the Triggerer (async, no worker slot, must stay picklable)."""

    def __init__(self, report_id: str, audit_conn_id: str, poll_seconds: int = 30):
        super().__init__()
        self.report_id = report_id
        self.audit_conn_id = audit_conn_id
        self.poll_seconds = poll_seconds

    async def run(self):                        # one asyncio task among thousands
        while True:
            ready = await asyncio.to_thread(   # blocking HTTP off the event loop
                AuditHook(self.audit_conn_id).is_ready, self.report_id
            )
            if ready:
                yield TriggerEvent({"report_id": self.report_id})
                return
            await asyncio.sleep(self.poll_seconds)


class ReportReadySensorAsync(BaseSensorOperator):
    """Deferrable sensor: execute() hands the task to the Triggerer."""

    template_fields = ("report_id",)

    def __init__(self, *, report_id: str, audit_conn_id: str = "audit_api_default", **kwargs):
        super().__init__(**kwargs)
        self.report_id = report_id
        self.audit_conn_id = audit_conn_id

    def execute(self, context: dict):           # never returns normally
        self.defer(
            trigger=ReportReadyTrigger(self.report_id, self.audit_conn_id),
            method_name="execute_complete",
            timeout=timedelta(hours=1),
        )

    def execute_complete(self, context: dict, event: dict | None = None) -> None:
        self.log.info("trigger fired: %s", event)


@dag(
    dag_id="audit_report_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["interview", "custom-operator"],
)
def audit_report_pipeline():
    wait = ReportReadySensorAsync(
        task_id="wait_for_audit_report",
        report_id="{{ ds_nodash }}_audit",
        pool="audit_api_pool",        # global concurrency cap for this API
        priority_weight=10,           # wins contended pool slots
        sla=timedelta(minutes=20),    # business expectation (≠ execution_timeout)
    )
    fetch = RunAuditOperator(task_id="fetch_audit_report", report_id="{{ ds_nodash }}_audit")

    @task
    def post_slack(gcs_path: str) -> None:
        log.info("audit report ready at %s", gcs_path)

    wait >> fetch >> post_slack(fetch.output)


dag_instance = audit_report_pipeline()

if __name__ == "__main__":
    dag_instance.test()   # executes every task in-process — no scheduler needed
