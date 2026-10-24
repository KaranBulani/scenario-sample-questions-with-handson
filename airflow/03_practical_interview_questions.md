# Airflow practical interview questions — scenario bank

Companion to the two hands-on DAG files in this folder:

- [`01_elt_dag_taskflow.py`](01_elt_dag_taskflow.py) — daily incremental ELT: TaskFlow, dynamic
  mapping, sensors, branching, trigger rules, catchup/backfill, idempotency.
- [`02_custom_operator_deferrable.py`](02_custom_operator_deferrable.py) — custom Hook +
  Operator + deferrable sensor: hooks vs operators vs triggers, Triggerer, pools,
  `priority_weight`, SLA vs timeout, `dag.test()`.

Those two files answer the "write me a DAG" classics; this bank covers what senior rounds
lean on beyond them: **incident debugging, cross-DAG orchestration, platform/scaling
decisions, and DAG hardening / multi-tenancy**. Practice by covering the answer column
and speaking the response aloud in 60–90 seconds.

---

## A. Design / live-coding rounds

### A1. Cross-DAG dependency — "DAG B must run after DAG A finishes, and A is owned by another team. How do you wire it?"

Strong answer hits:

- At least three options with trade-offs: `ExternalTaskSensor`, `TriggerDagRunOperator`
  with `wait_for_completion=True`, and **Datasets / data assets** (Airflow 2.4+, much
  richer in 3.x) as the preferred decoupled option.
- The classic `ExternalTaskSensor` trap: it looks for a task instance with the **same
  logical date**, so when the two DAGs have different schedules you must pass
  `execution_delta` or `execution_date_fn` or it pokes forever.
- Also mention `check_existence=True` (fail fast on typo'd ids), `mode="reschedule"` or a
  deferrable variant so the sensor doesn't hold a worker slot.
- Follow-up they always ask: *"what happens to B if A's run is skipped or never gets
  created?"* → sensor timeout behavior, `allowed_states`, and why datasets sidestep the
  date-alignment problem entirely.

### A2. Config-driven DAG generation — "We have 300 pipelines described in YAML config. Generate DAGs from it."

Strong answer hits:

- A top-level loop that builds a DAG per config entry via a factory function — but the
  senior part is knowing the cost: all of it runs on **every parse cycle** by every
  scheduler / DAG processor. Keep top-level code trivial (cached config, no API calls
  per parse), use `.airflowignore`, and rely on serialized-DAG caching
  (`min_serialized_dag_update_interval`).
- The stronger answer challenges the premise: **one parameterized DAG with dynamic task
  mapping (`.expand`) often beats 300 DAGs** unless teams need separate ownership,
  pausing, and alerting. Discuss when you genuinely need per-DAG isolation.
- Testing: assert on the generated `DagBag` (no import errors, expected dag_ids, count).

### A3. Late-arriving data — "Design incremental loading when data can arrive late."

Strong answer hits:

- Watermark / high-watermark table + `MERGE` (or partition overwrite), a **lookback
  window** that reprocesses the last N intervals, everything keyed off
  `data_interval_start` — never `datetime.now()`.
- Extends file `01`: be ready for the follow-up *"why did my backfill double the data?"*
  → non-idempotent write + `now()`-derived paths.

### A4. Fan-out sizing — "You discover 5,000 files at runtime and process each. Fan out to 5,000 task instances or loop in one task?"

Strong answer hits:

- Dynamic mapping gives per-unit retry, isolation, and UI visibility — but cap it with
  `max_active_tis_per_dag` and a pool so you don't starve the worker fleet.
- Batching (map over chunks of files) is the middle ground.
- Loop-in-one-task only when per-item retry visibility genuinely doesn't matter — you
  lose independent retries and granular state.

### A5. Backfill safety — "Make this DAG safe to backfill" (or its inverse: "this DAG isn't backfill-safe — why?")

Strong answer hits (file `01` covers this deeply — practice the 60-second version):

- `catchup=True` creates a run for every missed data interval; correctness therefore
  depends on deriving paths from `data_interval_start/end`, never from "now".
- Logical date = interval **start** (why the 03:00 run on Oct-2 processes Oct-1's
  interval).
- Idempotent partition-keyed writes / `MERGE` so re-running can never double-load.
- `max_active_runs=1` to serialize overlapping intervals for ordered loads.

---

## B. Incident / debugging rounds ("walk me through how you'd diagnose...")

These are the senior differentiators — a **structured triage path** matters more than
naming the single right answer.

### B1. "A DAG has been 'running' for an hour but no task instances appeared. What do you check, in order?"

Strong answer hits:

- Is the scheduler alive and is the DAG file parsing without errors (UI → Scheduler
  Status, DagFile processing)?
- Is the DAG paused, or has the run's data interval not closed yet?
- Are pool slots exhausted, or `max_active_runs` / `max_active_tasks` saturated by a
  backlog?
- Is the executor actually consuming the queue — Celery workers up / K8s pods
  schedulable?
- Close with commands: `airflow tasks states-for-dag-run <dag_id> <run_id>`, scheduler
  logs.

### B2. "A task shows 'running' for 6 hours; the worker process is long gone."

Strong answer hits:

- Zombie/reaper mechanics: task instances heartbeat; if heartbeats stop, the scheduler
  reaps the TI and fails it (`scheduler.zombie_*` settings).
- Contrast with the Celery orphan case (`task_adoption_timeout`) when a worker dies
  mid-task.
- The fix on your side: `execution_timeout` on every task so this never depends on the
  reaper noticing.

### B3. "Someone unpaused a DAG with `catchup=True` and a start_date two years back. You now have 730 queued runs and duplicates downstream. What do you do in the next 10 minutes?"

Strong answer hits:

- Pause the DAG; stop the run bleed (limit `max_active_runs`, or correct the schedule
  window / start_date).
- Decide clear-vs-mark per task based on that task's idempotency; use the re-run /
  reset tooling deliberately rather than blanket-clearing.
- Postmortem answer: idempotent writes and a shorter `start_date` / `catchup=False`
  default would have made this a non-event.

### B4. "The Airflow metadata DB is slow; webserver times out."

Strong answer hits:

- The big two causes: **XCom bloat** (people passing data instead of pointers → custom
  XCom backend on GCS/S3, enforce "metadata only") and **DAG-parse cost at scale**
  (thousands of DAGs × heavy top-level code → separate dag-processor, serialization
  tuning, splitting the DAG repo).
- DB maintenance (`airflow db clean`) and the rule that business logic never runs at
  top level.

### B5. "An `ExternalTaskSensor` has been poking for hours and the upstream DAG already ran."

Strong answer hits:

- The date-alignment trap (see A1): same logical date ≠ same wall-clock run when
  schedules differ → `execution_delta` / `execution_date_fn`.
- `check_existence=True` to fail fast on bad ids; `mode="reschedule"` or deferrable.
- "In greenfield I'd use datasets instead."

### B6. "A task wrote half the data, then the worker died mid-task. Retries succeeded. Now the table has duplicates. Fix the design."

Strong answer hits:

- At-least-once task execution is a given — retries can always re-run side effects.
- The fix is idempotent sinks: `MERGE` on natural key, partition overwrite, or
  delete+insert per interval; plus per-partition checkpointing so a retry resumes
  rather than blindly rewrites.
- Publish-side complement: the quality-gate / quarantine pattern from file `01`.

---

## C. Platform / scaling rounds

### C1. "Choose an executor for our workload and defend it."

Strong answer hits:

- The axes: Celery (steady worker pool, simple, fixed capacity), Kubernetes (elastic,
  per-task pod with right-sized resources — the natural answer when tasks are
  heterogeneous Spark/dbt jobs), hybrid (Celery workers + `KubernetesPodOperator` for
  the heavy stuff).
- Bring it back to admission control: pools + `priority_weight` so a 3-hour Spark job
  can't starve 30-second ops tasks.

### C2. "What happens when the scheduler dies mid-run? What about the triggerer?"

Strong answer hits:

- Scheduler death: already-queued tasks keep executing on workers (execution is
  decoupled from scheduling); on restart the scheduler reconciles state from the
  metadata DB.
- Multiple schedulers are active-active since 2.3 — DB contention is the scaling limit.
- Triggerer death: deferred tasks stay deferred; a restarted / HA triggerer
  re-instantiates triggers from the DB and fires them (chains off file `02`).

### C3. "How do your DAGs get from a PR to production?"

Strong answer hits:

- CI: import/parsing check, `dag.test()` / `airflow dags test` smoke run, unit tests on
  the plain-Python task logic (that's why TaskFlow matters — the logic is testable
  without Airflow).
- CD: git-sync / object-store sync, canary environment; in 3.x, DAG-only deployments
  that don't touch the scheduler deployment.
- Config: connections/variables per environment from a **secrets backend** (Secret
  Manager / Vault), never baked into images or exported between envs.

### C4. "We're on Airflow 2.x. What does moving to 3.x change for us?"

Strong answer hits:

- Headline features: datasets upgraded to **data assets** with conditional outputs, DAG
  versioning, the FastAPI/React rewrite, DAG-only deployments, removal of the
  `backfill` CLI, stricter scheduler / DAG-processor separation.
- The senior angle: migration risk (deprecated provider APIs, behavioral changes), and
  staging the move via the compatibility release first.

---

## D. Quick-fire traps (instant, correct answers expected)

- **`sla` vs `execution_timeout`**: SLA only *records/alarms* a miss (callback); it
  never kills a task — `execution_timeout` does.
- **`depends_on_past`**: only chains consecutive runs, which forces serial backfills
  and breaks parallelism — rarely what people actually want.
- **Skip vs fail**: `AirflowSkipException` for "no data today" so on-call doesn't get
  paged; trigger rule `NONE_FAILED_MIN_ONE_SUCCESS` to join branches where one side was
  skipped (file `01`'s `summarize`).
- **Retries**: `retries` + `retry_exponential_backoff` with `max_retry_delay`;
  DAG-level `default_args` retries are inherited but overridable per task.
- **Concurrency knobs**: `default_pool` has 128 slots; know `parallelism` vs
  `max_active_tasks` vs `max_active_tis_per_dag` and which one actually gates a mapped
  task storm.

---

## E. DAG hardening / multi-tenancy

The "DAG code is semi-trusted code" questions — what DAG *code* is and isn't allowed to
do, and how to enforce it. Framing that scores points: **UI RBAC is irrelevant to task
code** — tasks run inside the worker process with the worker's credentials; no Airflow
permission system intercepts what Python does at runtime.

### E1. "How do you prevent a DAG from modifying Variables — it can fetch, but not update?"

The answer is layered:

1. **Make the metadata-DB copy irrelevant (secrets backend / env vars).**
   Variable lookup order is: `AIRFLOW_VAR_*` environment variables → configured secrets
   backend → metadata DB. `Variable.set()` only ever writes the metadata-DB row. So if
   you front variables with the env-var backend or a read-only secrets backend (on GCP:
   Secret Manager with workers granted only `roles/secretmanager.secretAccessor` — no
   version-manager role):
   - `Variable.get("x")` resolves from the backend — reads work everywhere.
   - A rogue `Variable.set("x", ...)` writes a DB row that is *shadowed forever* — and
     if the config lives in Secret Manager, IAM makes real mutation impossible even in
     principle.
   - The `EnvironmentVariablesBackend` pattern is the cleanest variant: variables are
     injected at deploy time by the platform team, so there is nothing mutable at
     runtime at all.
2. **Ban the write in CI.**
   A pre-commit / CI lint rule (grep or a small ruff/flake8 plugin) rejecting
   `Variable.set(` / `Variable.delete(` / `Variable.update(` in DAG repos. This is what
   real platform teams actually rely on in a shared deployment, because Airflow has no
   native per-DAG ACL on the Variable model.
3. **(Optional, hacky but real) Import-time guard.**
   Ship a platform module imported at parse time (via `airflow_local_settings` or a
   plugin) that wraps `airflow.models.Variable.set` to raise. Worth *mentioning* as "we
   evaluated it; CI + backend shadowing was less fragile" — knowing the trade-off is
   the senior signal.

Bonus points to drop:

- Top-level `Variable.get()` calls hit the metadata DB on *every parse cycle* — fetch
  inside tasks, or pull a list once and cache.
- `Variable.get(..., deserialize_json=True)` for structured config.

### E2. "How do you ensure a DAG can be triggered only by one specific DAG — not manually, not by API, not by other DAGs?"

Enumerate the trigger paths first (that's the structured part the interviewer wants):
timetable, manual UI, REST API, CLI, `TriggerDagRunOperator` from any DAG, dataset
updates. Then close each one:

- **Timetable**: `schedule=None` — the DAG never schedules itself.
- **Humans / API**: DAG-level RBAC — give everyone only `can_read` on that DAG (no
  edit, so no Trigger button) and don't grant the REST `dag-runs:create` permission or
  API tokens.
- **Datasets**: simply don't declare a dataset schedule.
- **Other DAGs** — the weak spot: **Airflow has no native DAG-to-DAG authorization**.
  Any DAG can point a `TriggerDagRunOperator` at any `dag_id`. So:
  - *Governance*: a CI rule that `TriggerDagRunOperator` for this `dag_id` is allowed
    only in the parent's file.
  - *Runtime guard*: the child's first task validates provenance. Airflow 3 exposes
    `dag_run.triggered_by` (an enum distinguishing REST API / CLI /
    TriggerDagRunOperator / timetable / asset). In Airflow 2 the equivalent flag
    (`external_trigger`) is `True` for *both* manual and operator triggers, so you
    can't distinguish mechanism — pass an explicit `dag_run_id` / `conf` token from the
    parent and assert it:

    ```python
    @task
    def assert_authorized_parent():
        ctx = get_current_context()
        dr = ctx["dag_run"]
        # 3.x: check dr.triggered_by is TriggerDagRunOperator
        # 2.x: parent passes conf; manual/UI triggers can't know it
        if dr.conf.get("parent_dag") != "ingestion_master":
            raise AirflowException("Only ingestion_master may trigger this DAG")
    ```

- **The hard guarantee**: if "exactly one parent" must be *enforced* rather than
  guarded, the honest architectural answer is either merge the two into one DAG
  (TaskGroups) or put the child in a separate Airflow deployment where only the
  parent's service account has REST trigger rights. Within a shared scheduler,
  isolation between DAGs is convention + guards, not a security boundary — saying that
  out loud is the senior answer.

---

## F. Reusable prompt for this question class

Prepend this whenever you ask any assistant for interview questions, to keep the
hardening / multi-tenancy category covered:

> For Airflow (or any tool), also include a **"DAG hardening / multi-tenancy"**
> category of practical questions: what DAG *code* is and isn't allowed to do, and how
> to enforce it. Cover things like making Variables/Connections read-only to DAG
> authors, restricting who/what can trigger a specific DAG (including cross-DAG
> triggers), scoping pool/resource usage per team, restricting secrets access, and
> top-level code rules. For each question give the **layered real-world answer**: what
> Airflow enforces natively at runtime (RBAC, schedule, pools), what needs CI/lint
> governance, what needs IAM or a secrets backend, and what is only achievable with
> separate deployments — plus Airflow 2 vs 3 differences where they matter.

The key phrase doing the work is *"layered real-world answer: native runtime
enforcement vs CI governance vs IAM vs deployment isolation"* — it forces coverage of
the enforce-vs-convention distinction that the hardening questions test.
