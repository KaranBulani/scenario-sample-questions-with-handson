# Scenario-based sample questions with hands-on code

Interview prep for a GCP data-engineering stack. One file per classic scenario:
a **50–100 line implementation** followed by a **structured explanation**
(the ask, walkthrough, concepts probed, likely follow-up Q&A) in the module
docstring. Read the header first, then the code — each file stands alone.

| File | Scenario | Key topics |
|---|---|---|
| `airflow/01_elt_dag_taskflow.py` | Daily incremental ELT DAG | TaskFlow API, dynamic task mapping (`.partial`/`.expand`), XComs, `@task.branch`, sensors (`reschedule`), catchup/backfill, idempotency, trigger rules |
| `airflow/02_custom_operator_deferrable.py` | Custom Hook + Operator + deferrable sensor | Hook vs Operator vs Trigger responsibilities, `self.defer`, Triggerer, pools, `priority_weight`, SLA vs timeout, `dag.test()` |
| `airflow/03_practical_interview_questions.md` | Practical/scenario Q&A bank (all rounds) | design & live-coding, incident debugging, executor/HA/CI-CD, quick-fire traps, DAG hardening (read-only Variables, single-parent trigger control) |
| `bigquery/01_practical_interview_questions.md` | Practical/scenario Q&A bank (all rounds) | cost & performance (partitioning/clustering, proving pruning, editions vs on-demand, `maximumBytesBilled`, materialized views), query debugging (execution details, skewed joins, broadcast vs hash, shuffle-heavy stages), correctness (idempotent MERGE on retry, streaming-buffer caveats, time travel/snapshots, concurrent-DML quotas), hardening & multi-tenancy (authorized views/datasets, policy tags, row access policies, per-team reservations, custom quotas, killing full scans) |
| `gcs/01_practical_interview_questions.md` | GCS scenario Q&A bank (senior rounds) | resumable/retried uploads + `ifGenerationMatch` idempotency, generation dedup, strong consistency, notifications → Pub/Sub at-least-once, hive-prefix pruning for BQ/Spark, storage classes + Autoclass + lifecycle, retention/Bucket Lock, UBLA/PAP/per-pipeline SAs/CMEK, quick-fire traps |
| `spark/01_batch_sessionization.py` | Clickstream sessionization (batch) | `row_number` dedup, `lag` + running-sum session boundaries, top-N per group, shuffle counting, skew/salting, cache, AQE |
| `spark/02_structured_streaming_watermark.py` | Streaming dedup + windowed counts | watermark, `dropDuplicatesWithinWatermark`, output modes, `foreachBatch` + MERGE idempotent sink, checkpointing, triggers |
| `spark/03_practical_interview_questions.md` | Practical/scenario Q&A bank (all rounds) | skew/spill/OOM/GC diagnosis, checkpoint & state-store restart semantics, at-least-once MERGE hardening, idempotent incremental loads, salting vs bucketing vs AQE, fair-share/multi-tenancy, retry safety |
| `dataflow/01_streaming_pubsub_windows.py` | Pub/Sub → windows → BigQuery + DLQ | side outputs / dead-letter, event-time re-stamping, `FixedWindows`, `AfterWatermark` triggers, accumulation, `allowed_lateness`, `CombinePerKey` |
| `dataflow/02_batch_join_side_inputs.py` | Batch join + enrichment | `CoGroupByKey` left-join semantics, `AsDict` side inputs, DoFn lifecycle, partitioned/clustered `WRITE_TRUNCATE` loads |
| `dataproc/01_practical_interview_questions.md` | Practical/scenario Q&A bank (infra round) | cluster design & sizing (primary vs spot secondaries, autoscaling, graceful decommission), cost & architecture (shared vs ephemeral vs Serverless, idle shutdown, when to use BigQuery), ops (image upgrades, init actions vs custom images, Dataproc Metastore, workload identity, YARN-UI/component-gateway debugging), hardening & multi-tenancy (YARN queues, OOM containment, driver placement), quick-fire traps |
| `fastapi/01_crud_catalog_api.py` | Data-catalog CRUD API | lifespan, DI + per-request caching, Pydantic v2 request/response models, pagination, `BackgroundTasks`, healthz/readyz, async-vs-sync endpoints |
| `fastapi/02_jwt_streaming_testing.py` | JWT-secured metrics API | OAuth2 password flow, dependency factory for roles, 401 vs 403, NDJSON `StreamingResponse`, middleware, `TestClient` + `dependency_overrides` |
| `fastapi/03_ingestion_service.py` | Ingestion service (API client side) | pagination via async generator, exponential backoff + jitter + `Retry-After`, cursor checkpointing / incremental sync, `asyncio.gather` + `Semaphore` fan-out, `run_in_executor` for blocking SDKs, mocked pytest |
| `python/01_rate_limiter_lru_toposort.py` | Mini-system coding round | token-bucket rate limiter, LRU cache via `OrderedDict`, Kahn's topological sort + cycle detection, thread safety, monotonic time |

## Quick start

```bash
# Spark #1 is fully self-contained:
pip install pyspark && python spark/01_batch_sessionization.py

# Pure-Python mini-systems (stdlib only, no install at all):
python python/01_rate_limiter_lru_toposort.py

# FastAPI:
pip install "fastapi[standard]" && uvicorn 01_crud_catalog_api:app --reload   # from fastapi/

# FastAPI #2/#3 tests:
pip install "fastapi[standard]" pyjwt python-multipart httpx && pytest fastapi/02_jwt_streaming_testing.py fastapi/03_ingestion_service.py -v

# Dataflow (local DirectRunner; see each file's HOW TO RUN for DataflowRunner flags):
pip install "apache-beam[gcp]"

# Airflow (DAG files — load under $AIRFLOW_HOME/dags/, or run file #2 directly):
pip install apache-airflow apache-airflow-providers-google
```

Every file's docstring contains its own **HOW TO RUN**, **IMPLEMENTATION
WALKTHROUGH**, **CONCEPTS THE INTERVIEWER IS PROBING**, and **LIKELY
FOLLOW-UP QUESTIONS** — revise from the headers, practice from the code.
