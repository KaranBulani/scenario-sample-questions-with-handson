# Spark practical interview questions — scenario bank

Companion to the two hands-on Spark files in this folder:

- [`01_batch_sessionization.py`](01_batch_sessionization.py) — batch sessionization:
  `row_number` dedup, `lag` + running-sum session boundaries, top-N per group, shuffle
  counting, skew/salting, cache, AQE.
- [`02_structured_streaming_watermark.py`](02_structured_streaming_watermark.py) —
  streaming dedup + windowed counts: watermark, `dropDuplicatesWithinWatermark`,
  output modes, `foreachBatch` + MERGE idempotent sink, checkpointing, triggers.

Those two files cover sessionization and watermark *basics*; this bank deliberately
skips both and goes deeper: incident diagnosis paths, restart/recovery semantics past
the basics, design trade-offs, and hardening/multi-tenancy. Practice by covering the
answer and speaking the response aloud in 60–90 seconds. Every fix is framed as the
**layered real-world answer**: fix in the code vs job-level config (`spark.*`
properties, executor memory, shuffle partitions) vs cluster-level config vs
data-layout/architecture change — plus Spark 2.4 vs 3.x differences where they matter.

---

## 1) Performance debugging

### Q1 — Skewed join (the 40-min-to-4-hour incident)

**Incident:** "The daily join ran 40 min yesterday, 4 h today after a new partner's
data landed. One task in stage 5 shows 12 GB shuffle read; the median task shows
80 MB. Walk me through your diagnosis, in order."

- **Diagnosis:** Spark UI → stage detail → sort tasks by shuffle read size, compare
  max vs median. Then confirm the distribution:
  `df.groupBy("key").count().orderBy(desc("count"))`. Always check for nulls and
  sentinel keys (`-1`, `"UNKNOWN"`) — they hash to one partition together.
- **Code:** filter nulls/sentinels before the join; broadcast the other side if it
  fits (kills the shuffle entirely); salt the hot keys for a join, or two-phase salt
  for a skewed aggregation (salt → local aggregate → unsalt → final aggregate).
- **Job config:** enable AQE skew handling (`spark.sql.adaptive.enabled` +
  `skewJoin.enabled`) — splits skewed partitions of a sort-merge join at runtime.
- **Architecture:** pre-aggregate the hot partner in BigQuery before it hits Spark,
  or split the top-N hot keys into a separate code path.
- **2.4 vs 3.x:** AQE is 3.0+ (default-on since 3.2) and only handles *joins* — a
  skewed `groupBy` still needs salting in every version.
- **Follow-up trap:** *"why not just raise shuffle partitions?"* → the hot key still
  hashes to exactly one partition regardless of count.

### Q2 — Shuffle spill

**Incident:** "Task metrics show millions of spilled records; the job is
disk-IO-bound in what should be a compute step."

- Spill means partitions too big for executor memory — the opposite problem of
  too-many-tiny-partitions.
- **Job config:** raise `spark.sql.shuffle.partitions` (default 200; target ~128 MB
  per partition); more executor memory/overhead.
- **Code:** column pruning and predicate pushdown before the shuffle;
  `groupBy(...).agg()` instead of `groupByKey`/`collect_list` when you can aggregate
  early; broadcast a dimension to remove a shuffle stage entirely.
- **Architecture:** write the intermediate partitioned/sorted so the next job
  shuffles less; cache *after* the expensive step, not before it.

### Q3 — Container kills with PySpark UDFs

**Incident:** "Executors die with 'Container killed by YARN for exceeding memory
limits', exit 137/143. The job is full of Python UDFs."

- **Diagnosis first:** YARN kills the *container*, whose size is
  `spark.executor.memory` + `spark.executor.memoryOverhead` (default
  max(384 MB, 10%)). The Python worker in PySpark lives **inside overhead** —
  UDF-heavy jobs with default overhead are the classic kill.
- **Job config:** raise `spark.executor.memoryOverhead` (2–4 GB is normal for
  pandas-UDF jobs); `spark.executor.pyspark.memory` as the 3.x lever; if it's
  *virtual* memory kills, `yarn.nodemanager.vmem-check-enabled=false`.
- **Code:** replace Python UDFs with built-in expressions (Catalyst-optimizable), or
  pandas UDFs with Arrow (10–100× less serialization); watch driver OOM separately —
  `collect()`, and the broadcast build side, which is assembled **on the driver**,
  are the usual suspects.
- **Cluster:** on Dataproc set these as `spark:spark.executor.memoryOverhead`
  properties; distinguish JVM OOM (exception in executor logs) from a YARN kill (no
  heap trace, SIGTERM/exit 143 vs 137).

### Q4 — Cache that doesn't stick

**Incident:** "You cached the DataFrame, but the next action recomputes it / the
Storage tab keeps shrinking during the run."

- `.cache()` is lazy; a DataFrame caches as `MEMORY_AND_DISK`, and storage memory is
  the **unified** pool — shuffle/aggregation *execution* pressure evicts cached
  blocks (to disk at that storage level; to recompute at `MEMORY_ONLY`).
- Reliable full materialization: `df.write.format("noop").save()` — a bare
  `.count()` can be optimized against column pruning and not warm the whole cache.
- **Layered fix:** `unpersist()` everything you're done with; `persist(DISK_ONLY)`
  for big intermediates so execution keeps RAM; `checkpoint()` when the *lineage*,
  not the data, is the cost (repeated recompute of a long plan).
- Trap to state out loud: too many cached DataFrames evict each other; caching before
  a filter caches the *unfiltered* size.

### Q5 — Straggler tasks

**Incident:** "P95 task time is 3 minutes; max is 45, on roughly equal input sizes."

- Check in order: shuffle-read/input size skew (it's Q1 again), GC time in task
  metrics, one degraded node (task details → host; check locality + disk), then
  external calls inside tasks (API rate limits).
- **Mitigation:** `spark.speculation=true` (with quantile threshold) reruns slow
  tasks — **only safe if task output is idempotent** (ties into Q13).
- Version note: nothing 2.4/3.x-specific here — the trap is reaching for AQE, which
  only fixes shuffle-stage skew, not slow readers or GC.

---

## 2) Correctness & recovery (past the watermark basics)

### Q6 — What restart actually does

**Ask:** "Batch job with a 40-step lineage fails at step 38. Streaming job restarts
after a week. Explain exactly what recomputes and what resumes, and what code
changes break the restart."

- Batch: restart = recompute from the last shuffle boundary / checkpoint / cached
  block; `df.checkpoint()` truncates lineage into a reliable RDD in
  `setCheckpointDir` (HDFS/GCS); `localCheckpoint()` truncates but is lost with the
  executor — say why that's a footgun.
- Streaming: the checkpoint holds **committed offsets + state store versions + task
  commits**; on restart the source replays from committed offsets and state is
  restored (HDFSBackedStateStore re-reads its version files — restore time grows
  with state size; the RocksDB provider (3.2+) and changelog checkpointing in recent
  3.x cut this).
- Compatibility: additive changes are fine; changing state schema / aggregation keys
  fails the state-schema compatibility check at restart — you're into
  new-checkpoint-dir + batch-backfill territory. `Trigger.AvailableNow` is 3.3+.

### Q7 — The at-least-once MERGE incident (deeper than foreachBatch basics)

**Incident:** "Your streaming `foreachBatch` MERGE into BigQuery intermittently
fails with *'UPDATE/MERGE must match at most one source row for each target row'*
right after Kafka rebalances."

- Root cause: rebalance → batch redelivered → the same natural key twice in one
  source batch → multi-match MERGE. Offsets commit only *after* `foreachBatch`
  returns, so re-execution of the whole function is expected behavior, not a bug.
- **Code fix:** dedup inside the batch before the MERGE, and guard against
  regression on redelivery:

  ```python
  dedup = (df.withColumn("rn", row_number().over(
               Window.partitionBy("natural_key").orderBy(desc("event_ts"))))
           .where("rn = 1").drop("rn"))
  # MERGE ... ON t.natural_key = s.natural_key
  # WHEN MATCHED AND t.processed_ts < s.event_ts THEN UPDATE ...
  ```

- Two MERGEs inside one `foreachBatch` = no cross-statement atomicity: if the second
  fails, both re-run — make both idempotent, or stage-then-merge.
- **Layered:** code-level dedup + regression guard vs architecture-level (staging
  table + single transactional statement in BigQuery).

### Q8 — State store creep

**Incident:** "The dedup streaming job's batch duration has climbed steadily since
launch; checkpoint state files keep growing."

- Diagnosis: unbounded state — dedup keys never expire. Monitor
  `stateOperators.numRowsTotal` in the streaming query listener.
- **Layered:** watermark-bounded dedup (`dropDuplicates` under a watermark,
  `dropDuplicatesWithinWatermark` in 3.5) or TTL-style eviction via
  `flatMapGroupsWithState`; switch the state store to RocksDB
  (`spark.sql.streaming.stateStore.providerClass`) when state hits GBs; more shuffle
  partitions → state is partitioned by key, so more partitions spread the load; last
  resort: new checkpoint dir + batch backfill to reset state.

---

## 3) Design

### Q9 — Idempotent incremental load (live-coding)

**Ask:** "Daily incremental GCS→BigQuery load, late data, must be safe to re-run."

- Watermark column + lookback window (re-read the last N intervals);
  partition-keyed writes so re-runs replace, never append:

  ```python
  spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
  df.write.mode("overwrite").insertInto("db.events")   # replaces only touched partitions
  ```

- Partial-update semantics need MERGE instead (Q7 pattern); control small files with
  `repartition(n)` before the write plus a periodic compaction job.
- Trap: `insertInto` vs `saveAsTable` differ in partition-overwrite behavior — know
  which one you're calling.

### Q10 — Salting vs bucketing vs broadcast vs AQE

**Ask:** "When do you actually pick each?"

- **Broadcast** first if a side fits (~auto threshold 10 MB, hard ceiling = driver +
  executor memory) — removes the shuffle and makes skew irrelevant.
- **AQE skew join** (3.x) is the default join-skew mitigation; salting when the skew
  is in an *aggregation* or AQE's split isn't enough; bucketing only for stable,
  big, repeatedly-joined tables written once — both sides bucketed on the join key —
  and it's high-maintenance: it predates AQE, and AQE won't coalesce bucketed
  shuffles.
- Senior signal: state that bucketing is a last resort on GCP stacks — Parquet on
  GCS + AQE + salting covers most cases with less operational debt.

### Q11 — "Convince me NOT to use Spark for this"

- Under ~tens of GB or purely SQL-expressible → BigQuery (no cluster to size;
  on-demand $/TB usually beats running Dataproc for it).
- File-move EL → Dataflow templates / storage transfer / Airflow sensors; frequent
  small transforms → BigQuery scheduled queries.
- Spark earns its keep for: custom/UDF-heavy logic, ML, complex stateful streaming,
  cross-source joins needing code.
- Cost framing: Dataproc Serverless per-batch vs an idle long-lived cluster is the
  modern version of this answer.

---

## 4) Hardening & multi-tenancy

### Q12 — The job that starves the cluster

**Incident:** "One team's unbounded batch makes everyone queue in YARN on the
shared Dataproc cluster."

- **Job level:** `spark.dynamicAllocation.maxExecutors` cap; sane `repartition`;
  FAIR scheduling *within* an app via
  `sc.setLocalProperty("spark.scheduler.pool", ...)`.
- **Cluster level:** YARN capacity/fair scheduler queues per team with caps and
  preemption (`spark.scheduler.mode=FAIR`, queue configs in Dataproc properties);
  verify the external shuffle service is on, or dynamic allocation drops shuffle
  data on executor removal and forces recomputation.
- **Architecture:** separate clusters or Dataproc Serverless batches per team = hard
  isolation (the money quote: caps and queues are cooperative; only separate
  capacity/environments is a guarantee).

### Q13 — Which retries are safe when tasks have side effects

- Spark retries *tasks* automatically → any non-idempotent sink (raw DB inserts,
  external API calls) can apply twice. Speculative execution multiplies this —
  enable it only with idempotent output.
- File sinks: FileOutputCommitter v1 (job-level commit, safe, slower) vs v2
  (task-level, faster, but with retries/speculation you're trusting idempotent task
  output) — `mapreduce.fileoutputcommitter.algorithm.version`.
- External DBs: idempotent upserts keyed by natural key, batch-id versioning, or
  stage files + one transactional load. `foreachPartition` opening connections is
  fine; just assume the partition *will* re-run.
- The Kafka sink is documented at-least-once — "effectively once = replayable
  source + idempotent sink" is the correct mental model to quote.

---

## Quick-fire traps

- `cache()` = `persist(MEMORY_AND_DISK)` for DataFrames; `persist()` is where you
  pick `DISK_ONLY` etc. (RDD `cache()` is `MEMORY_ONLY` — the asymmetry is the
  trap).
- Narrow vs wide = stage boundaries: `map`/`filter` narrow; `groupBy`/`join`/
  `repartition` wide; `coalesce` is narrow, `repartition` is wide — that's why
  `coalesce` can't increase parallelism.
- `foreach`/`foreachPartition` are *actions* — side effects re-execute on task
  retry.
- Broadcast joins are assembled on the **driver** — a too-big "small" table OOMs the
  driver, not the executors.
- UDFs are opaque to Catalyst — no pushdown, no predicate optimization; pandas UDFs
  fix speed, not optimizer visibility.
- Default `spark.sql.shuffle.partitions` = 200; default broadcast threshold =
  10 MB — know both cold.
