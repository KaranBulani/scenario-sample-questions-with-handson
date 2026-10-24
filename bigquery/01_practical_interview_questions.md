# BigQuery practical interview questions — scenario bank

Companion to the rest of this repo (the Airflow DAG files and
[`airflow/03_practical_interview_questions.md`](../airflow/03_practical_interview_questions.md)
cover the orchestration side of the same interviews). This bank covers the four
areas senior GCP rounds lean on for the warehouse itself:

- **A. Cost & performance** — partitioning/clustering design, *proving* pruning,
  editions/reservations vs on-demand, `maximumBytesBilled` guardrails, materialized views.
- **B. Query debugging** — reading execution details, skewed joins, broadcast vs
  hash joins, finding the shuffle-heavy stage.
- **C. Correctness** — idempotent MERGE on retry, streaming-buffer caveats,
  time travel / snapshots for rollback, concurrent DML & table-update quotas.
- **D. Hardening & multi-tenancy** — authorized views/datasets, policy tags,
  row-level security, per-team reservations, custom quotas, killing full scans.

Every question is phrased as an incident ("walk me through the diagnosis") or a
design ask, and every strong answer is **layered** — the rubric the interviewer
is scoring against:

1. **SQL / schema design** — the query itself (predicates, join shape, dedup).
2. **Table / dataset config** — partitioning, clustering, `require_partition_filter`,
   expiration, table settings.
3. **IAM / policy tags** — who can see which rows/columns at all.
4. **Org-level quotas / reservations** — what the platform enforces centrally.
5. **Architecture** — staging tables, serving layers, storage write patterns.

Practice by covering the answer and speaking for 60–90 seconds. Exact quota and
price numbers drift — quote the *mechanism* confidently and flag "numbers move,
here's how I'd verify".

---

## A. Cost & performance

### A1. "Our dashboard query cost 10x'd overnight after a 'harmless' WHERE-clause tweak. Walk me through the diagnosis."

Strong answer is a **structured triage path**, not a guess at the cause:

1. **Confirm and scope with job history, not the UI tab.** Query
   `INFORMATION_SCHEMA.JOBS_BY_PROJECT` for the last week, sort by
   `total_bytes_processed`, diff the `query` text of yesterday's cheap run vs
   today's expensive one:

   ```sql
   SELECT job_id, user_email, creation_time,
          total_bytes_processed / POW(1024, 4) AS tib_scanned,
          total_slot_ms, cache_hit, state
   FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
   WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
     AND job_id LIKE some_pattern  -- or filter by user / referenced_tables
   ORDER BY total_bytes_processed DESC
   LIMIT 50;
   ```

2. **The usual suspects, in likelihood order:**
   - A function now wraps the partition column — `DATE(ts) = ...`,
     `TIMESTAMP_TRUNC(..., 'DAY')` with a timezone, `FORMAT_TIMESTAMP` — so the
     predicate is no longer a bare column compare and **pruning dies**.
   - The filter moved into a join/subquery (filter on a dimension table's column
     instead of the fact's partition column) — pruning needs compile-time
     constants, and a join-derived value isn't one.
   - `SELECT *` appeared or new columns got referenced — on-demand bills **bytes
     of the columns scanned** (columnar), so projection matters.
   - Someone recreated the table (`CREATE OR REPLACE TABLE ... AS SELECT`)
     **without** re-declaring partitioning/clustering.
   - The query stopped hitting the result cache (non-deterministic function like
     `CURRENT_TIMESTAMP()`, or a table write invalidated it).
3. **Prove pruning happened (or didn't)** — the step that separates seniors:
   - Dry-run the query (`bq query --dry_run`) and read the bytes-scanned estimate.
   - Compare that against the expected partitions' size:

     ```sql
     SELECT partition_id, total_logical_bytes
     FROM `proj.ds.INFORMATION_SCHEMA.PARTITIONS`
     WHERE table_name = 'events'
     ORDER BY partition_id;
     ```

     Dry-run bytes ≈ sum of targeted partitions → pruning worked. Dry-run bytes ≈
     whole table → it didn't. In the console's **Execution details**, the stage's
     "Read" bytes tells you the same thing post-hoc.

**Layered fix:** SQL — bare partition-column predicate, explicit column lists.
Table config — `require_partition_filter=TRUE` so an unpruned query *errors*
instead of silently scanning. Org — custom quotas / budgets so one bad query
can't 10x a day of spend (see A4/D5).

**Follow-ups they always ask:** *"How do you catch this before it runs?"* →
dry-run in CI for scheduled/BI queries, `maximumBytesBilled` as the hard stop,
budgets with Pub/Sub alerts for the rest. *"Does clustering help here?"* → only
with a prefix filter on the cluster columns; it's a second line, not a
replacement for partition pruning.

---

### A2. "Design the physical layout: ~5 PB/year of raw events, queries filter by event date, always by tenant, often by user_id and event_type."

Strong answer hits — this is where you narrate the *decision*, not the DDL:

- **Partition by event date, not ingestion date.** Queries are event-time
  scoped; ingestion-time partitioning (`_PARTITIONTIME`) forces every consumer
  to reason about arrival time instead, and late data lands in the wrong
  partition. Use a `DATE` column derived from the event timestamp. Discuss late
  data explicitly: cluster + a lookback window handles it; strict daily
  partitioning of *curated* tables with a merge (see C1) handles restatement.
- **Clustering order is a prefix decision:** `CLUSTER BY tenant_id, user_id,
  event_type`. The **first column gets the strongest pruning**; filtering only
  on a later column gives little. Order by filter frequency/selectivity across
  the workload — tenant first if almost every query is tenant-scoped, user_id
  first if cross-tenant user lookups dominate.
- **Config that makes it stick:** `require_partition_filter=TRUE` (nobody full-
  scans by accident), partition expiration on raw (e.g. 13 months) with the
  curated layer kept longer, and consider `partition_expiration_days` vs table
  expiry interaction.
- **Constraints to name unprompted:** 4,000 partitions per table by default
  (raisable to ~10,000) — daily granularity over years is fine, *hourly* is not.
  Don't over-partition: thousands of near-empty partitions cost metadata and
  scan-floor overhead. Clustering has a practical floor (~tables under a GB
  gain nothing). Clusterable types only — no `FLOAT64`, no `STRUCT`/`ARRAY`.
- **Architecture layer:** raw (cheap, long retention) → curated marts (clustered
  for the access patterns, shorter retention) → BI Engine / MVs for serving.

**Follow-ups:** *"A query filters only by user_id across all dates — what
happens?"* → full partition scan, but clustering on `user_id` (if it's early in
the prefix) prunes blocks — this is exactly the trade-off that decides cluster
order. *"Integer-range partitioning?"* → for id-range lookups, not time series.

---

### A3. "We're paying ~$70k/month on-demand for 40 analysts plus nightly pipelines. Should we buy slots? Defend the math."

The senior move: **derive the answer from data, not vibes.**

1. **Pull the workload shape** from job history:

   ```sql
   SELECT TIMESTAMP_TRUNC(start_time, HOUR) AS hr,
          SUM(total_slot_ms) / 3600000 AS avg_slots_used,  -- slot-hours/hr ≈ avg slots
          SUM(total_bytes_processed) / POW(1024, 4)        AS tib_processed
   FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
   WHERE start_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
   GROUP BY hr ORDER BY hr;
   ```

   Look at steady-state (pipelines, committable), p95 spikes (ad-hoc), and the
   bytes/slot-hour ratio.
2. **The trade:** on-demand bills **bytes** (~$6.25/TiB, first 1 TiB/month
   free); editions/reservations bill **slot-time** whether used or not. A
   commitment for `N` baseline slots is worth it when your consistent load
   would amortize `N` × slot-hour price below the bytes bill — heavy, steady,
   scan-efficient pipelines almost always win; spiky ad-hoc rarely does alone.
3. **BigQuery Editions specifics:** pick Standard/Enterprise(/Plus) by feature
   needs, capacity commitment (term = cheaper per slot-hour; shorter/flex =
   walk-away), then **reservations carve the commitment up** and
   **assignments** route projects/folders to them. Autoscaling adds slots
   above baseline on demand, billed per slot-second — that's the answer to the
   spike problem without over-committing.
4. **Hybrid is legitimate:** put the pipeline project on a reservation, leave
   the analyst project on-demand (assignment is per project — a project is
   either on a reservation or on-demand, not both).
5. **Name the failure modes:** reservation saturation → queries queue and
   wall-clock explodes even though bytes are unchanged; commitments are
   **region-scoped** and billed idle; commitment ≠ reservation ≠ assignment
   (paying ≠ carved ≠ routed).

**Follow-ups:** *"How would you size the pipeline reservation?"* → peak-hour
`avg_slots_used` + headroom, watch queue depth. *"What breaks when analysts
starve pipelines?"* → see D4 for the per-team carve-up.

---

### A4. "An analyst's exploratory query just billed $400 in one shot. 'Be careful' is not an answer. Make it structural."

Layered defense — name all of these, best candidates lead:

- **Per-query hard stop: `maximumBytesBilled`.** Job-level config
  (`bq query --maximum_bytes_billed=...`, `maximum_bytes_billed` in the client
  libraries, query settings in the console). The job **fails at submission**
  if the estimate exceeds it — it errors, never truncates. Make it a default in
  every scheduled-query and client tool config; the console setting alone is
  per-project/user and skippable.
- **Org-level: BigQuery custom quotas** — per-project and per-user daily
  on-demand usage caps (TiB/day) set by admins; once exhausted, queries are
  rejected. Blunt (it's a kill, not a throttle) — pair with **budgets →
  Pub/Sub alert** at 50/80% so people get warned before the wall.
- **Table config:** `require_partition_filter=TRUE` on big partitioned tables
  so *any* query is at least partition-bounded; a `SELECT *` can still scan all
  columns of one partition, but not five years.
- **IAM:** analysts never hold `getData` on the raw billing-scale tables at
  all — they see curated marts and authorized views (D1/D5). The cheapest scan
  is the one that's impossible.
- **Culture/CI:** dry-run estimate as a PR check for BI-tier queries.

**Traps:** `maximumBytesBilled` counts **bytes scanned, not returned**; each
referenced table bills a 10 MB minimum; a budget alert doesn't stop anything by
itself — the stop needs the custom quota or the max-bytes config.

---

### A5. "A dashboard needs 5-minute-fresh aggregates over a 2 TB fact table. Materialized view, scheduled query, or BI Engine?"

Compare on freshness, cost, and restrictions:

- **Materialized view** — the default answer. Automatic (incremental) refresh
  as the base table changes, and **automatic query rewrite** for queries that
  are a compatible prefix of the MV definition:

  ```sql
  CREATE MATERIALIZED VIEW `proj.ds.mv_hourly_revenue`
  PARTITION BY revenue_date        -- must mirror the base table's partitioning
  CLUSTER BY tenant_id
  OPTIONS (enable_refresh = true, refresh_interval_minutes = 5) AS
  SELECT revenue_date, tenant_id, SUM(amount) AS revenue, COUNT(*) AS n
  FROM `proj.ds.fact_payments`
  GROUP BY 1, 2;
  ```

  Restrictions to rattle off: no `COUNT(DISTINCT)`, no window functions, inner
  joins only and few of them, no non-deterministic functions, no
  `ORDER BY`/`LIMIT`. **Incremental refresh works for streaming inserts but
  NOT for DML-mutated base tables** — DML on the base forces
  `allow_non_incremental_definition` (full recompute, expensive) or breaks the
  pattern. If staleness is acceptable for some panels, use the freshness hints
  (`FROM mv WHEN FRESH` / `WHEN STALENESS < INTERVAL ...`) to trade freshness
  for skipping refresh waits.
- **Scheduled query** — full SQL freedom (window functions, DISTINCT, multi-hop
  transforms); staleness = the schedule interval; cost = whatever the query
  scans (make it partition-incremental). Choose when MV restrictions bite.
- **BI Engine** — in-memory reservation in front of the dashboard; sub-second
  tiles, no rewrite needed, but priced per GB-hour of reservation and it's a
  serving layer, not a data-shaping one. Common winning combo: **MV for the
  aggregates + BI Engine for the render**.

**Follow-ups:** *"Why is my query not hitting the MV?"* → it isn't a prefix
match of the MV (different aggregation grain, extra columns, filter on a column
not in the MV). *"What does refresh cost?"* → refresh queries consume slots /
on-demand bytes like any query; incremental keeps them proportional to changed
rows.

---

## B. Query debugging ("walk me through how you'd read the plan")

The differentiator here is a **structured reading of Execution details** in the
console: the stage graph; per-stage **Wait / Read / Compute / Write** ms,
**bytes shuffled**, records read/written, slot-ms, parallelism. Everything else
follows from those numbers.

### B1. "Same query ran 4 minutes for months; today 47. Where do you look?"

1. **Slot-time vs wall-clock first.** From `INFORMATION_SCHEMA.JOBS_BY_PROJECT`:
   avg slots = `total_slot_ms / duration_ms`. High slot-ms + long duration →
   genuinely more work (data grew, plan got worse). **Low slot-ms + long
   duration → the query waited for slots** (reservation contention — check
   what else ran in that window, and `reservation_id` per job).
2. **Execution details → find the stage that ballooned.** Long **Wait** bars
   across stages = slot starvation. Long **Compute** on one stage = more data
   or a worse plan.
3. **Data drift:** upstream table grew (`INFORMATION_SCHEMA.TABLE_STORAGE`),
   a cluster column's cardinality collapsed, or the small side of a join
   crossed the broadcast threshold and the join flipped to a hash join (B3).
4. **Plan change:** did the query text actually change (dashboard tool update)?
   Compare against job history.

**Layered fix:** platform (carve reservations / autoscale — D4), SQL (shrink
the scanned bytes: partition predicate, projection), table config (cluster for
the new access pattern), or architecture (pre-aggregate — A5).

### B2. "A join on `user_id` has been stuck at 99% for 20 minutes; execution details show one stage with a huge compute time and almost no parallelism. Diagnose."

That's **key skew**. The hash join shuffles by key; one dominant key sends a
disproportionate share of rows to one worker and the stage serializes on it.

1. **Confirm the hot keys** — cheap query, run it now:

   ```sql
   SELECT user_id, COUNT(*) AS c
   FROM `proj.ds.events`
   WHERE ds = CURRENT_DATE()
   GROUP BY 1 ORDER BY c DESC LIMIT 20;
   ```

   Usual culprits: a shared **service/test account** behaving like a tenant,
   `NULL`/default keys from a broken upstream, one mega-tenant.
2. **Fixes, in order of elegance:**
   - **Split the hot path:** pre-aggregate the top-N hot keys to one row per
     key, `UNION ALL` two joins — broadcast/deduped join for the hot slice,
     hash join for the well-behaved remainder.
   - **Salt the key** on both sides for the hot slice (key + salt bucket),
     then re-aggregate — the classic Spark trick (see
     [`spark/01_batch_sessionization.py`](../spark/01_batch_sessionization.py)),
     works here when the two-path split is awkward.
   - Filter/fix `NULL` keys before the join.
3. **Credit where due:** BigQuery dynamically repartitions work and absorbs
   *moderate* skew; extreme single-key dominance is what defeats it.

**Follow-ups:** *"Same symptom in a GROUP BY?"* → same diagnosis; approximate
aggregation (`HLL_COUNT.*`) or two-phase aggregation is the fix. *"How would
you have caught this pre-prod?"* → profile top-N key counts as a data-quality
check, not a query-tuning step.

### B3. "When does BigQuery broadcast vs hash join — and why did our join regress after the dimension table grew?"

- **Broadcast join:** one side is small enough (threshold unpublished — on the
  order of tens-to-hundreds of MB; say *that* out loud) that BQ replicates it
  to every worker reading the big side. Cheap: no shuffle of the big side; the
  cost is small-side × workers (visible as that stage's shuffle-output bytes).
- **Hash join:** both sides shuffled on the key when neither qualifies.
- **Why the regression:** the dimension crossed the threshold, or the "small"
  side is a CTE that BQ **inlines and recomputes** — it materializes bigger
  than you think. **No join hints exist in BigQuery**; you influence the plan
  only by shaping inputs.
- **Fix:** genuinely shrink the small side — filter it first, or materialize a
  deduped/pre-aggregated temp table (temp table, not CTE, precisely because
  CTEs aren't materialized). If the join must be a hash join, cluster both
  tables on the join key so the read side is cheap.

**Trap:** "why not always CTE for readability" → fine for humans, invisible to
the optimizer; when a CTE is reused 3× it may be computed 3×.

### B4. "Slot-time looks reasonable but wall-clock is terrible. Find the shuffle-heavy stage."

Read the stage list sorted by **shuffle output bytes** — the expensive stages
announce themselves. The generators of huge shuffles, ranked:

- Exact `COUNT(DISTINCT x)` on high-cardinality `x` — swaps to
  `APPROX_COUNT_DISTINCT` / `HLL_COUNT.INIT`-`MERGE` with ~zero dashboards
  noticing.
- High-cardinality `GROUP BY` / `PARTITION BY` (window functions shuffle the
  whole window domain).
- `ORDER BY` at the end of a huge result (global sort stage).
- Joins on keys unrelated to the clustering.

**Fixes:** approximate aggregation; pre-aggregate at a coarser grain that
matches the cluster prefix (GROUP BY columns ≈ leading cluster columns); narrow
the window spec; only select needed columns before joins (shuffle bytes count
post-pruning columns, so projection *before* the shuffle literally shrinks it).

---

## C. Correctness

### C1. "Your Airflow task runs a MERGE. The API call timed out, Airflow retried, and the second attempt also succeeded. Did we double-apply? Design the write so the answer is always 'no'."

Layered answer:

- **What's true about MERGE:** it's atomic; a committed MERGE + a re-run of the
  *same deterministic* MERGE is idempotent — matched rows update to the same
  values, `NOT MATCHED` rows now match, nothing duplicates. The danger is
  everything that makes the second run *not the same statement*:
  - A **non-deterministic source** (`now()`, random sampling, "rows since last
    run" watermarks) — the retry reads a different source.
  - **Duplicates in the source** — two source rows for one key: MERGE errors
    (good) or, in softer patterns, duplicates land.
  - `WHEN NOT MATCHED BY SOURCE THEN DELETE` variants — a re-run with a
    smaller source deletes legit rows.
- **The production pattern — staging table per run, keyed off the data
  interval, never `now()`:**

  ```sql
  CREATE OR REPLACE TABLE `proj.ds.stage_orders`   -- a retry rebuilds this
  PARTITION BY ds AS
  SELECT ... FROM `raw...` WHERE ds BETWEEN @interval_start AND @interval_end;

  MERGE INTO `proj.ds.fact_orders` t
  USING `proj.ds.stage_orders` s
    ON t.order_id = s.order_id            -- stable natural key
  WHEN MATCHED THEN UPDATE SET ...
  WHEN NOT MATCHED THEN INSERT ROW;
  ```

  Retry semantics: stage is rebuilt identically, MERGE converges. Late data is
  handled by widening the interval (Airflow file `01`'s lookback pattern).
- **The belt-and-suspenders trick worth volunteering:** submit the job with a
  **deterministic client-generated `job_id`** (e.g. derived from
  `dag_id + task_id + run_id`). BigQuery **de-duplicates by job ID** — the
  retry returns the original job instead of re-executing it, which kills the
  ambiguous-timeout case entirely.

**Follow-ups:** *"Load jobs instead of DML?"* → `WRITE_TRUNCATE` on a
partition/decorated path is idempotent for free; `WRITE_APPEND` doubles on
retry. *"Why not transactions?"* → multi-statement transactions exist but live
inside a single job; they don't span Airflow retries — staging + idempotent
MERGE is the cross-job pattern.

### C2. "Rows streamed via `insertAll` 'disappeared' from the hourly job, couldn't be deleted, and didn't survive a copy. Explain all three."

One cause: they were still in the **streaming buffer**. The boundary behavior
that's being tested:

- Rows are **queryable immediately**, but live in the buffer until extraction
  (minutes to an hour+; no hard SLA). Detect it:

  ```sql
  SELECT partition_id, row_count, total_logical_bytes
  FROM `proj.ds.INFORMATION_SCHEMA.PARTITIONS`
  WHERE table_name = 'events';
  -- the row with partition_id IS NULL is the streaming buffer
  ```

- **The classic three symptoms:**
  1. `_PARTITIONTIME` is **NULL while in the buffer** → any
     `_PARTITIONTIME`-filtered query (ingestion-time partitioning) misses
     fresh rows — "disappearing data". Fix at the schema layer: partition on an
     explicit event-time **column** and filter on that column, not the
     pseudo-column.
  2. **DML on buffered rows** — the textbook answer is "UPDATE/DELETE/MERGE
     can't touch streaming-buffer rows"; since 2023 BigQuery lifted this.
     Know both, lead with "historically blocked, now supported, but I'd verify
     current behavior for the region/edition" — that's the senior version.
  3. **Copies and exports** may not include buffered rows — verify row counts
     after any copy before deleting the source.
- **Delivery semantics:** `insertAll` is **at-least-once**; `insertId` dedup is
  best-effort with a bounded window — long-delayed retries produce duplicates.
- **The modern answer (architecture layer):** the **Storage Write API** —
  roughly half the price of legacy streaming, higher quotas, and its
  *default stream* gives **exactly-once** semantics via offset commit.
  New pipelines shouldn't use `insertAll` at all; and hot-path pipelines
  shouldn't DML into freshly streamed rows regardless — land in a staging
  table, curate out of the buffer on a schedule (C1's MERGE).

### C3. "A bad UPDATE shipped at 14:02. It's 16:00. Restore the table — and tell me what you'll re-apply."

1. **Find the damage precisely:** the job in `INFORMATION_SCHEMA.JOBS_BY_PROJECT`
   (`statement_type`, `total_bytes_processed`, end_time) — confirm which table,
   how many rows mutated, and whether anything legitimate ran after 14:02.
2. **Restore from time travel** (window is 2–7 days, configurable via
   `max_time_travel_hours`; on-demand billing applies to the restore scan):

   ```sql
   CREATE TABLE `proj.ds.orders_restored`
   PARTITION BY ds CLUSTER BY customer_id      -- CTAS drops these; re-declare!
   AS SELECT * FROM `proj.ds.orders`
   FOR SYSTEM_TIME AS OF TIMESTAMP('2026-10-24 13:55:00+00');
   ```

   Then verify with a row count + checksum (e.g.
   `SUM(FARM_FINGERPRINT(TO_JSON_STRING(t)))` on old vs new), swap by rename or
   a view flip.
   **Trap to narrate:** CTAS creates a *fresh* table — partitioning, clustering,
   **row access policies, policy tags, and table options do not come along**;
   re-apply them deliberately. Pre-created **snapshots**
   (`CREATE SNAPSHOT TABLE ... CLONE ...`) survive beyond the travel window and
   past table deletion — that's the pattern for retention > 7 days.
3. **Re-apply legitimate post-14:02 writes** — usually a re-run of downstream
   tasks from the interval (this is why C1-style interval-keyed idempotent
   pipelines exist: the recovery story is "re-run the day").
4. **Prevention layer:** dry-run + review gate for DML on prod, snapshot before
   risky deployments, and know that time-travel storage beyond ~72h is billed —
   the window is a cost knob, not a free archive.

**Follow-up:** *"The table was dropped, not updated"* → restore by copying the
pre-drop system-time version (`bq cp ds.t@-2h ds.t_restored`) within the
window; past it, only snapshots save you.

### C4. "dbt with 32 threads intermittently fails on the big fact table with queue/rate-limit errors on DML. Diagnose and redesign."

- **Mechanism:** concurrent **mutating DML against one table serializes** (a
  handful run concurrently; the rest queue, and past the queue cap they fail),
  plus per-table daily modification quotas (numbers move; ~1,500 load
  jobs/table/day is the famous one). 32 threads × small incremental models ×
  one table = collisions.
- **Redesign, layered:**
  - **SQL/orchestration:** funnel writes into **one MERGE per table** — stage
    all sources into a per-run staging table, single merge (C1's pattern
    exactly). Lower dbt thread count for models that share a target.
  - **Job type:** if the model is append-only, **load jobs** (`WRITE_APPEND`)
    or partition-scoped `WRITE_TRUNCATE` — loads are **free**, parallel, and
    don't compete in the DML mutation path at all.
  - **Platform:** `BATCH` priority for non-interactive merges so they don't
    fight the 100-interactive-concurrent-queries-per-project budget.
- **Pricing tie-in they're probing:** on-demand DML bills **scanned + written
  bytes** (a one-row MERGE still scans the predicate space); load jobs bill
  zero. Under editions, DML is just slot-time like any query. "Append-only
  models should load, not merge" is the sentence that scores.

---

## D. Hardening & multi-tenancy

The framing that scores: **UI etiquette is not a control.** Every answer should
say which layer actually *enforces* the guarantee.

### D1. "Team B needs 3 of 40 columns of team A's raw dataset. Team A grants nothing at the dataset level. Design it."

**Authorized view** pattern:

1. Create the view in a **separate sharing dataset** (owned by the platform
   team), selecting only the permitted columns/rows.
2. On the **source** dataset, authorize that specific view (`authorizedViews`)
   — the view gains read access to the source *as itself*, without any grant
   to team B on the source.
3. Grant team B `dataViewer` on the **sharing dataset only**. They can query
   the view; they cannot see the base table, and a plain view would have
   failed here (callers of ordinary views need access to referenced tables).

**The escalation trap to volunteer:** **authorized datasets** authorize *every
current and future view* in a dataset — convenient at scale, but if analysts
can `createTable` in that dataset, anyone can author a new view over source
columns = privilege escalation. Lock creation rights on sharing datasets.
**Defense in depth:** pair with policy tags (D2) on sensitive columns so even a
mis-authorized view that includes `ssn` still errors for non-readers.

**Follow-up:** *"Performance?"* → an authorized view prunes partitions/clusters
like any query on the base; add a partition filter inside the view definition
and the sharing dataset can't cause full scans.

### D2. "Compliance: only group X may ever read the `ssn` column; everyone else still queries the table."

**Policy tags (column-level security via Dataplex/Data Catalog):**

1. Create a taxonomy → policy tag per sensitivity tier (`pii.direct`).
2. Attach the tag to the column (schema update / Dataplex UI — needs
   `bigquery.tables.setCategory`-style admin; taxonomy lives at org/project
   level).
3. Grant group X `roles/datacatalog.categoryFineGrainedReader` **on the tag**
   (not on the table — the grant travels with the tag).

Behavior worth stating precisely: unauthorized users get an **error** if their
query references the column — including `SELECT *` (which is why BI tools
break loudly rather than leak); authorized users query normally. Enforcement
applies at query time, `tabledata.list`, and the Storage API alike. Policy tags
**deny, they don't mask** — if the requirement is "others see `***`", that's
**dynamic data masking** (masking rules attached to the tag) or a view.

**Traps:** a view that *excludes* the tagged column is the standard sharing
pattern (composes with D1); a view that *includes* it still enforces the tag;
the tag list is part of the table schema — CTAS/recreate drops it (same class
of trap as C3's CTAS warning).

### D3. "Regional leads must see only their own region. Ship it — and explain why our ELT service account broke the night RAPs launched."

**Row access policies:**

```sql
CREATE ROW ACCESS POLICY rap_emea
ON `proj.ds.fact_orders`
GRANT TO ('group:emea-leads@corp.com')
FILTER USING (region = 'EMEA');
-- one per region; rows visible via ANY RAP whose grant covers the caller
```

- Semantics: once a table has any RAP, a row is visible only through a RAP
  that both **grants to the caller** and whose **filter matches the row**.
  Applies to queries *and* DML *and* time travel — retroactively scoped.
- **The incident:** RAPs filter *service accounts too*. The ELT service
  account that did full-table MERGEs started silently matching zero rows
  outside any grant — pipelines "succeeded" while losing data. Fix: grant the
  pipeline identity in a `FILTER USING (TRUE)` RAP (or per-region RAPs), and
  add a "pipeline sees all" test to deploys.
- **Traps:** max ~100 RAPs per table; RAPs are not carried by CTAS-style
  recreates (verify after any table rebuild — recurring theme with C3/D2);
  they're not a substitute for policy tags (rows vs columns, orthogonal and
  composable).

### D4. "Three teams share one org commitment; the analysts' dashboards starve the SLA pipelines. Carve it up."

Editions reservation design, in order:

1. **One capacity commitment** (the org pays for a pool of baseline slots + a
   shared autoscaling ceiling) in the pipeline's **region** (commitments don't
   span regions).
2. **Reservations carve the pool:** `pipelines` (baseline ≥ SLA requirement,
   modest autoscale ceiling — predictable), `analysts` (low baseline, high
   autoscale ceiling — elastic), `adhoc/sandbox` (tiny). Toggle idle-slot
   sharing consciously: it recovers stranded baseline but re-couples teams.
3. **Assignments route work:** project/folder → reservation. Anything unassigned
   falls back to on-demand (or is blocked outright with a `NONE` assignment —
   the strong-governance move for sandbox projects).
4. **Monitor per reservation:** `reservation_id` in
   `INFORMATION_SCHEMA.JOBS_BY_PROJECT` + the reservation admin views; alert
   on queue wait time, not just utilization.

**Honest limitation to state:** reservations are **soft isolation** — noisy-
neighbor pressure at the commitment level still bites; the hard guarantees are
baseline slots + autoscale ceilings per reservation.

### D5. "An analyst full-scanned the 90 TB billing-export for one number — $560. Make that query impossible, not discouraged."

The full layered stack, best-first:

- **IAM (the real fix):** analysts don't get `getData` on raw billing tables,
  period. Access via **authorized views** (D1) that are partition-bounded and
  column-limited, or curated cost marts (aggregated daily per team/project).
- **Table config:** `require_partition_filter=TRUE` — even an accidental raw
  query is date-scoped.
- **Query-level:** default `maximumBytesBilled` in every client/console the
  analysts use (A4) — the one-row lookup on a 90 TB table fails at dry-run
  estimate.
- **Org-level:** custom quotas per project/user (daily on-demand cap) as the
  org-wide backstop; budget → Pub/Sub → (page | auto-disable the offending
  user's query path) as the tripwire.
- **Architecture:** the reason the analyst hit raw billing at all is that no
  curated layer existed — the durable fix is the mart + a catalog entry that
  makes the right table the easy table.

---

## E. Quick-fire traps (instant, correct answers expected)

**Pricing model:**
- **Load jobs are free.** Streaming: `insertAll` ~$0.05/GiB (2 GiB/mo free);
  Storage Write API roughly half that, exactly-once on the default stream.
- **On-demand DML bills scanned + written bytes** (a one-row UPDATE scans the
  predicate space); under **editions, DML is slot-time** — the pricing model
  flips when you buy reservations.
- On-demand ≈ $6.25/TiB scanned, 1 TiB/month free, **billed bytes = columns
  referenced** (columnar), 10 MB minimum per referenced table. Cache hits are
  free.
- Storage: active logical vs physical (physical includes time-travel bytes);
  92 days unchanged → long-term (≈50%) discount.

**Partitioning & clustering:**
- Pruning needs **bare-column predicates with compile-time constants** — a
  function on the column or a join/subquery-derived value kills it; query
  parameters are fine.
- Cluster order is a **prefix** — filtering on the 2nd cluster column without
  the 1st ≈ no clustering benefit. `FLOAT64`/`STRUCT`/`ARRAY` columns can't
  cluster.
- 4,000 partitions/table default cap; `require_partition_filter` errors
  unpruned queries; ingestion-time `_PARTITIONTIME` is NULL in the streaming
  buffer.
- CTAS/recreate drops partitioning, clustering, RAPs, policy tags — re-declare
  everything; `CLONE` preserves structure (verify policy attachents).

**Execution & joins:**
- No join hints, no indexes to hint with — you shape plans by shaping inputs;
  CTEs are inlined, not materialized (use temp tables to fix a small side).
- Broadcast threshold is unpublished (~order of hundreds of MB) and visible as
  the join name in execution details; the flip broadcast→hash is a classic
  "query used to be fast" cause.
- `total_slot_ms / duration_ms` = avg slots; low value + long duration =
  contention, not compute.
- Search indexes exist for point lookups on non-clustered columns — that's the
  exception to "BigQuery has no indexes."

**Correctness boundaries:**
- Streaming buffer: queryable immediately, but `_PARTITIONTIME` NULL, copies
  may miss rows, DML historically blocked (supported since 2023 — say both).
- Time travel: 2–7 days, billed on restore scans; snapshots survive deletion
  and outlive the window; deterministic client `job_id` de-duplicates retried
  jobs.
- Concurrent DML on one table queues then fails; append-only data should use
  load jobs (free, parallel, no DML quota).

**Security:**
- Policy tags **deny** (errors on `SELECT *`), they don't mask; the reader
  grant is on the **tag**, not the table.
- RAPs filter service accounts too — pipelines break *silently*; authorized
  **datasets** + open `createTable` = escalation path.
- `maximumBytesBilled` fails the job before scanning; budgets alert, custom
  quotas actually block.

---

## F. Reusable prompt for this question class

Prepend this whenever you ask any assistant for BigQuery interview questions,
to keep the layered coverage intact:

> For BigQuery (or any warehouse), phrase each question as an incident
> ("query cost 10x'd after a change — walk me through the diagnosis") or a
> design ask. For each, give the **layered real-world answer**: SQL/schema
> design vs table/dataset config (partitioning, clustering,
> require_partition_filter) vs IAM/policy tags (authorized views, column- and
> row-level security) vs org-level quotas/reservations (maximumBytesBilled,
> custom quotas, editions carve-ups) vs architecture (staging patterns,
> Storage Write API, serving layers) — plus likely follow-ups and quick-fire
> traps: DML vs load-job pricing, cluster column ordering, and what breaks at
> the streaming-buffer boundary.

The phrases doing the work are *"layered real-world answer"* and *"what breaks
at the boundary"* — they force the enforce-vs-convention and
cost-vs-correctness distinctions that separate senior answers from
checklist answers.
