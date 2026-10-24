# Dataproc practical interview questions — scenario bank

The **job-config layer** of Spark answers (skew/salting, AQE, caching, idempotent sinks)
lives in the hands-on files in this repo — practice them alongside:

- [`../spark/01_batch_sessionization.py`](../spark/01_batch_sessionization.py) — skew,
  salting, shuffle counting, cache, AQE (the "fix it in the job" answers you'll cite here).
- [`../spark/02_structured_streaming_watermark.py`](../spark/02_structured_streaming_watermark.py) —
  checkpointing and idempotent MERGE sinks (what makes node loss survivable).

This bank covers the **infrastructure round on top of those jobs**: cluster design &
sizing, cost/architecture trade-offs, operations, and hardening/multi-tenancy. Every
answer is layered cheapest-first: **job config → cluster properties → autoscaling
policy → architecture change** (ephemeral cluster, Serverless, or BigQuery). Cover the
answer, speak it in 60–90 seconds.

---

## A. Cluster design & sizing

### A1. Design ask — "Nightly pipeline: 40 Spark jobs, 2-hour window, data in and out of GCS. Size the cluster. How much spot?"

Layered answer (cheapest layer first):

- **Job config before hardware.** Half of oversized clusters exist because executors
  are small and mis-partitioned — right-size `spark.executor.memory` + cores, fix skew
  and shuffle partitions first (file `spark/01`). A cluster sized to a broken job is
  permanently expensive.
- **Cluster design.** Do the YARN math out loud: each N-core / M-GB machine exposes
  roughly M−(1–2 GB reserved) as `yarn.nodemanager.resource.memory-mb` (Dataproc
  computes it — don't override blindly). Jobs in the window × avg parallelism → total
  cores/memory → primaries (+ secondaries if bursty). Data is on GCS, so HDFS is not a
  sizing input — that's what licenses a high spot fraction.
- **Primary vs secondary.** Primaries: fixed, on-demand, sized for the steady state
  (and all HDFS if any). Secondaries: spot, carry the burst — they run NodeManager
  (compute) but **no HDFS DataNode** (no storage role). Because the autoscaler only
  ever adds/removes secondaries, primaries are your guaranteed-progress floor: keep
  them ≥ ~⅓ of peak so a spot reclaim wave can't stall the pipeline past its window.
- **Autoscaling policy** (instead of statically provisioning for peak):

  ```json
  {
    "workerConfig":          { "minInstances": 2, "maxInstances": 2   },
    "secondaryWorkerConfig": { "minInstances": 0, "maxInstances": 200 },
    "basicAlgorithm": {
      "cooldownPeriod": "180s",
      "yarnConfig": { "scaleUpFactor": 1.0, "scaleDownFactor": 0.5,
                      "gracefulDecommissionTimeout": "3600s" }
    }
  }
  ```

- **Architecture.** A 2-hour nightly burst is exactly the profile where you challenge
  the standing cluster: an ephemeral cluster created/destroyed by the orchestrator, or
  Dataproc Serverless batches (no cluster at all), are usually cheaper and remove the
  image-drift problem. Defend whichever you pick with the window, start-latency
  tolerance, and spot behavior.

Follow-ups: *Why not one giant machine?* (shuffle locality, spot blast radius, YARN
parallelism.) *How do you validate sizing before the deadline?* (run a representative
job, watch YARN cluster-metrics utilization; target ~70–80%, not 100%.) *What breaks at
80% spot?* (see A3 — recompute storms on reclaims.)

### A2. Incident — "The autoscaler added 20 workers, but jobs still sit QUEUED in YARN. Diagnose."

Diagnose in this order (each failure has a different fix layer):

- **Is the pending work queue-limited, not cluster-limited?** Capacity Scheduler queue
  caps (`yarn.scheduler.capacity.root.<q>.maximum-capacity`) cap a queue regardless of
  free nodes. New workers join, YARN shows them free, the team's queue is full →
  still queued. Fix layer: **cluster properties**, not more nodes.
- **Did the workers actually join?** `gcloud compute instances list` filtered by the
  cluster's prefix: spot capacity exhausted in the zone, subnet out of free IPs, or an
  init action failing on the new secondaries (init actions run on **every** autoscaler
  -added node — a slow/failing one silently breaks scale-up). Fix: zone spread,
  secondary IP ranges, custom image.
- **Can a container physically fit?** If `spark.executor.memory` + overhead exceeds a
  node's allocatable memory, or `yarn.scheduler.maximum-allocation-mb` is below the
  request, the RM can never place it — pending forever no matter how many nodes. Fix
  layer: **job config** (right-size executors) or cluster property (max allocation).
- **Policy ceiling.** `maxInstances` in the autoscaling policy reached, or the
  cooldown/sampling period hasn't elapsed (the autoscaler samples YARN metrics on an
  interval; scale-up isn't instant). Fix layer: **autoscaling policy**.
- **App-master throttling.** `yarn.scheduler.capacity.maximum-am-resource-percent`
  (defaults ~10–20%) limits how many *concurrent applications* can start their AMs —
  classic with many tiny jobs: nodes are free, applications wait for AM resource.
- **Is it YARN-queued at all?** Sometimes "queued" is the orchestrator or the Dataproc
  job itself waiting — check the actual YARN application state before blaming capacity.

Follow-ups: *Where do you see autoscaler decisions?* (cluster operations / Cloud
Logging — the scale actions are logged.) *Why will it never add primaries?* (by design:
secondaries are stateless compute; primaries hold HDFS/masters state.) *Which single
metric do you alert on?* (YARN pending memory that persists > N minutes.)

### A3. Incident — "Scale-down / spot reclaim mid-job: the Spark job slowed to a crawl, one failed outright. What actually happens to running jobs when nodes go away?"

Mechanics first — this is what's being probed:

- **Graceful decommission** (autoscaler scale-down, and manual resize with
  `--graceful-decommission-timeout`): the NodeManager stops accepting **new**
  containers and waits for running ones to finish, up to the timeout; after that it's
  **standard decommission** — remaining containers are killed.
- Killed containers = lost executors. In-flight tasks retry (`spark.task.maxFailures`,
  default 4). Worse: the node's **shuffle files die with it** (the external shuffle
  service runs inside the NodeManager), so *later* stages needing those map outputs
  recompute the lost map tasks. A cache-heavy job loses the cached partitions on that
  node too. Result: job "survives" but quietly doubles its work — or dies when retries
  exhaust.
- **A spot reclaim is not a decommission at all**: ~30 s ACPI notice, then the VM is
  gone — no container-draining. Graceful decommission only protects *deliberate*
  scale-down, never reclaims.

Layered fixes:

- **Job config:** idempotent outputs (re-run safe), checkpoint long lineages, don't
  build correctness on `cache()`/`persist()` on spot-heavy clusters, keep
  `spark.task.maxFailures` sane (raising it blindly just hides the problem).
- **Cluster properties:** graceful-decommission timeout sized to your longest stage
  (a 3600 s timeout with 5-minute stages just keeps nodes billing); tune
  `spark.excludeOnFailure` so decommissioning nodes aren't blacklisted.
- **Autoscaling policy:** make downscale conservative — `scaleDownFactor` low, long
  cooldown, and let it fire only after the batch window (utilization-based downscale
  between waves is what kills the next wave's warmup).
- **Architecture:** intermediate data on GCS, not HDFS (nothing to lose with a node);
  loss-intolerant long jobs on on-demand (or `non-preemptible` secondaries); for
  pure-batch pipelines, Serverless makes node loss Google's problem entirely.

Follow-ups: *graceful vs standard decommission in one sentence* (wait-for-containers
vs kill-now). *Why did the same job pass on on-demand and fail on spot?* (same
mechanics — reclaims aren't decommissions). *What happens to the driver?* (client-mode
driver on the master is untouched by worker loss; a cluster-mode AM on the lost node
is restarted by YARN as a new application attempt — job restarts, doesn't silently
continue.)

### A4. Design ask — "When does autoscaling actually hurt? Would you put it on this cluster?"

Name the failure modes, not just the feature:

- **Short jobs.** Scale-up latency is minutes (sampling interval + cooldown + node
  provisioning + init actions + YARN join). Jobs shorter than that either never get
  the capacity or thrash the cluster up/down. Rule of thumb: autoscaling absorbs
  bursts on the minutes-to-hours scale; per-minute elasticity is Serverless.
- **HDFS-dependent workloads.** Secondaries add zero storage; if the job's bottleneck
  is HDFS I/O or it writes intermediates to HDFS, autoscaling adds compute you can't
  feed and the scale-down story gets dangerous.
- **Warm-state workloads.** Interactive/notebook clusters lose JVM warmth, JIT,
  page cache, and cached RDDs on downscale — "the cluster got slower after we enabled
  autoscaling" is this.
- **Oscillation cost.** Scale-down that's too eager followed by scale-up (cooldowns
  mis-set) means you pay provisioning latency on every cycle, and slow graceful
  decommission keeps nodes billing while they drain.
- The senior close: autoscaling is a policy on a cluster you still own — if the
  workload is bursty *per job*, the architectural answer is ephemeral clusters or
  Serverless batches, and autoscaling is for the in-between (sustained cluster, bursty
  load).

Follow-up: *autoscaling to zero?* (secondaries can scale to 0, primaries and masters
never do — true scale-to-zero is deleting the cluster or Serverless.)

---

## B. Cost & architecture

### B1. Design ask — "Choose for each: (a) 30 scheduled Spark batch jobs nightly, (b) analysts doing ad-hoc notebooks + shared tables, (c) one 90-minute Spark job per day. Long-lived shared cluster, ephemeral per-job cluster, or Serverless?"

- **(a) Nightly batch fleet → ephemeral or Serverless.** Jobs are isolated, the
  template is versioned, no idle burn, image upgrades become a one-line change.
  Orchestrator pattern: `DataprocCreateClusterOperator → SubmitJob → Delete` (or just
  Serverless batches — no create/delete at all).
- **(b) Interactive analysts → long-lived** (with `--max-idle`), because session
  state, warm JVMs, and a shared metastore/Thrift endpoint are the product. Cost of
  long-lived: idle time, noisy neighbors (section D), drift — manage with IaC and
  external Metastore.
- **(c) One 90-min job → Serverless** (or the smallest ephemeral cluster). A standing
  cluster for 90 min/day is ~94% idle.
- **Decision axes to say out loud:** job density vs interactivity, state reuse
  (metastore/caches), start-latency tolerance, per-team isolation needs, upgrade
  cadence, who's on call for the cluster itself.
- **Architecture layer:** if (a) is really SQL-shaped transforms, the senior move is
  questioning Dataproc at all — BigQuery or a couple of scheduled queries may delete
  the whole platform (see B4).

Follow-ups: *how do you stop the long-lived cluster becoming a snowflake?* (Terraform
module, pinned-by-policy image, external Metastore, no manual installs — init actions
in git only). *What's the actual cost delta?* (work it: vCPU-hours × on-demand vs spot
price vs Serverless per-second billing; cluster creation is ~1.5–3 min of pure
overhead per ephemeral run).

### B2. Incident — "The bill doubled. A 'temporary' shared cluster created three months ago has been idling at 40 nodes since. Fix it, then make it impossible again."

- **Now:** delete it (it's 'temporary', data's on GCS, right? — *verify nothing wrote
  state to local HDFS first*; that check is the senior signal).
- **Settings layer:** `--max-idle` (delete after N idle; "idle" = no running/pending
  YARN applications and no notebook sessions) plus `--max-age` as a hard lifetime
  belt-and-braces. Know the three lifecycle knobs: `--max-idle`, `--max-age`
  (age since creation), `--expiration-time` (absolute timestamp).
- **Trap:** a hung application keeps the cluster "not idle" forever — idle timers are
  only as good as your job timeouts upstream (orchestrator-level
  `execution_timeout`).
- **Detection layer:** labels on every cluster (`team`, `env`, `cost-center`) →
  Billing export by label + budget alerts; a scheduled audit for clusters older than
  N days.
- **Architecture:** the real fix is that this workload never needed a standing
  cluster — move it to ephemeral/Serverless so "forgot to delete" is structurally
  impossible.

Follow-up: *why didn't autoscaling prevent this?* (autoscaling scales secondaries
based on YARN load; an idle-but-large **primary** set still bills 24/7 — autoscaling
is not a cost-management control for idle clusters).

### B3. Design ask — "How do you pick the spot/on-demand mix?"

- **Workload tolerance first:** map/shuffle-heavy batch with idempotent outputs eats
  60–80% spot happily (reclaims cost recompute, not correctness — A3). Loss-intolerant
  long jobs (huge shuffle, heavy cache, tight deadline) get on-demand — or
  `non-preemptible` secondaries when you want autoscaling without reclaim risk.
- **Floor rule:** enough primaries that the pipeline makes progress through a
  simultaneous reclaim wave (spot reclaims cluster in maintenance events — size for
  the bad day, not the average).
- **Placement:** spot is per-zone capacity — spread or pick zones with reclaim rates
  you've observed; check the reclaim metrics, don't assume.
- **Architecture:** if a team's job can't tolerate reclaims *and* can't be made
  idempotent, that's a signal to move that job to Serverless or on-demand and stop
  bending the shared cluster around it.

Follow-up trap: *preemptible vs spot?* (below, section E — 24 h cap vs none, 30 s
notice for both, `--secondary-worker-type=spot|preemptible|non-preemptible`).

### B4. Architecture ask — "Why are we running Spark for this at all — why not BigQuery?"

The answer that scores: a decision framework, not loyalty:

- **BigQuery wins** when the work is SQL-shaped analytics, has many concurrent
  consumers, no custom algorithms, and you want zero-cluster ops; storage/compute
  split and reservations change the economics favorably.
- **Dataproc wins** when there's real code (custom PySpark, ML libs, UDF-heavy
  logic), existing Spark assets, fine-grained control over shuffle/partitioning, or
  non-SQL formats/streaming prep.
- **Hybrid is the common end-state:** Spark does heavy shaping → writes BigQuery via
  the `spark-bigquery-connector` (reads via the Storage API; writes are load jobs) →
  BQ serves SQL/consumers.
- Name the cost trap: BQ is cheap to *load* and expensive to *scan badly* — moving
  compute to BQ without partitioning/clustering the target just relocates the bill.

Follow-up: *when is the answer Dataflow?* (streaming / unbounded data — Dataproc is a
batch platform; Spark Structured Streaming on Dataproc exists but Dataflow is the
managed answer).

---

## C. Operations

### C1. Design ask — "All our clusters are on image 2.0. Walk me through getting the platform to 2.2."

- **The fact they're testing: there is no in-place image upgrade.** Image version is
  fixed at creation; upgrade = create new clusters on the target image and move
  workloads (blue/green).
- **Plan:** run the job suite against a canary cluster on 2.2 (image bumps = Hadoop/
  Spark/Hive major versions → behavioral changes: parser changes, default flags,
  serde differences); rebuild **custom images** and vet **init actions** against the
  new image; check Dataproc Metastore version compatibility if attached; then cut the
  scheduled launches over and delete old clusters.
- **Why ephemeral wins here:** if clusters are created per job from a template, the
  "migration" is changing one variable and letting fleet turnover do the work. The
  long-lived shared cluster is the expensive one — this question is secretly an
  argument for B1's model.
- **Governance:** a Terraform/CI module that pins allowed image versions so teams
  can't create fossils.

Follow-ups: *what breaks most often on image bumps?* (implicit-version dependencies —
init actions pip-installing against changed Python/Hadoop, Hive 3 serde behavior,
Spark config deprecations). *How do you run both versions side by side safely?*
(same metastore version, template-ized properties, canary jobs gated on data-quality
checks).

### C2. Incident — "Cluster creation takes 12 minutes and 9 of them are init actions. Autoscaling also feels sluggish. Fix."

- **Diagnosis:** init actions run on **every node at creation and on every node the
  autoscaler adds later** — so a 9-minute init action taxes cluster start *and* every
  scale-up event (A2's "workers never joined" hides here too).
- **Fix ladder:** trim to seconds (install nothing that can be baked); move slow
  installs into a **custom image** (built with the `dataproc-custom-images` tool) —
  jobs start faster and init actions shrink to config-only; keep genuinely dynamic
  bits as init actions parameterized via `--metadata`.
- **Supply-chain point (senior signal):** pin init actions to a **versioned GCS
  object**, never a repo `HEAD` — a cluster build must be reproducible and can't
  depend on whatever main says today.
- **Architecture:** if clusters are ephemeral and created constantly, image-baking
  isn't an optimization, it's the design — creation time is on your critical path
  every job.

Follow-ups: *an init action fails on one node — what happens?* (cluster creation
fails / the scale-up node doesn't join; errors in Cloud Logging). *Custom image vs
init action trade-off?* (bake time + image maintenance vs per-create latency and
network flakiness).

### C3. Design ask — "Cluster-local Hive metastore or Dataproc Metastore?"

- **Cluster-local** (MySQL on the master): dies with the cluster, per-cluster
  catalog. Fine for a single self-contained ephemeral job; useless the moment tables
  are shared or clusters are replaced (which is your target operating model — C1).
- **Dataproc Metastore:** managed, versioned, HA metastore service; attach clusters
  with `--dataproc-metastore`; one catalog shared across clusters (and Serverless);
  separate lifecycle and IAM from any cluster.
- **The trap that fails people:** the warehouse dir. A cluster's default
  `hive.metastore.warehouse.dir` points at the cluster's own **HDFS** — attach
  Dataproc Metastore to ephemeral clusters without overriding it and your *managed*
  table metadata survives in the shared metastore while the *data* evaporates with
  the cluster. Set `hive:hive.metastore.warehouse.dir=gs://...`. (External tables over
  GCS paths are immune — another reason external tables are the default on ephemeral
  fleets.)
- **Cost layer:** Metastore bills by capacity units — if a job doesn't need a catalog
  at all (reads/writes paths directly), skip it.

Follow-up: *Spark vs Hive talking to the metastore — version pitfalls?*
(requires Metastore service version compatible with the cluster's Hive version;
mismatched versions produce the classic "table exists but Spark can't read it"
class of bugs). *Where does the warehouse bucket live and who pays egress?* (GCS,
same region — cross-region warehouse dirs are a silent performance tax.)

### C4. Incident — "A job failed at 03:00. The ephemeral cluster is already deleted. Where's your evidence?"

Layered evidence trail:

- **Cloud Logging is the default and the backbone:** Dataproc ships driver
  stdout/stderr and per-daemon logs (e.g. `yarn-yarn-resourcemanager`, `spark`) to
  Cloud Logging at cluster create — they survive the cluster. Start with
  `gcloud dataproc jobs describe` (state, exit code, error info), then the driver
  logs in Logging.
- **The trap:** Spark **event logs** default to the cluster's local storage — gone
  with the cluster. Set `spark:spark.eventLog.dir=gs://...` on every ephemeral
  template and keep a **Spark History Server** (a small cluster or the Serverless
  history server) pointed at that bucket for post-mortem stage/executor detail.
- **Live debugging when the cluster still exists:** the **component gateway** —
  IAM-authenticated HTTPS endpoints for the YARN/Spark/HDFS UIs, no SSH tunnel to
  `localhost:19888` gymnastics. From the YARN app → attempt → executor logs; that's
  where "container killed beyond physical memory limits" actually shows.
- **Job config layer:** structured errors + meaningful exit codes make the 03:00 page
  actionable without a UI at all.
- **Architecture:** for repro: re-run the job on a matching image-version cluster
  with `--enable-component-gateway` and no auto-delete; for prevention: the
  debuggability budget (logs retained, event logs in GCS) is part of the ephemeral
  template, not an afterthought.

Follow-ups: *SSH is disabled org-wide — how do you debug?* (component gateway +
Logging — SSH should not be the answer). *What do you check first: driver log or
executor log?* (driver for the failure topology, executor for the physical cause —
OOM, GC thrash, lost shuffle).

### C5. Design ask — "Teams' jobs need GCS/BigQuery access. Right now the SA JSON key sits in a GCS bucket the init action copies down. Do better."

Walk the ladder from worst to defensible:

- **Keys in a bucket (current state):** long-lived, unscoped, copyable, un-auditable;
  a leak is forever until rotated. Never acceptable as an end-state.
- **VM service account + scopes:** no key file, but everything on every node shares
  the VM identity — any job from any team can act as the full SA (`--scopes
  cloud-platform` is the smell). Better, still coarse.
- **Dataproc Workload Identity** (image 2.1+): cluster daemons and jobs obtain
  short-lived credentials for a nominated Google service account via a credential
  provider on the nodes — no keys, per-cluster scoping, revocable by IAM. This is the
  managed-cluster answer.
- **Per-job identity on Serverless:** each batch takes its own `--service-account` —
  least privilege per workload is the *default* model there, which is a quiet but
  strong argument for it in multi-team setups.
- **Architecture layer:** private clusters (no external IPs), VPC Service Controls
  around the data projects, and per-team projects/SA per workload identity.

Follow-ups: *how do you audit which identity a job used?* (data-access logs + the SA
in the request). *what breaks moving a job cluster→Serverless?* (auth model
simplifies, but init actions/local HDFS assumptions don't port — C2/C3 answers
recur).

---

## D. Hardening & multi-tenancy

### D1. Design ask — "Three teams share one cluster. Team A ran a backfill and everyone else's jobs starved. Isolate them."

Layered answer:

- **Cluster properties — YARN Capacity Scheduler queues** (Dataproc's default
  scheduler) per team, set at create:

  ```text
  yarn:yarn.scheduler.capacity.root.queues = teamA,teamB,teamC
  yarn:yarn.scheduler.capacity.root.teamA.capacity = 40
  yarn:yarn.scheduler.capacity.root.teamA.maximum-capacity = 60
  yarn:yarn.scheduler.capacity.root.teamA.user-limit-factor = 1
  yarn:yarn.scheduler.capacity.root.teamA.acl_submit_applications = teamA
  ```

  (gcloud's `--properties` needs the `^#^` list separator because the queues value
  itself contains commas — or just do it in Terraform.)
- **Job config — the enforcement point:** `spark.yarn.queue=teamA` on every submit.
  A queue nobody routes to protects nothing — so wrap submission (template/wrapper/
  CI check) rather than trusting 200 job authors.
- **Cap the app masters, not just executors:** `maximum-am-resource-percent` per
  queue, or one team's fleet of small jobs can occupy all AM slots and block
  everyone's *start*, even with free compute.
- **Be honest about the boundary (the senior signal):** queues are *utilization*
  isolation, not *security* isolation. They don't stop a team's job hammering the
  shared HDFS, the master node, GCS quotas, or the metastore. If isolation must be a
  guarantee: per-team ephemeral clusters, or per-team Serverless batches each with
  their own service account (C5) — same pattern as the Airflow answer: convention +
  guards inside a shared deployment, real isolation only with separate deployments.
- **Chargeback:** cluster/team labels → Billing export by label; YARN queue metrics
  for usage-based showback.

Follow-ups: *a queue's cap is hit and the team's SLA job is queued behind their own
backfill — now what?* (priority within queue / `user-limit-factor`, or run the
backfill on its own ephemeral cluster — bursts are exactly what shouldn't share).
*Does preemption between queues exist?* (Capacity Scheduler preemption — say the
trade-off: hard caps + preemption config is fiddly; usually simpler to push the
spiky workload out of the shared cluster.)

### D2. Incident — "One team's job keeps OOMing. Sometimes its own containers die; last night it took down the whole cluster. Diagnose, then contain."

Diagnose (evidence first):

- **Executor OOM** shows as YARN killing a container: *"Container ... is running
  beyond physical memory limits"* — the container (executor memory + overhead
  `spark.executor.memoryOverhead`, default ~10% + 384 MB) exceeded its YARN
  allocation. Look for `collect()`, `toPandas()`, unbounded keys in joins, skewed
  partitions — the `spark/01` material.
- **Cluster-wide OOM** is a different culprit: the **driver**. Dataproc runs drivers
  on the **master node** by default — the master isn't a YARN node, so no queue caps
  it; one `--driver-memory 32g` collect under a big team load OOMs the master and
  every job on the cluster dies with it. (Also: dynamic allocation is **on by
  default** on Dataproc — an uncapped `spark.dynamicAllocation.maxExecutors` lets one
  job eat every free container.)

Layered containment:

- **Job config:** right-size executors/driver; fix skew and materialization; set
  `spark.dynamicAllocation.maxExecutors` per job as a ceiling.
- **Cluster properties:** `yarn.scheduler.maximum-allocation-mb` as a hard container
  ceiling; queue caps (D1) so the blast radius is the team's queue.
- **Driver containment (the real fix for the cluster-wide case):** run the driver
  under YARN — `spark.submit.deployMode=cluster` — so the driver is a container
  bounded by queue caps and `maximum-am-resource-percent` (see D3).
- **Architecture:** the repeat offender gets its own ephemeral cluster or Serverless
  batch — a job that needs the freedom to eat 200 GB shouldn't live on a shared
  cluster at all.

Follow-ups: *why didn't retries save it?* (OOM is deterministic per partition —
retries re-hit the same wall; fix the plan/skew). *`MEMORY_AND_DISK` vs container
OOM — what's the difference?* (storage-level spill is normal; container OOM is the
executor process exceeding its cgroup — different knobs).

### D3. Design ask — "Where should Spark drivers run on a shared cluster? 'Safe driver placement.'"

- **Default:** drivers on the **master**, client mode (jobs API). Simple, logs in one
  place, unaffected by worker loss — but every team's drivers contend for master RAM
  **outside** YARN's control; master OOM = total outage (D2's worst night).
- **Cluster mode** (`spark.submit.deployMode=cluster`): the driver is the YARN
  ApplicationMaster in a container on a worker — queue-capable, `maximum-am-resource-
  percent`-bound, and it survives master failover on an HA cluster (RM restarts the
  AM as a new attempt; a client-mode driver on a failed master just dies).
- **New risk you must volunteer:** an AM container can be placed on a **spot
  secondary** — a reclaim now kills the whole job, not a task. Mitigations: YARN
  **node labels** partitioning on-demand vs spot nodes with queue
  `accessible-node-labels` pinning AMs to on-demand nodes (YARN-level config, not a
  one-click Dataproc feature); or on-demand-only clusters for driver-heavy shared
  estates; or accept the risk on idempotent batch jobs.
- **Rule of thumb to close:** few long-lived critical jobs → drivers on master,
  well-sized masters. Many concurrent multi-team jobs → drivers in YARN under queue
  caps, labels if spot is in the mix.

Follow-ups: *how do you find where a driver ran after the fact?* (YARN UI app
attempt / Spark UI — cluster mode shows the AM container; client mode = master).
*What changes on Serverless?* (driver placement is managed for you — one more ops
burden that disappears).

---

## E. Quick-fire traps (instant, correct answers expected)

- **Preemptible vs spot VM:** both reclaimable with ~30 s notice and spot-priced;
  **preemptible has a 24 h max runtime, spot has none** — spot is the successor and
  the default recommendation. Dataproc: `--secondary-worker-type=spot|preemptible|
  non-preemptible`.
- **Graceful vs standard decommission:** graceful = NodeManager stops taking new
  containers and waits (up to a timeout) for running ones; standard = kill now.
  **Spot reclaims are neither** — no draining at all.
- **Autoscaler scope:** only **secondary** workers; primaries fixed at create.
  Secondaries carry **no HDFS DataNode** — autoscaling adds compute, never storage.
- **HDFS on autoscaling/ephemeral clusters** is an anti-pattern — GCS is the storage
  layer; the one HDFS question to ask before deleting any cluster (B2).
- **Spark dynamic allocation is ON by default** on Dataproc — cap
  `spark.dynamicAllocation.maxExecutors` or a job scales to whatever's free.
- **Driver placement default:** master node, client mode — **not** under YARN control
  (D3).
- **Dataproc's YARN scheduler default:** Capacity Scheduler — that's why queue
  properties are the multi-tenancy answer (D1).
- **No in-place image upgrade** — new clusters, blue/green, always (C1).
- **Lifecycle knobs:** `--max-idle` (idle timer) vs `--max-age` (age since creation)
  vs `--expiration-time` (absolute); idle = no YARN apps *and* no notebook sessions —
  a hung app keeps the cluster alive.
- **Init actions run on every added node** — including every autoscaler scale-up
  instance (C2); pin them to versioned GCS objects.
- **Dataproc Serverless:** no clusters, no SSH, no YARN UI, **no init actions**
  (custom container images instead), per-batch `--service-account`, per-second
  billing; event logs to GCS + managed history server.
- **Dataproc Metastore + ephemeral clusters:** override the warehouse dir to GCS or
  managed-table data dies with the cluster (C3).
- **Component gateway** = IAM-authenticated HTTPS UIs; the SSH SOCKS tunnel is the
  legacy answer.
- **Spark event logs** default to cluster-local storage — point
  `spark.eventLog.dir` at GCS or lose the post-mortem with the ephemeral cluster (C4).
- **`spark.executor.memoryOverhead`** is *outside* `spark.executor.memory` — a "my
  job uses 8g but containers are 8g" OOM is overhead, always.

---

## F. Reusable prompt for this question class

Prepend this whenever you ask any assistant for Dataproc interview questions, to keep
the layering and the trap coverage:

> For Dataproc (or any managed Spark/Hadoop platform), phrase each question as an
> incident ("autoscaling scaled up but jobs still queue in YARN — diagnose") or a
> design ask, and for each give the **layered real-world answer**: what's fixed in
> job config (executor sizing, queue, deploy mode), what's a cluster property (YARN
> queues, max allocation, warehouse dir), what's an autoscaling-policy change, and
> what's an architecture change (ephemeral clusters, Dataproc Serverless, or move to
> BigQuery/Dataflow) — plus likely follow-ups and quick-fire traps: preemptible vs
> spot, graceful vs standard decommission, when autoscaling hurts, drivers on master
> vs cluster mode, init actions vs custom images, cluster-local vs Dataproc
> Metastore, workload identity vs SA keys.

The phrases doing the work are *"layered answer: job config vs cluster properties vs
autoscaling policy vs architecture"* and *"queues are utilization isolation, not
security isolation"* — together they force both the fix ladder and the
enforce-vs-convention distinction that the hardening questions test.
