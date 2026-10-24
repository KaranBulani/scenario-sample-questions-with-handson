# Google Cloud Storage practical interview questions — scenario bank

Senior GCP data-engineering rounds treat GCS as a system with failure modes, not a
hard drive. Every question below is phrased as an incident or a design ask, and the
expected answer is **layered** — what you fix in:

- **Client code** (retries, preconditions, checksums),
- **Layout / naming** (deterministic keys, hive-style prefixes),
- **Bucket config** (versioning, lifecycle, retention, UBLA/PAP),
- **IAM** (per-pipeline service accounts, conditions, deny policies),
- and what only **architecture** (eventing, manifests, bucket separation) solves.

Practice by covering the answer and speaking it in 60–90 seconds. Section E is the
rapid-fire round; section F has hands-on drills — two of the settings in them are
**irreversible**, so only run those on scratch buckets.

---

## A. Ingestion correctness

### A1. "A nightly export retried after a flaky network and wrote the file twice — the dashboard doubled. Fix the design."

Strong answer hits:

- **Start with the diagnosis: GCS cannot produce this by itself.** Object names are
  unique per bucket — writing the same name again **overwrites** (new generation);
  it never creates a second object. Duplicate files mean the client minted a
  **non-deterministic name per attempt** (`transactions_20261024_021451.csv` →
  retry → `transactions_20261024_024403.csv`). The bug is the naming, not the storage.
- **Client code:** deterministic, partition-derived keys
  (`crm/dt=2026-10-24/transactions.csv`) so a retry lands on the *same* name and the
  overwrite is idempotent. Retries with **exponential backoff + full jitter**, honoring
  `Retry-After`. Know your client library's retry defaults — older `google-cloud-storage`
  releases did **not** retry uploads by default; set an explicit retry policy. Use
  **resumable uploads** for large payloads (a dropped connection resumes from the last
  committed 256 KiB-aligned chunk instead of restarting) — and note a retried
  *initiation* creates a new upload session, so dedup still rests on the object name.
  Verify integrity with **crc32c** (`x-goog-hash`); the Python client computes and
  validates it automatically.
- **Generations & preconditions:** make create-only semantics explicit with
  `ifGenerationMatch=0` — exactly one racing/retried writer wins; the loser gets
  `412 Precondition Failed`, which the client must treat as *success (already
  written)*, not an error. For read-modify-write cycles: read the object, remember its
  generation, write back with `ifGenerationMatch=<gen>` — optimistic concurrency; a
  412 means someone else won, so re-read and retry. Metadata-only updates use
  `ifMetagenerationMatch`. (Objects start at generation 1 / metageneration 1;
  overwrites bump the generation, metadata updates bump the metageneration.)
- **Bucket config:** object **versioning** on the landing bucket so overwrites keep the
  old generation (instant rollback + audit trail), paired with a lifecycle rule
  (`NumberOfNewerVersions >= 1` or `DaysSinceNoncurrentTime = 30`) so noncurrent
  versions don't bill forever.
- **IAM:** the ingestion service account gets `roles/storage.objectCreator` —
  create-only. It cannot delete objects or **overwrite existing ones** (overwriting an
  existing name requires `storage.objects.update`), so a buggy retry can't clobber a
  prior landing file either.
- **Architecture:** storage-level idempotence is necessary but not sufficient — the
  load must be idempotent too: BigQuery `MERGE` keyed on (uri, generation),
  partition-scoped `WRITE_TRUNCATE`, or a manifest table. Use `_SUCCESS` marker files
  for batch commit semantics so readers ignore incomplete partitions.
- **Red flag:** "turn off retries." The fix is idempotent design, not giving up
  reliability.

Follow-ups they always ask: walk through a resumable upload step by step (initiate →
session URI → chunked PUTs with `Content-Range` → query the committed offset with
`Content-Range: bytes */N`); when multipart vs resumable (multipart = one request with
metadata + data, fine for small objects; resumable for anything large or on flaky
links); two writers race with `ifGenerationMatch=0` — what does the loser do? (Check
the winner's checksum, treat as done.)

### A2. "Design the upload path: 5,000 IoT gateways on cellular, 100 MB hourly each, flaky links. No duplicates, no losses, no thundering herd."

Strong answer hits:

- **Naming/layout:** identity-addressed keys
  `site={id}/dt=YYYY-MM-DD/hr={HH}/{gateway}-{seq}.bin` — the device's monotonic
  sequence number makes every retry naturally idempotent (same name → overwrite).
  Never trust device clocks for partition placement; re-partition server-side from
  object metadata.
- **Client code:** resumable upload with automatic offset recovery (query session
  status after reconnect); backoff **with full jitter** so 2,000 gateways
  reconnecting after a tower outage don't synchronize into a thundering herd; crc32c
  checked end-to-end; a durable local queue → at-least-once upload from the device,
  with idempotence absorbed by the naming.
- **Bucket config:** lifecycle moves raw hourly objects to Nearline after ~30 days if
  the raw tier is rarely read post-ingestion.
- **IAM:** per-fleet service accounts (or short-lived signed URLs for third-party
  devices) with `objectCreator` only.
- **Architecture:** event-driven validation — GCS notification → Pub/Sub → loader that
  upserts into a manifest (dedupe key `name + generation`). Add an **anti-entropy
  reconcile**: hourly listing of one `dt=` prefix, diffed against the manifest, catches
  anything the event path missed. Alert on per-site gaps ("gateway 417 absent for
  hour H").
- **Trade-off to say out loud:** content-addressed storage (`{sha256}.bin` + metadata
  index) gives free dedup but kills date-partitioned reads; partition + sequence
  naming stays queryable and is enough when devices are honest about sequence numbers.

Follow-ups: what happens at the top of the hour (everyone uploads simultaneously —
stagger upload windows with jitter); late-arriving data (upserts, not appends); how
you'd detect a gateway replaying its queue (duplicate `seq` → manifest rejects,
alert fires).

### A3. "Downstream readers occasionally call freshly-ingested files corrupt. Where do integrity checks go?"

Strong answer hits:

- **Client code:** checksum at the source *before* upload; upload with crc32c (and
  md5 when a downstream cross-checks); the client library compares the returned hash —
  never ignore a mismatch. For **composite objects**, `md5Hash` is a hash of the
  component MD5s, *not* of the content — naive md5 verification breaks (the classic
  gsutil parallel-composite-upload surprise). crc32c is the cheap, always-defined
  default.
- **Layout/naming:** write a sidecar or manifest column (`uri, size, crc32c`) at
  finalize time; readers validate against it before loading; reject or quarantine
  `size == 0` finals (a real failure mode when producers upload empty buffers).
- **Bucket config:** lifecycle `AbortIncompleteMultipartUpload` after ~7 days —
  incomplete resumable/multipart sessions hold orphaned parts that are **billed as
  storage** and confuse size-based audits.
- **Architecture:** the loader validates *before* parsing (cheap crc32c before
  expensive Parquet decode), writes rejects to a quarantine prefix with its own
  lifecycle rule, and every hop into BigQuery runs with `--max_bad_records=0` so
  corruption fails loudly instead of statistically.

Follow-ups: the gzip trap — objects stored with `Content-Encoding: gzip` are
transparently decompressed on read, so byte counts/checksums differ between stored
and served sizes; TLS covers transit; corruption "inside GCS" isn't something you
design for — you design for broken producers.

---

## B. Consistency & events

### B1. "Right after the writer job finishes, the reader lists the prefix and sometimes sees zero new files. On-call says 'GCS is eventually consistent.' Diagnose — and separately, one file keeps getting processed twice."

Strong answer hits:

- **Correct the premise first — that's the interview point.** Since 2021 GCS is
  **strongly consistent for everything**: read-after-write, overwrite, delete, and
  **listing** (list-after-write included). A candidate who agrees "listing is
  eventually consistent, add a sleep" is reciting pre-2021 docs. For depth: say that
  object *content* was always strongly consistent; it was listing and metadata that
  used to be eventual.
- **Diagnose "sometimes zero" — it's your system, in this order:** (1) **prefix
  mismatch** — writer wrote `dt=2026-10-24/`, reader listed a typo'd path or built the
  date in a different timezone (UTC vs local, off-by-one around midnight); (2)
  **orchestration gate** — "writer finished" really means "the client call returned"
  while the listing job fired from a stale trigger; (3) **pagination bug** — ignoring
  `nextPageToken` silently truncates large listings; (4) wildcard expansions that
  match nothing. Verify with `gcloud storage ls` on the exact prefix vs the job's
  constructed URI, and count listed objects in the reader's logs.
- **Diagnose "processed twice":** overlapping listing windows with no **watermark**
  (the job re-lists from scratch and reprocesses), or — once you're event-driven —
  Pub/Sub at-least-once redelivery. Fix: watermark/manifest + idempotent load, never
  "we'll process exactly once" wishes.
- **Bucket config:** nothing is needed *for consistency* — that's the punchline.
  Versioning is still recommended so the twice-processed overwrite case is
  recoverable.
- **Architecture:** stop polling. Either go event-driven (B2) or make batch reads
  snapshot-based: `_SUCCESS` commit markers + a manifest, so readers consume a
  declared complete partition, not a moving prefix.

Follow-ups: what changed in 2021 (Google moved listing/metadata to strong
consistency — the old "eventual" caveats are gone); when is a sleep acceptable
(never as a correctness mechanism).

### B2. "Replace the 15-minute cron listing with GCS notifications → Pub/Sub → BigQuery. What breaks, and how do you get effectively-once?"

Strong answer hits:

- **Setup mechanics, cold:** a notification config on the bucket (`OBJECT_FINALIZE`,
  optional `object_name_prefix`, custom attributes) targeting a topic where the
  **GCS service account** (`service-<proj>@gs-project-accounts.iam.gserviceaccount.com`)
  holds `roles/pubsub.publisher`. The classic *silent* failure is forgetting that
  grant — the notification doesn't deliver and a pipeline that never verifies ships
  blind. Always prove the path: test upload + `gcloud pubsub subscriptions pull`.
- **Delivery semantics:** Pub/Sub is **at-least-once and unordered** — GCS
  notifications carry no ordering keys. Duplicates arrive via ack-deadline expiry and
  redelivery; `OBJECT_FINALIZE` fires for **every new generation, overwrites
  included** — so the dedupe key is **(object name, generation)**, never name alone.
- **Consumer design:** ack **after** durable commit; the sink must be idempotent —
  BigQuery `MERGE` keyed on `uri + generation`, or partition-scoped `WRITE_TRUNCATE`
  for full-file loads. Exactly-once doesn't exist here; you build *effectively-once =
  at-least-once delivery + idempotent sink*. Load time vs ack deadline → extend the
  lease or checkpoint with a pull subscription; poison messages → dead-letter topic;
  alert on `oldest_unacked_message_age` and `num_undelivered_messages`.
- **Gap coverage:** misconfigured or removed notifications, and deploy windows, drop
  events silently — run a **periodic reconcile**: list the partition prefix, diff
  against the manifest, backfill the difference. Events are the fast path; listing is
  the source of truth.
- **Ordering:** make the loader order-independent (file B can arrive before file A —
  upserts, not appends). If per-key ordering is a hard requirement, that's an
  ordering-key discipline on a *different* producer; GCS notifications won't give it.

Follow-ups: what's in the message (base64 JSON payload: bucket, name, generation,
metageneration, size, crc32c/md5, timeCreated; attributes carry the event type); how
to replay a bad day (re-list + manifest diff — don't "replay Pub/Sub"); why lifecycle
`OBJECT_ARCHIVE` events can surprise consumers that assumed only FINALIZE exists.

---

## C. Layout, cost & lifecycle

### C1. "An external BigQuery table over two years of daily CSV dumps — every query scans everything and the bill is brutal. Restructure the lake."

Strong answer hits:

- **Layout/naming:** hive-style prefixes `dt=YYYY-MM-DD/` so partition pruning happens
  at the *listing* level; one bucket per environment/tier instead of one giant flat
  bucket.
- **BigQuery side:** external table over `gs://lake/curated/dt=*/*.parquet` with
  `format='PARQUET'`, `hive_partitioning_mode='AUTO'`, and
  **`require_hive_partition_filter=true`** — queries without a partition filter are
  *rejected*, which is a cost circuit-breaker, not an inconvenience. Columnar Parquet
  (+ snappy/zstd) instead of CSV cuts scanned bytes roughly 5–10x. **Kill small
  files**: thousands of 5 MB files mean listing overhead plus per-file open costs —
  compact to ~128 MB–1 GB per file per partition.
- **Spark side:** `option("basePath", "gs://lake/curated")` so partition columns
  survive the read; path predicates prune partitions. The senior escalation is a real
  table format (**Iceberg/Delta on GCS**) for genuine pruning, ACID, and schema
  evolution instead of raw files.
- **Bucket config/lifecycle:** raw landing tier → Nearline/Archive after N days;
  curated Parquet stays Standard while query-hot. **Autoclass** when access patterns
  are genuinely unpredictable (per-object transitions, no retrieval/early-delete
  fees, small per-object management fee — the wrong choice for billions of tiny
  objects).
- **Architecture:** external tables are for cold/occasional/governed access; hot
  paths get **materialized into native partitioned + clustered tables** on load.
  Land raw → curate → query, three tiers, each with its own bucket, lifecycle, and
  access model. Mention BigLake with metadata caching if they probe external-query
  latency.
- **Trade-off to name:** external tables read *live* files — mutable data under a
  live external table is a footgun; prefer append-only partitions + rebuild, or
  Iceberg snapshots.

Follow-ups: why not one giant file per day (loses parallelism, breaks incremental
loads); late partition updates (rebuild the partition behind a `_SUCCESS` marker);
how you'd migrate two years of CSV (incremental rewrite, dual-read, backfill).

### C2. "The GCS bill doubled. The only changes: 'we turned on versioning for safety' and a new partner uploading via multipart."

Strong answer hits:

- **Versioning is prime suspect #1:** every overwrite/delete now retains a
  **noncurrent generation, billed at full rate** — a bucket that turns over daily
  keeps N days of ghosts. Diagnose with `gcloud storage ls --all-versions` or a
  storage inventory report. Then add the missing half of the design: lifecycle
  `Delete` with `NumberOfNewerVersions >= 1` (keep one ghost) or
  `DaysSinceNoncurrentTime = 30`. **Versioning without lifecycle is an open tab.**
- **Multipart is suspect #2:** incomplete resumable/multipart sessions hold uploaded
  parts **billed as storage**; add lifecycle `AbortIncompleteMultipartUpload` with
  `age: 7`.
- **The rest of the differential, unprompted (this is the senior part):** Class A ops
  explosion from polling (a 5-minute cron flat-listing 1M objects ≈ 1,000 requests
  *per sweep* — each List page is a Class A op returning ≤1,000 names);
  **early-deletion and retrieval fees** from lifecycle rules moving data into
  Nearline/Coldline younger than their minimum durations; Autoclass management fees
  on huge object counts; egress from cross-region reads; **soft-deleted storage**
  (still billed); and copy sprawl (five teams, five buckets, one dataset).
- **Layers:** bucket config (lifecycle rules), layout (compact small files; partition
  prefixes so listings are narrow), architecture (one curated copy in one place —
  consumers read, they don't clone).
- **Cost math to know cold:** Nearline minimum 30 days, Coldline 90, Archive 365 —
  deleting younger bills the prorated remainder; all three charge retrieval per GB;
  colder classes also cost more per operation.

Follow-ups: how lifecycle `age` is computed (**object creation time, not last
access** — `daysSinceNoncurrentTime` for noncurrent versions, `daysSinceCustomTime`
for contractual expiry); lifecycle executes within ~24h, not at midnight — never
build timing-sensitive logic on it; how to audit spend (billing export → BigQuery,
storage inventory).

### C3. "Compliance: transaction records must be immutable and retained 7 years; auditors want WORM proof; analytics wants to delete after 90 days."

Strong answer hits:

- **Bucket config — the core:** a **retention policy** (e.g., 2,555 days) makes object
  versions undeletable until they age out; **Bucket Lock** then makes the policy
  itself permanent — nobody (owner, admin, Google) can shorten or remove it, and the
  bucket can't be deleted while retained objects exist. Lengthening is still allowed.
  **Locking is irreversible** — stage it: run unlocked, verify the pipeline, then
  lock (and rehearse on a scratch bucket first).
- **Overwrite nuance (probe-worthy):** deletion of retained versions is blocked.
  Combine retention with **versioning** so correction flows write new generations
  while old generations stay retained — and understand they keep **billing** until
  they age out. Know exactly how your pipeline behaves on write-to-same-name under
  retention before you depend on it.
- **Holds:** per-object **temporary holds** and **event-based holds** block deletion
  for individual objects (litigation freeze, pending processing) independent of the
  bucket policy — the surgical tool when 7-year retention on everything is overkill.
- **Architecture — split the buckets:** a **compliance bucket** (retention-locked,
  Archive-class lifecycle for year 2+, CMEK, owned by the security/compliance
  project) and an **analytics bucket** (pipeline-owned, 90-day lifecycle). Same-region
  GCS→GCS copies pay no egress; the copy job runs under its own audited service
  account. Analytics lifecycle rules must never touch the compliance tier.
- **IAM:** pipeline SAs get create + read on the compliance bucket and no delete —
  belt and braces, since delete wouldn't work post-lock anyway. Separation of duties:
  the data platform can't alter the retention policy; compliance can't browse data
  contents.
- **Distinguish soft delete:** soft delete (7-day default, configurable to 90) is an
  *accidental-deletion* safety net an admin can manage — it is **not** WORM.
  Retention + Bucket Lock is the compliance control.

Follow-ups: how auditors *prove* immutability (lock metadata + Cloud Audit Logs
showing zero successful deletes); a regulator demands early deletion of one record
(impossible by design — that's the point; set retention = legal minimum, use
per-object holds for freezes); what retention does to compaction jobs (they
copy-then-age-out, never delete in place).

---

## D. Hardening

### D1. "A data scientist's 'cleanup script' ran with inherited project Editor and deleted 3 TB of raw landing data. Prevent recurrence — at every layer."

Strong answer hits:

- **Incident response first:** if versioning was on, restore noncurrent generations
  (`gcloud storage cp gs://lake/obj#<generation> ...`); if not, **soft delete**
  (7-day default) may still hold them — restore before it lapses. Then the layers:
- **IAM:** no human holds project Editor on data projects — humans get read/list
  (`objectViewer`); write access flows only through **per-pipeline service accounts**
  with least privilege (ingestion = `objectCreator`: no delete, no overwrite of
  existing objects — overwriting an existing name needs `storage.objects.update`).
  Break-glass delete lives in a separate role behind an approval; optionally an
  **IAM deny policy** on `storage.objects.delete` for everyone but the break-glass
  identity.
- **IAM conditions:** scope pipeline SAs to their prefix:
  `resource.name.startsWith("projects/_/buckets/lake/objects/ingest/team-a/")`;
  time-bound conditions for contractors. Pair conditions with **UBLA** — ACL-granted
  access can bypass them.
- **Bucket config:** **uniform bucket-level access** (disables object ACLs — kills
  the "someone shared an object with allUsers via ACL" accident class; after 90
  consecutive days enabled it can't be turned off); **public access prevention**
  enforced; versioning + soft delete as the tamper safety net; retention policies on
  raw tiers where the business case exists.
- **Architecture:** the landing bucket is **append-only by design**; deletes happen
  only through a controlled compaction job with its own audited SA. **Cloud Audit
  Logs data-access logs** (require UBLA) sink to BigQuery with an alert on any
  `DeleteObject` in raw buckets — detection, not just prevention.
- **Red flag:** answering with only "training" or only "least privilege." The senior
  answer is *layers + detection + a quarterly-tested restore path*.

Follow-ups: UBLA side effects (no ACLs; required for data-access audit logs and
object-level conditions); why signed URLs are the partner-sharing answer, not public
objects; how you'd test the restore path.

### D2. "Regulated fintech: encryption keys under our control, provably no public exposure, an audit trail of every read. Design the controls."

Strong answer hits:

- **CMEK:** bucket default `kmsKeyName` from Cloud KMS; applies **at object
  creation** — existing objects need a rewrite/copy to re-encrypt. Key location must
  be compatible with the bucket's location. Disabling or destroying the key makes
  objects unreadable (data isn't deleted — re-enabling restores access); rotation is
  configured in KMS. CMEK is about **control and revocation**, not strength — default
  encryption is already per-object AES-256. Distinguish **CSEK** (keys supplied per
  request, never stored by Google — higher ops burden, some features unsupported).
  Note CMEK encrypts object **bytes**, not names or metadata.
- **Separation of duties — the actual point of CMEK:** the security team owns the KMS
  project; data engineers can't disable keys; security can't browse data; the GCS
  service account gets scoped key-use permission only.
- **Public exposure:** **UBLA** (no ACL sharing) + **public access prevention
  enforced** at bucket level + the **org policy
  `storage.publicAccessPrevention.enforced`** so it can't be flipped per bucket;
  monitor with Security Command Center. External sharing goes through **V4 signed
  URLs** (short TTL, method- and object-scoped) — they work fine under PAP — or
  partner-owned buckets with Storage Transfer Service.
- **Audit trail:** Cloud Audit Logs — admin activity on by default; **data-access
  logs (`DATA_READ`/`DATA_WRITE`) must be enabled and require UBLA**; sink to
  BigQuery/Log Analytics for "who read object X" queries; alert on
  `allUsers`/`allAuthenticatedUsers` principals appearing anywhere.
- **IAM/architecture:** per-pipeline SAs, prefix conditions, deny policies for
  delete, documented break-glass; buckets segregated by sensitivity tier so controls
  scale per tier instead of one mega-bucket with one mega-policy.

Follow-ups: what exactly breaks when a key is disabled (writes fail immediately;
reads fail once caches expire); can you adopt CMEK later (yes — new objects only, or
rewrite existing ones); multi-region bucket ↔ key placement; why CMEK alone isn't
compliance (it's one control in the chain).

### D3. "An external partner must upload daily files. You won't create identities for them or widen bucket IAM. Options?"

Strong answer hits:

- **Signed URLs (V4):** time-limited, method-scoped PUT URLs your API mints per
  batch — no identities, no shared keys. Even better, **signed POST policies**
  constraining key prefix, max size, and content-type so they *can't* upload outside
  `incoming/partner-x/`.
- **Transfer patterns:** partner uploads to their own bucket (or S3/Azure) and
  **Storage Transfer Service** pulls on schedule — auth stays account-to-account,
  retries and checksums built in; same-region GCS→GCS transfers pay no egress.
- **Layout/naming:** a dedicated `incoming/<partner>/` prefix (or bucket) treated as
  **untrusted**: notification → validation/quarantine job → curated zone; its own
  lifecycle (`age: 14d` cleanup of *everything*, valid or not) so junk can't
  accumulate.
- **Bucket config/IAM:** UBLA + public access prevention on the incoming area; the
  minimal create-only grant goes only to whatever identity performs the transfer;
  never `allUsers` write "just for the partner."
- **Traps to name:** signed-URL leakage (mitigate with short TTL + prefix
  constraints); **Requester Pays** if partners pull from your public-ish datasets
  (callers pay ops + egress); upload-window bandwidth planning.

Follow-ups: how you'd rotate a compromised signing path; how validation
distinguishes "bad file" from "attack" (quarantine + alert, never execute);
monitoring partner SLAs (expected-file manifest, missing-file alerts).

---

## E. Quick-fire round

| Prompt | Answer |
|---|---|
| Which operations are strongly consistent? | **All of them, including listing** — read-after-write, overwrite, delete, list-after-write (since 2021). "Listing is eventually consistent" is the classic stale answer. |
| Nearline vs Coldline vs Archive minimums? | **30 / 90 / 365 days**; deleting younger bills the prorated remainder; all three charge retrieval fees; colder classes also cost more per operation. |
| When is listing expensive? | Every page is a **Class A op returning ≤1,000 objects**; flat-listing 1M objects ≈ 1,000 requests per sweep; `gs://bucket/**` lists everything. Fix: partition prefixes, `delimiter='/'`, manifests/inventory, notifications. |
| `ifGenerationMatch=0` means? | Create-only: succeed iff no live object with that name, else `412`. Two racing writers → exactly one `201`. |
| Generation vs metageneration? | Generation bumps on content writes (overwrite → new generation); metageneration bumps on metadata updates. Preconditions pair accordingly (`ifGenerationMatch` vs `ifMetagenerationMatch`). |
| Do notifications give exactly-once? | No — **at-least-once, unordered**; `OBJECT_FINALIZE` fires per generation (overwrites included). Dedupe on name + generation; ack only after durable commit. |
| #1 notification failure mode? | The **GCS service account** missing `roles/pubsub.publisher` on the topic — silent no-delivery. |
| Lifecycle `age` measures? | **Creation time, not last access**; use `daysSinceNoncurrentTime` for noncurrent versions, `daysSinceCustomTime` for contractual expiry; conditions AND together; execution lands within ~24h. |
| Are incomplete multipart uploads billed? | Yes — orphaned parts bill as storage; clean with `AbortIncompleteMultipartUpload`. |
| UBLA — and the 90-day rule? | Disables object ACLs (bucket IAM only); required for data-access audit logs and object-level IAM conditions; after 90 consecutive days enabled it becomes permanent. |
| PAP enforced vs inherited? | `enforced` blocks any `allUsers`/`allAuthenticatedUsers` binding; `inherited` defers to the org policy; the org-level constraint locks it fleet-wide. Signed URLs still work under PAP. |
| `roles/storage.objectCreator`? | Create-only: no read, no delete, no overwrite of existing objects. |
| Bucket Lock? | Retention policy permanently locked — lengthen-only; objects and the bucket are undeletable until data ages out; **irreversible**, rehearse on scratch. |
| Soft delete? | 7-day default recovery window (configurable up to 90) for deleted objects, billed — a safety net, **not** WORM. |
| Autoclass? | Per-object class transitions based on access; no retrieval/early-delete fees; small per-object management fee; great for unpredictable access, wrong for enormous tiny-object sets. |
| Max object size / resumable session life? | 5 TiB per object; resumable sessions stay valid about a week. |
| CMEK vs CSEK vs default? | Default: per-object AES-256 under Google-managed keys. CMEK: KMS key you control, applied at write (rewrite to re-encrypt, region must match). CSEK: key supplied per request, never stored by Google. |
| Same-region GCS→GCS copy cost? | No egress — you pay operations + storage only. |
| `gsutil` vs `gcloud storage`? | gsutil is deprecated for GCS work; `gcloud storage` is the successor. |
| Composite-object MD5? | It's a hash of component MD5s, not of content — verify with crc32c instead. |
| Does GCS have folders? | No — flat namespace; "folders" are prefix conventions. Zero-byte `dir/` markers are real objects that pollute listings. |

---

## F. Hands-on drills (scratch buckets only — some steps are irreversible)

1. **Racing writers / create-only semantics.** Simulate a retried upload and watch
   the second attempt resolve as "already exists":

   ```python
   from google.api_core.exceptions import PreconditionFailed
   from google.cloud import storage

   bucket = storage.Client().bucket("scratch-lake")
   blob = bucket.blob("crm/dt=2026-10-24/transactions.csv")
   try:
       blob.upload_from_string(payload, if_generation_match=0)
       print("created")
   except PreconditionFailed:
       print("already exists — retry treated as success")
   ```

   Then: overwrite without preconditions and confirm the generation increments; try
   `if_generation_match=<stale>` and read the 412.

2. **Notification end-to-end, including the classic failure.** Create a topic,
   deliberately skip the publisher grant, create the notification
   (`gcloud storage buckets notifications create gs://scratch --topic=... --event-types=OBJECT_FINALIZE`),
   upload, pull — nothing arrives. Grant the GCS SA `roles/pubsub.publisher`, upload
   again, pull and decode the payload. Upload the same object name twice; observe two
   FINALIZE events with different generations — the dedupe argument, made concrete.

3. **Versioning + generations + lifecycle cleanup.** Enable versioning, overwrite one
   object three times, `gcloud storage ls --all-versions`, restore generation 1 by
   copying `gs://scratch/obj#1`. Then apply lifecycle rules via JSON:
   `NumberOfNewerVersions: 1`, `DaysSinceNoncurrentTime: 30`,
   `AbortIncompleteMultipartUpload` at `age: 7`.

4. **Hive-pruned external table.** Build `dt=YYYY-MM-DD/` Parquet partitions and:

   ```sql
   CREATE EXTERNAL TABLE lake.transactions (...)
   OPTIONS (
     uris = ['gs://scratch/curated/dt=*/*.parquet'],
     format = 'PARQUET',
     hive_partitioning_mode = 'AUTO',
     require_hive_partition_filter = true);
   ```

   Run the same query with and without the `dt` filter (the second is rejected) and
   compare bytes scanned.

5. **Retention drill (irreversible — scratch only).**
   `gcloud storage buckets update gs://scratch --retention-period=1d`; upload; try to
   delete (fails). Then `--lock-retention-period` and confirm even the bucket owner
   cannot shorten it or delete the bucket. Enable versioning and test what a
   write-to-same-name does, then reason about the billing implication.

6. **Least-privilege proof.** Two service accounts: A gets `objectCreator` (upload
   succeeds; delete and overwrite are denied); B gets `objectViewer` plus a
   `resource.name` prefix condition. Attempt cross-prefix access and read the exact
   403s — knowing what the denial *looks like* is half the battle.

---

## G. Reusable prompt for this question class

Prepend this whenever you ask any assistant for storage interview questions, to keep
the layering covered:

> For any object storage (GCS, S3, ADLS), ask scenario questions in four categories —
> **ingestion correctness** (idempotent/retryable writes, preconditions, dedup),
> **consistency & events** (read-after-write and listing consistency, notifications,
> at-least-once + downstream idempotence), **layout/cost/lifecycle** (partitioned
> prefixes for pruning, storage classes + minimums, retention/WORM), and
> **hardening** (uniform access, versioning, per-pipeline identities, CMEK,
> public-access prevention). Phrase each as an incident or design ask, and require a
> **layered real-world answer: client code vs object layout/naming vs bucket config
> vs IAM vs architecture** — plus follow-up probes and quick-fire traps (consistency
> guarantees, class minimum durations, listing costs).

The key phrase doing the work is *"layered real-world answer: client code vs layout
vs bucket config vs IAM vs architecture"* — it forces candidates to separate what
code must guarantee from what the platform already enforces.
