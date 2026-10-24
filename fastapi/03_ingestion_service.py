"""
================================================================================
FASTAPI INTERVIEW SCENARIO #3 — Ingestion service: pagination, retries,
                                 concurrency, cursors, tests (topics 5+6+7)
================================================================================
THE ASK
    "Build a service that syncs several sources from a partner REST API into
     the warehouse: follow pagination, retry transient failures with
     exponential backoff, respect Retry-After, keep an incremental cursor so
     re-runs never reprocess old rows, fetch sources concurrently but
     politely, use the blocking warehouse SDK anyway, and prove it works
     with tests that don't touch the real upstream."

HOW TO RUN
    pip install "fastapi[standard]" httpx
    uvicorn 03_ingestion_service:app --reload    # from fastapi/
    POST /sync?sources=orders,customers   → 202 + run_id, runs in background
    GET  /sync/cursors                    → current checkpoint per source
    Tests:  pytest fastapi/03_ingestion_service.py -v

IMPLEMENTATION WALKTHROUGH
    1. fetch_page — HAND-ROLLED retry loop (interviewers ask you to write
       it, not import tenacity): retry only on retryable statuses
       (429/5xx), exponential delay 2**attempt, plus jitter so a cohort of
       failing workers doesn't retry in lockstep ("thundering herd"), and
       honor the server's Retry-After over our own math.
    2. paginate — an ASYNC GENERATOR yielding pages; the caller's loop
       stays flat and readable and memory stays O(page) even with
       thousands of pages. This pattern gets reused everywhere in DE code.
    3. run_ingestion — fan-out with asyncio.gather over sources, gated by
       Semaphore(4): concurrent (event-loop IO), but bounded so we don't
       hammer the partner API or blow memory. Async = one thread, tasks
       interleave at await points — perfect for IO, useless for CPU
       (the GIL question lands right here).
    4. Cursor checkpointing — only rows with id > cursor are written; the
       cursor advances ONLY after the batch is durably written. Crash
       before write → cursor unchanged → reprocess (at-least-once), never
       skip. Write batch + cursor in ONE transaction in real life
       (or make the load idempotent via MERGE on row id) → effectively
       exactly-once ingestion.
    5. Blocking SDK escape hatch — blocking_sync_sdk stands in for any
       sync-only client (old DB drivers, some GCP SDKs): awaiting it
       directly would freeze the event loop, so it runs on the default
       ThreadPoolExecutor via run_in_executor.
    6. Tests — no real upstream: a fake client object for the retry test,
       unittest.mock.patch to swap paginate/write for the incremental test,
       and patch("asyncio.sleep") so backoff doesn't slow the suite.

CONCEPTS THE INTERVIEWER IS PROBING
    * Incremental extraction: monotonic cursor vs updated_at watermark —
      cursors need stable ordering; updated_at needs an overlap window +
      dedupe for late upserts.
    * Retry design: which statuses are retryable, why jitter, why cap
      attempts, idempotency of the retried call (GET = safe).
    * Async concurrency vs threads vs processes; when the GIL matters.
    * Checkpoint atomicity — cursor must never outrun durable writes.
    * Test doubles: fakes vs mocks, patch targets, async generators.

LIKELY FOLLOW-UP QUESTIONS
    Q: Upstream has no stable ordering — cursor breaks. Now what?
    A: Use a time watermark + overlap (e.g., re-pull updated_since - 24h)
       plus MERGE-by-id dedupe. Say "high-water mark + overlap window".
    Q: Why not a ThreadPoolExecutor for the HTTP fetches?
    A: Works (requests + executor), but async gives thousands of cheap
       concurrent tasks vs tens of threads; threads make sense for
       blocking SDKs, which is exactly where run_in_executor is used.
    Q: Partner rate-limits to 10 req/s globally?
    A: Semaphore caps CONCURRENCY, not RATE — add a token-bucket limiter
       (see python/01) or respect Retry-After/429 with backoff.
    Q: How do you observe a background run?
    A: Persist run_id → status rows; /sync/cursors grows into a runs
       table; alert on runs that never reach "committed".
================================================================================
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import BackgroundTasks, FastAPI, Request
from fastapi import status as http_status

log = logging.getLogger("ingestion")
UPSTREAM = "https://partner.api/v1"
RETRYABLE = {429, 500, 502, 503, 504}
MAX_RETRIES = 4


class CursorStore:
    """Stand-in for a checkpoint table (BigQuery metadata / GCS state file)."""

    def __init__(self) -> None:
        self._cursors: dict[str, str] = {}
        self.last_run: dict[str, str] = {}

    def get(self, source: str) -> str | None:
        return self._cursors.get(source)

    def advance(self, source: str, last_seen: str | None, run_id: str) -> None:
        self._cursors[source] = last_seen or ""      # ONLY after durable writes
        self.last_run[source] = run_id

    def snapshot(self) -> dict[str, str]:
        return dict(self._cursors)


async def fetch_page(client: httpx.AsyncClient, url: str, params: dict) -> dict:
    """One page, hand-rolled exponential backoff + jitter + Retry-After."""
    for attempt in range(MAX_RETRIES + 1):
        resp = await client.get(url, params=params)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code not in RETRYABLE or attempt == MAX_RETRIES:
            resp.raise_for_status()                  # 4xx / out of retries: fail loudly
        retry_after = float(resp.headers.get("Retry-After", 0) or 0)
        delay = max(retry_after, 2 ** attempt + random.random())
        log.warning("%s -> %s; retry %d/%d in %.1fs", url, resp.status_code,
                    attempt + 1, MAX_RETRIES, delay)
        await asyncio.sleep(delay)
    raise RuntimeError("unreachable")  # for type checkers


async def paginate(client: httpx.AsyncClient, source: str):
    """Async generator over pages — O(page) memory for any result size."""
    token: str | None = None
    while True:
        params = {"page_token": token} if token else {}
        page = await fetch_page(client, f"{UPSTREAM}/{source}", params)
        yield page
        token = page.get("next_page_token")
        if not token:
            return


def blocking_sync_sdk(page: dict) -> int:
    """Stand-in for a sync-only warehouse client (old DB driver, some SDKs)."""
    time.sleep(0.05)                                  # blocking on purpose
    return len(page["rows"])


async def write_batch_via_blocking_client(page: dict) -> int:
    """Run the blocking call on the threadpool — NEVER inside the event loop."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, blocking_sync_sdk, page)


async def run_ingestion(state, sources: list[str], run_id: str) -> dict[str, int]:
    client, cursors = state.client, state.cursors
    sem = asyncio.Semaphore(4)                        # bounded concurrency

    async def sync_one(source: str) -> int:
        async with sem:
            last_seen = cursors.get(source)
            written, high_water = 0, last_seen
            async for page in paginate(client, source):
                new_rows = [r for r in page["rows"] if not last_seen or r["id"] > last_seen]
                if new_rows:
                    written += await write_batch_via_blocking_client({"rows": new_rows})
                    high_water = max(high_water or "", max(r["id"] for r in new_rows))
            cursors.advance(source, high_water, run_id)   # checkpoint after success
            log.info("[%s] %s: wrote %s rows", run_id, source, written)
            return written

    counts = await asyncio.gather(*(sync_one(s) for s in sources))
    return dict(zip(sources, counts))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = httpx.AsyncClient(timeout=10)
    app.state.cursors = CursorStore()
    yield
    await app.state.client.aclose()


app = FastAPI(title="Ingestion Service", version="1.0.0", lifespan=lifespan)


@app.post("/sync", status_code=http_status.HTTP_202_ACCEPTED, tags=["ingestion"])
async def trigger_sync(
    request: Request, background: BackgroundTasks, sources: str = "orders,customers"
):
    run_id = uuid.uuid4().hex[:8]
    background.add_task(run_ingestion, request.app.state, sources.split(","), run_id)
    return {"status": "accepted", "run_id": run_id, "sources": sources.split(",")}


@app.get("/sync/cursors", tags=["ingestion"])
async def get_cursors(request: Request):
    return request.app.state.cursors.snapshot()


# ------------------- tests (pytest; no server, no real upstream) ---------------

def test_fetch_page_retries_then_succeeds():
    import asyncio
    from unittest.mock import patch

    import httpx as _httpx

    calls = {"n": 0}

    class FakeClient:
        async def get(self, url, params=None):
            calls["n"] += 1
            req = _httpx.Request("GET", url)
            if calls["n"] < 3:
                return _httpx.Response(503, request=req)
            return _httpx.Response(200, request=req,
                                   json={"rows": [{"id": "r1"}], "next_page_token": None})

    async def _no_sleep(_delay):        # keep the suite fast
        return None

    with patch("asyncio.sleep", _no_sleep):
        page = asyncio.run(fetch_page(FakeClient(), "https://x", {}))
    assert calls["n"] == 3 and page["rows"][0]["id"] == "r1"


def test_run_ingestion_is_incremental():
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import patch

    state = SimpleNamespace(client=None, cursors=CursorStore())

    async def fake_paginate(client, source):          # same upstream data both runs
        yield {"rows": [{"id": "r1"}, {"id": "r2"}], "next_page_token": None}

    async def fake_write(page):
        return len(page["rows"])

    mods = f"{__name__}.paginate", f"{__name__}.write_batch_via_blocking_client"
    with patch(mods[0], fake_paginate), patch(mods[1], fake_write):
        first = asyncio.run(run_ingestion(state, ["orders"], "run1"))
        second = asyncio.run(run_ingestion(state, ["orders"], "run2"))
    assert first == {"orders": 2} and second == {"orders": 0}   # cursor filtered
    assert state.cursors.snapshot() == {"orders": "r2"}
