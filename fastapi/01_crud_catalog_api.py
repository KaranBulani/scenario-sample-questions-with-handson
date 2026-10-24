"""
================================================================================
FASTAPI INTERVIEW SCENARIO #1 — Data-catalog CRUD API (DI, validation, async)
================================================================================
THE ASK
    "Build a small data-catalog service: create/list/get/patch/delete
     datasets with validation, API-key auth, pagination, an async endpoint
     that triggers a pipeline refresh in the background, and health/readiness
     probes. It should be stateless enough to scale horizontally."

HOW TO RUN
    pip install "fastapi[standard]"        # uvicorn included
    uvicorn fastapi.01_crud_catalog_api:app --reload   (package-style import)
    or from this folder:  uvicorn 01_crud_catalog_api:app --reload
    Docs at http://127.0.0.1:8000/docs (OpenAPI is automatic — mention it).

IMPLEMENTATION WALKTHROUGH
    1. Lifespan — replaces the deprecated @app.on_event. Startup creates
       the store/clients (in real life: async engine pool, BigQuery
       client); shutdown closes them. Yield = the app-serving moment.
    2. Dependencies — get_store pulls from app.state; require_api_key
       guards endpoints. FastAPI caches a dependency's result PER REQUEST:
       both deps here call get_store once per request even if injected
       twice. A dependency with `yield` runs cleanup after the response.
    3. Pydantic v2 — DatasetIn validates + documents the request body
       (Field constraints → automatic 422 with field-level errors).
       DatasetPatch is all-optional → PATCH semantics. DatasetOut is the
       response contract: response_model strips anything not declared, so
       accidental extra fields can never leak.
    4. Pagination — limit/offset with sane caps (le=100). Offset pagination
       is fine for a catalog; a cursor token is the answer for mutable,
       high-churn feeds (interviewers love the trade-off talk).
    5. BackgroundTasks — refresh runs AFTER the response is sent, on the
       same event loop: fine for a lightweight "kick the Airflow REST API"
       call; anything CPU-heavy or slow belongs in Celery/Cloud Tasks.
    6. /healthz (liveness: process up — restart me if this fails) vs
       /readyz (readiness: dependencies OK — stop routing traffic if not).
       Kubernetes/Cloud Run wording; knowing the difference scores points.

CONCEPTS THE INTERVIEWER IS PROBING
    * async def vs def endpoints: `def` endpoints run in a threadpool and
      are FINE for blocking sync clients; `async def` + blocking calls
      (requests, time.sleep) stalls the ENTIRE event loop — the classic
      trap question.
    * DI as composition: auth deps chain (see file #2 for the factory
      pattern), testability via dependency_overrides.
    * Status codes: 201 create, 202 accepted-for-later, 204 delete,
      404 missing, 422 validation (FastAPI default).
    * Horizontal scaling: this app is stateless because the store is
      injected — swap CatalogStore for an async DB session and every
      worker shares state through Postgres.

LIKELY FOLLOW-UP QUESTIONS
    Q: Why async at all if the DB driver is sync?
    A: Use plain `def` endpoints (threadpool) or an async driver
       (asyncpg/async-bigquery). Mixing async def + sync IO is the worst
       option — one slow query freezes every request on that worker.
    Q: How would you test this?
    A: TestClient(app) with dependency_overrides[get_store] = fake_store —
       no real DB, no HTTP server. (Demonstrated in file #2.)
    Q: PATCH with all-None body?
    A: Fields stay untouched — model_dump(exclude_unset=True) on the
       server distinguishes "absent" from "set to null".
    Q: Where does API-key auth actually belong?
    A: For machine clients, a dependency is fine; for humans, JWT (file
       #2); in a shared platform, an API gateway in front.
================================================================================
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import (
    BackgroundTasks, Depends, FastAPI, Header, HTTPException, Query, Request, status,
)
from pydantic import BaseModel, Field

log = logging.getLogger("catalog")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class CatalogStore:
    """Stand-in for an async SQLAlchemy session / BigQuery client."""

    def __init__(self) -> None:
        self._rows: dict[str, dict] = {}

    async def create(self, payload: dict) -> dict:
        row = {"id": uuid.uuid4().hex[:12], "created_at": _now(), "updated_at": _now(), **payload}
        self._rows[row["id"]] = row
        return row

    async def list(self, owner: str | None, limit: int, offset: int) -> tuple[list[dict], int]:
        rows = [r for r in self._rows.values() if owner is None or r["owner"] == owner]
        return rows[offset: offset + limit], len(rows)

    async def get(self, ds_id: str) -> dict | None:
        return self._rows.get(ds_id)

    async def patch(self, ds_id: str, updates: dict) -> dict | None:
        row = self._rows.get(ds_id)
        if row is None:
            return None
        row.update(updates, updated_at=_now())
        return row

    async def delete(self, ds_id: str) -> bool:
        return self._rows.pop(ds_id, None) is not None


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.store = CatalogStore()      # startup: pools, clients
    yield
    app.state.store = None                # shutdown: close connections


app = FastAPI(title="Data Catalog API", version="1.0.0", lifespan=lifespan)


# ---------------------------- dependencies ------------------------------------

def get_store(request: Request) -> CatalogStore:
    return request.app.state.store


async def require_api_key(x_api_key: str = Header()) -> str:
    if x_api_key != "let-me-in":          # real life: Secret Manager + rotation
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="invalid api key")
    return x_api_key


StoreDep = Depends(get_store)
AuthDep = Depends(require_api_key)


# ------------------------------- models ---------------------------------------

class DatasetIn(BaseModel):
    name: str = Field(min_length=3, max_length=64, examples=["sales_orders"])
    owner: str = Field(min_length=3)
    source: str = Field(examples=["bigquery://proj.datasets.sales"])
    description: str | None = None
    tags: list[str] = []


class DatasetPatch(BaseModel):
    description: str | None = None
    tags: list[str] | None = None          # all-optional → PATCH semantics


class DatasetOut(DatasetIn):
    id: str
    created_at: datetime
    updated_at: datetime


# ------------------------------ endpoints -------------------------------------

async def _get_or_404(store: CatalogStore, ds_id: str) -> dict:
    row = await store.get(ds_id)
    if row is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"dataset {ds_id!r} not found")
    return row


@app.post("/datasets", status_code=201, response_model=DatasetOut, tags=["datasets"])
async def create_dataset(payload: DatasetIn, store: CatalogStore = StoreDep, _: str = AuthDep):
    return await store.create(payload.model_dump())


@app.get("/datasets", response_model=list[DatasetOut], tags=["datasets"])
async def list_datasets(
    owner: str | None = None,
    limit: int = Query(20, le=100),
    offset: int = Query(0, ge=0),
    store: CatalogStore = StoreDep,
    _: str = AuthDep,
):
    rows, total = await store.list(owner, limit, offset)
    return rows  # offset pagination is fine for catalogs; cursors for churny feeds


@app.get("/datasets/{ds_id}", response_model=DatasetOut, tags=["datasets"])
async def get_dataset(ds_id: str, store: CatalogStore = StoreDep, _: str = AuthDep):
    return await _get_or_404(store, ds_id)


@app.patch("/datasets/{ds_id}", response_model=DatasetOut, tags=["datasets"])
async def patch_dataset(ds_id: str, patch: DatasetPatch, store=StoreDep, _: str = AuthDep):
    await _get_or_404(store, ds_id)
    updates = {k: v for k, v in patch.model_dump(exclude_unset=True).items() if v is not None}
    return await store.patch(ds_id, updates)


@app.delete("/datasets/{ds_id}", status_code=204, tags=["datasets"])
async def delete_dataset(ds_id: str, store=StoreDep, _: str = AuthDep):
    if not await store.delete(ds_id):
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"dataset {ds_id!r} not found")


async def trigger_airflow_dag(ds_id: str) -> None:
    await asyncio.sleep(2)  # stand-in: POST /api/v1/dags/{dag}/dagRuns (Airflow REST)
    log.info("refresh triggered for %s", ds_id)


@app.post("/datasets/{ds_id}/refresh", status_code=202, tags=["datasets"])
async def refresh_dataset(
    ds_id: str, background: BackgroundTasks, store: CatalogStore = StoreDep, _: str = AuthDep
):
    await _get_or_404(store, ds_id)
    background.add_task(trigger_airflow_dag, ds_id)   # runs after the response is sent
    return {"status": "refresh triggered", "dataset": ds_id}


# ------------------------------- probes ---------------------------------------

@app.get("/healthz", tags=["ops"])          # liveness: is the process up?
async def healthz():
    return {"status": "ok"}


@app.get("/readyz", tags=["ops"])           # readiness: are dependencies usable?
async def readyz(request: Request):
    if getattr(request.app.state, "store", None) is None:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail="store not ready")
    return {"status": "ready"}
