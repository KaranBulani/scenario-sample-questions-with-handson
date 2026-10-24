"""
================================================================================
FASTAPI INTERVIEW SCENARIO #2 — JWT auth + NDJSON streaming + middleware + tests
================================================================================
THE ASK
    "Secure a metrics API with OAuth2 password flow + JWTs and role checks.
     Add one endpoint that streams large query results as NDJSON instead of
     one giant JSON array, per-request logging middleware, and tests that
     run without a database or real auth."

HOW TO RUN
    pip install "fastapi[standard]" pyjwt python-multipart   # multipart for the form
    uvicorn 02_jwt_streaming_testing:app --reload
      1) POST /token  (form: username=analyst, password=pw-analyst) → token
      2) GET /metrics/sales  with header: Authorization: Bearer <token>
    Tests:  pytest fastapi/02_jwt_streaming_testing.py -v

IMPLEMENTATION WALKTHROUGH
    1. OAuth2PasswordBearer(tokenUrl="token") — wires Swagger's Authorize
       button and 401 + WWW-Authenticate automatically. /token validates
       against the user store (PBKDF2 here; bcrypt/argon2 in real life)
       and issues a signed JWT: claims {sub, roles, iat, exp, jti}.
    2. get_current_user — decodes/verifies signature and expiry; raises
       401 (unauthenticated) on any JWT error. require_role(...) is a
       DEPENDENCY FACTORY returning a dependency that composes on top of
       get_current_user — this chaining is the pattern to recite.
    3. 401 vs 403 — 401 = "I don't know who you are" (bad/no token);
       403 = "I know you; you may not do that" (missing role).
    4. Streaming NDJSON — an async generator yields one JSON object per
       line. Server memory is O(1) regardless of result size, the client
       processes rows as they arrive (jq, bq load, pandas chunks). A JSON
       array response must buffer the entire payload first.
    5. Middleware — wraps EVERY request (outermost first). Here:
       X-Request-ID + duration logging; the same id ties logs to a
       response header for support. call_next hands off to the next
       middleware / the router.
    6. Tests — TestClient runs the real ASGI app in-process.
       app.dependency_overrides[get_current_user] = ... swaps auth for a
       fake user; because require_role builds on get_current_user, the
       override flows through the whole chain. Always clear overrides.

CONCEPTS THE INTERVIEWER IS PROBING
    * JWT anatomy: header.payload.signature; server verifies with the
      secret/public key — no DB lookup, which is the strength AND the
      weakness.
    * Revocation: you can't delete a stateless token → short TTLs +
      refresh tokens, or a jti denylist in Redis. Saying "stateless means
      logout doesn't exist by default" is the senior answer.
    * Why not roll your own crypto — sign with library defaults, HS256
      shared secret for one service, RS256/JWKS when multiple services
      verify.
    * Blocking calls inside async def freeze the event loop (file #1's
      trap, asked again here for the streaming endpoint).

LIKELY FOLLOW-UP QUESTIONS
    Q: Token stolen — kill it?
    A: You can't un-issue it. Mitigate: short exp, refresh-token rotation,
       jti denylist until exp, or one-time tokens minted per request.
    Q: How does dependency caching interact with auth here?
    A: get_current_user result is cached per request — decode happens once
       even if three dependencies need the user. use_cache=False forces
       re-runs (rarely wanted).
    Q: Why does the NDJSON endpoint use a generator, not return [...]?
    A: StreamingResponse pulls chunks lazily; a list return materializes
       everything (and would time out / OOM on big exports).
    Q: How would you test middleware?
    A: It's outside routing, so TestClient still exercises it — assert
       X-Request-ID presence and measure status codes; no special setup.
================================================================================
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
import uuid
from datetime import datetime, timedelta, timezone

import jwt  # pip install pyjwt
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import StreamingResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm

log = logging.getLogger("metrics")
SECRET = "dev-secret"            # prod: Secret Manager; rotate via kid/JWKS
ALG = "HS256"
TOKEN_TTL_MIN = 30

oauth2 = OAuth2PasswordBearer(tokenUrl="token")


def _hash(pw: str) -> str:
    return hashlib.pbkdf2_hmac("sha256", pw.encode(), b"de-interview", 100_000).hex()


USERS = {
    "analyst": {"hash": _hash("pw-analyst"), "roles": ["reader"]},
    "eng": {"hash": _hash("pw-eng"), "roles": ["reader", "writer"]},
}


def create_token(username: str, roles: list[str]) -> str:
    now = datetime.now(timezone.utc)
    claims = {
        "sub": username, "roles": roles, "iat": now, "jti": uuid.uuid4().hex,
        "exp": now + timedelta(minutes=TOKEN_TTL_MIN),
    }
    return jwt.encode(claims, SECRET, algorithm=ALG)


async def get_current_user(token: str = Depends(oauth2)) -> dict:
    creds_error = HTTPException(
        status.HTTP_401_UNAUTHORIZED, "invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        claims = jwt.decode(token, SECRET, algorithms=[ALG])  # checks exp + signature
    except jwt.PyJWTError:
        raise creds_error
    return {"username": claims["sub"], "roles": claims.get("roles", [])}


def require_role(role: str):
    """Dependency factory: composes auth on top of get_current_user."""
    async def checker(user: dict = Depends(get_current_user)) -> dict:
        if role not in user["roles"]:
            raise HTTPException(status.HTTP_403_FORBIDDEN, f"role {role!r} required")
        return user
    return checker


app = FastAPI(title="Metrics API", version="1.0.0")


@app.middleware("http")  # wraps every request; first added = outermost
async def request_middleware(request, call_next):
    rid = uuid.uuid4().hex
    start = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Request-ID"] = rid
    log.info("%s %s -> %s in %.1fms [%s]", request.method, request.url.path,
             response.status_code, (time.perf_counter() - start) * 1000, rid)
    return response


@app.post("/token")
async def issue_token(form: OAuth2PasswordRequestForm = Depends()):
    user = USERS.get(form.username)
    if user is None or _hash(form.password) != user["hash"]:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "bad credentials")
    return {"access_token": create_token(form.username, user["roles"]), "token_type": "bearer"}


async def bq_rows(dataset: str):  # stand-in for an async BigQuery job iterator
    for i in range(5):
        yield {"dataset": dataset, "row": i, "value": i * 100}


@app.get("/metrics/{dataset}", dependencies=[Depends(require_role("reader"))])
async def stream_metrics(dataset: str):
    """NDJSON: one JSON object per line — O(1) server memory, streamable client."""

    async def rows():
        async for row in bq_rows(dataset):
            yield json.dumps(row) + "\n"

    return StreamingResponse(rows(), media_type="application/x-ndjson")


# ------------------- tests (pytest; no server, no DB, no real auth) ------------

def test_metrics_requires_auth():
    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        assert client.get("/metrics/sales").status_code == 401


def test_metrics_streams_ndjson():
    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        app.dependency_overrides[get_current_user] = lambda: {
            "username": "test", "roles": ["reader"]
        }
        try:
            r = client.get("/metrics/sales")
            assert r.status_code == 200
            assert r.headers["X-Request-ID"]                      # middleware ran
            assert len(r.text.strip().splitlines()) == 5           # five NDJSON rows
            assert json.loads(r.text.splitlines()[0])["dataset"] == "sales"
        finally:
            app.dependency_overrides.clear()
