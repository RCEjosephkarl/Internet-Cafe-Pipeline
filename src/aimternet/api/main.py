"""The AIMternet-Cafe operational API (spec §7.1, §7.2, §7.4).

This process is the only write path for the POS terminal, and it also serves the read-only
metrics router and the static dashboard. One app, three concerns kept separate:

* ``/v1/...``      — operational writes and reads, backed by RDS
* ``/v1/metrics/...`` — read-only aggregates
* ``/dashboard``   — static HTML/JS that talks to the metrics API and nothing else

There is deliberately no endpoint that accepts SQL (§3).
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path

from fastapi import FastAPI, Request, status
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from aimternet import __version__
from aimternet.api import errors
from aimternet.api.routers import concessions, members, metrics, rentals, workstations
from aimternet.api.schemas import Health
from aimternet.config.settings import settings

log = logging.getLogger("aimternet.api")

DASHBOARD_DIR = Path(__file__).resolve().parents[3] / "dashboard"

app = FastAPI(
    title="AIMternet-Cafe Operational API",
    version=__version__,
    description=(
        "The only write path for the POS terminal. Pricing, tier discounts, point accrual "
        "and redemption, inventory and transactional integrity are all enforced here, not "
        "in the client."
    ),
)


@app.middleware("http")
async def correlation_and_logging(
    request: Request, call_next: Callable[[Request], Awaitable[object]]
) -> object:
    """Attach a correlation id to every request and log the outcome (spec §7.1).

    The id is echoed in the response header and in every problem document, so an operator
    reporting "it said conflict" can be matched to the exact request in the logs.
    """
    correlation_id = request.headers.get("X-Correlation-ID") or uuid.uuid4().hex[:16]
    request.state.correlation_id = correlation_id
    started = time.perf_counter()

    response = await call_next(request)

    elapsed_ms = (time.perf_counter() - started) * 1000
    response.headers["X-Correlation-ID"] = correlation_id  # type: ignore[attr-defined]
    log.info(
        "%s %s -> %s in %.1fms [%s]",
        request.method, request.url.path,
        getattr(response, "status_code", "?"), elapsed_ms, correlation_id,
    )
    return response


@app.exception_handler(errors.ApiProblem)
async def handle_problem(request: Request, exc: errors.ApiProblem) -> JSONResponse:
    """Every expected failure becomes a structured problem document."""
    log.info(
        "%s on %s: %s [%s]",
        exc.__class__.__name__, request.url.path, exc.detail,
        getattr(request.state, "correlation_id", "-"),
    )
    return exc.to_response(request)


@app.exception_handler(Exception)
async def handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
    """An unexpected failure still returns the same shape, and is logged with a traceback."""
    correlation_id = getattr(request.state, "correlation_id", None)
    log.exception("unhandled error on %s [%s]", request.url.path, correlation_id)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "type": "/problems/internal-error",
            "title": "InternalError",
            "status": 500,
            "detail": "the request could not be completed",
            "instance": str(request.url.path),
            "correlation_id": correlation_id,
        },
    )


app.include_router(workstations.router)
app.include_router(members.router)
app.include_router(rentals.router)
app.include_router(concessions.router)
app.include_router(metrics.router)


@app.get("/healthz", response_model=Health, tags=["ops"])
def healthz() -> Health:
    """Liveness plus a real database round trip — a health check that never touches its
    dependencies reports health it cannot vouch for."""
    cfg = settings()
    database = "unreachable"
    try:
        from aimternet.db.session import fetch_all

        fetch_all("SELECT 1 AS ok")
        database = "ok"
    except Exception as exc:
        database = f"error: {type(exc).__name__}"

    return Health(
        status="ok" if database == "ok" else "degraded",
        database=database,
        schema_name=cfg.pg_schema,
        version=__version__,
    )


@app.get("/dashboard", include_in_schema=False)
def dashboard() -> FileResponse:
    """The operations dashboard (spec §7.4). Static HTML/JS; it reads the metrics API only."""
    return FileResponse(DASHBOARD_DIR / "index.html")


if DASHBOARD_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=DASHBOARD_DIR), name="static")


@app.get("/", include_in_schema=False)
def index() -> dict[str, str]:
    return {
        "service": "AIMternet-Cafe Operational API",
        "version": __version__,
        "docs": "/docs",
        "dashboard": "/dashboard",
        "health": "/healthz",
    }
