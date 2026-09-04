"""Thin HTTP client for the Streamlit dashboard.

HTTP only, by design (CLAUDE.md invariant: "the dashboard talks to /v1/metrics/* over HTTP
only"; enforced by ``tests/unit/test_streamlit_boundary.py``). This module imports ``httpx``
and nothing that could reach a database directly — no ``aimternet.db``, no
``aimternet.config.settings``, no driver. It reads the API's base URL from the same
environment variable the rest of the repo uses (``AIMTERNET_API_BASE_URL``), via ``os.environ``
directly rather than through ``aimternet.config.settings`` (which pulls in the whole typed
settings module and its DB-adjacent fields).

Every function is wrapped in ``st.cache_data(ttl=60)``, matching the API's own
``_CACHE_TTL_SECONDS`` (``routers/metrics.py``) so the two caches expire in step.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
import streamlit as st

API_BASE_URL = os.environ.get("AIMTERNET_API_BASE_URL", "http://127.0.0.1:8000")
_TIMEOUT = 10.0


def _get(path: str, params: dict[str, Any] | None = None) -> Any:
    """GET one ``/v1/metrics/*`` endpoint.

    Never raises: an unreachable API degrades into the same
    ``{"source": "unavailable", "error": ...}`` shape the endpoints themselves already return
    for an unreachable Redshift/DynamoDB, so every page only has to handle one failure shape.
    """
    url = f"{API_BASE_URL}/v1/metrics{path}"
    try:
        response = httpx.get(url, params=params, timeout=_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as exc:
        return {"source": "unavailable", "error": f"API unreachable: {exc}"}


# --------------------------------------------------------------------------- shared


@st.cache_data(ttl=60)
def get_data_window() -> dict[str, Any]:
    return _get("/data-window")


@st.cache_data(ttl=60)
def get_summary() -> dict[str, Any]:
    return _get("/summary")


# --------------------------------------------------------------------------- PC Telemetry


@st.cache_data(ttl=60)
def get_workstations_status() -> dict[str, Any]:
    return _get("/workstations/status")


@st.cache_data(ttl=60)
def get_rentals_active() -> list[dict[str, Any]]:
    result = _get("/rentals/active")
    return result if isinstance(result, list) else []


@st.cache_data(ttl=60)
def get_telemetry_recent(limit: int = 12) -> dict[str, Any]:
    return _get("/telemetry/recent", {"limit": limit})


@st.cache_data(ttl=60)
def get_telemetry_fleet_health() -> dict[str, Any]:
    return _get("/telemetry/fleet-health")


@st.cache_data(ttl=60)
def get_utilization_hourly(days: int) -> dict[str, Any]:
    return _get("/utilization/hourly", {"days": days})


@st.cache_data(ttl=60)
def get_utilization_heatmap(days: int) -> dict[str, Any]:
    return _get("/utilization/heatmap", {"days": days})


# --------------------------------------------------------------------------- Descriptive Analytics


@st.cache_data(ttl=60)
def get_revenue_today() -> dict[str, Any]:
    return _get("/revenue/today")


@st.cache_data(ttl=60)
def get_revenue_by_zone(days: int) -> dict[str, Any]:
    return _get("/revenue/by-zone", {"days": days})


@st.cache_data(ttl=60)
def get_revenue_trend(days: int) -> dict[str, Any]:
    return _get("/revenue/trend", {"days": days})


@st.cache_data(ttl=60)
def get_points() -> dict[str, Any]:
    return _get("/points")


@st.cache_data(ttl=60)
def get_points_history(days: int) -> dict[str, Any]:
    return _get("/points/history", {"days": days})


@st.cache_data(ttl=60)
def get_members_overview(days: int) -> dict[str, Any]:
    return _get("/members/overview", {"days": days})


# --------------------------------------------------------------------------- Data Science


@st.cache_data(ttl=60)
def get_members_leaderboard(days: int, limit: int = 25) -> dict[str, Any]:
    return _get("/members/leaderboard", {"days": days, "limit": limit})


@st.cache_data(ttl=60)
def get_members_tier_migration(days: int) -> dict[str, Any]:
    return _get("/members/tier-migration", {"days": days})


@st.cache_data(ttl=60)
def get_workstations_health_score(days: int) -> dict[str, Any]:
    return _get("/workstations/health-score", {"days": days})


@st.cache_data(ttl=60)
def get_efficiency_revenue_per_hour(days: int) -> dict[str, Any]:
    return _get("/efficiency/revenue-per-hour", {"days": days})
