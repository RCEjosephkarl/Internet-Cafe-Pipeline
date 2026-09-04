"""Smoke tests for ``streamlit_app/lib/api_client.py``.

Mocks ``httpx.get`` — no live API, no network. Only proves the client handles both the happy
path and the ``{"source": "unavailable", ...}`` degrade shape (and an outright connection
failure) without raising. Streamlit pages/UI are not tested here or anywhere in this suite.
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

STREAMLIT_APP = Path(__file__).resolve().parents[2] / "streamlit_app"
if str(STREAMLIT_APP) not in sys.path:
    sys.path.insert(0, str(STREAMLIT_APP))

from lib import api_client  # noqa: E402  (path must be extended first)


class _FakeResponse:
    def __init__(self, payload: Any, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("boom", request=None, response=self)  # type: ignore[arg-type]

    def json(self) -> Any:
        return self._payload


@pytest.fixture(autouse=True)
def _clear_streamlit_cache() -> None:
    """``st.cache_data`` persists across calls within a process; each test needs a clean slate.

    Skips dunder attributes deliberately: a module's ``__builtins__`` is a plain ``dict``,
    which also has a ``.clear()`` method -- iterating ``dir()`` without that guard finds it,
    calls it, and wipes the interpreter's builtins out from under the rest of the test run.
    """
    for attr in dir(api_client):
        if attr.startswith("_"):
            continue
        value = getattr(api_client, attr)
        if callable(getattr(value, "clear", None)):
            with contextlib.suppress(Exception):  # cache internals vary; best-effort reset
                value.clear()


def test_a_happy_path_response_is_returned_as_is(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        httpx, "get", lambda url, params=None, timeout=None: _FakeResponse({"active_rentals": 3})
    )
    result = api_client.get_summary()
    assert result == {"active_rentals": 3}


def test_a_degraded_upstream_response_passes_through_unmodified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    degraded = {"source": "unavailable", "days": 7, "cells": [], "error": "boom"}
    monkeypatch.setattr(
        httpx, "get", lambda url, params=None, timeout=None: _FakeResponse(degraded)
    )
    result = api_client.get_utilization_heatmap(7)
    assert result == degraded


def test_an_unreachable_api_degrades_instead_of_raising(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(url: str, params: Any = None, timeout: Any = None) -> Any:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx, "get", _raise)
    result = api_client.get_summary()
    assert result["source"] == "unavailable"
    assert "error" in result


def test_requests_target_the_metrics_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_urls: list[str] = []

    def _get(url: str, params: Any = None, timeout: Any = None) -> _FakeResponse:
        seen_urls.append(url)
        return _FakeResponse({})

    monkeypatch.setattr(httpx, "get", _get)
    api_client.get_workstations_status()
    assert seen_urls
    assert "/v1/metrics/workstations/status" in seen_urls[0]
