"""Idempotency for write endpoints (spec §7.1: "every write is safe to retry").

A POS terminal on a flaky connection will retry. Without this, a retried check-in creates a
second rental and a retried purchase charges twice and takes stock twice.

The client sends ``Idempotency-Key``. The first request stores its response against that key;
a replay returns the stored response instead of doing the work again. A replay of the *same
key* with a *different body* is a conflict, not a replay — that means the client reused a key
by mistake, and quietly returning the wrong stored answer would be worse than an error.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from aimternet.api import errors
from aimternet.db.session import connection

log = logging.getLogger(__name__)


def request_fingerprint(payload: Any) -> str:
    """A stable hash of the request body, so key reuse with different data is detectable."""
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def lookup(key: str, endpoint: str, fingerprint: str) -> tuple[int, dict[str, Any]] | None:
    """The stored response for this key, or None if it is the first time we have seen it."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT endpoint, request_hash, response_status, response_body "
            "FROM api_idempotency WHERE idempotency_key = %s",
            (key,),
        )
        row = cur.fetchone()

    if row is None:
        return None

    stored_endpoint, stored_hash, status_code, body = row
    if stored_endpoint != endpoint or stored_hash != fingerprint:
        raise errors.IdempotencyConflict(
            f"idempotency key {key!r} was already used for a different request; "
            f"use a fresh key rather than reusing one",
            idempotency_key=key,
            original_endpoint=stored_endpoint,
        )
    log.info("idempotency key %s replayed; returning the stored response", key)
    return int(status_code), dict(body)


def remember(
    key: str, endpoint: str, fingerprint: str, status_code: int, body: dict[str, Any]
) -> None:
    """Store a successful response so a retry can be answered from it."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO api_idempotency
                (idempotency_key, endpoint, request_hash, response_status, response_body)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
            """,
            (key, endpoint, fingerprint, status_code, json.dumps(body, default=str)),
        )
