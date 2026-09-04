"""Redshift connections and the COPY-credential decision.

The IAM role for ``COPY`` is deliberately not hardcoded. Three sources are tried in order,
and whichever one works is reported rather than assumed:

1. ``AIMTERNET_REDSHIFT_COPY_IAM_ROLE`` if set,
2. ``IAM_ROLE default`` -- works when the cluster has a default role attached,
3. no role, in which case the loader falls back to batched INSERT and says so.

The third case is decided by ``probe_copy_capability``, which tries a COPY and reads the
error, rather than by ``copy_credentials_clause``, which only ever builds a clause.

Inline access keys are never an option (spec §6.5).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import redshift_connector

from aimternet.config.settings import settings

log = logging.getLogger(__name__)


@contextmanager
def connection(*, autocommit: bool = False) -> Iterator[Any]:
    cfg = settings()
    conn = redshift_connector.connect(**cfg.redshift_credentials())  # type: ignore[arg-type]
    try:
        conn.autocommit = autocommit
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def ensure_schema() -> str:
    """Create the analytics schema if it is absent, and return its name.

    Redshift -- unlike PostgreSQL -- raises on ``SET search_path`` to a schema that does not
    exist, so this has to happen before any other statement in a fresh cluster.
    """
    cfg = settings()
    with connection(autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {cfg.redshift_schema}")
    return cfg.redshift_schema


@contextmanager
def cursor(*, autocommit: bool = False, schema: bool = True) -> Iterator[Any]:
    with connection(autocommit=autocommit) as conn, conn.cursor() as cur:
        if schema:
            cfg = settings()
            cur.execute(f"SET search_path TO {cfg.redshift_schema}, public")
        yield cur


def fetch_all(sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    with cursor() as cur:
        cur.execute(sql, params) if params else cur.execute(sql)
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


def copy_credentials_clause() -> str:
    """The ``IAM_ROLE`` clause for a COPY.

    Two cases, not three. Either ``AIMTERNET_REDSHIFT_COPY_IAM_ROLE`` names a role, or the
    clause asks the cluster for its default one -- and whether that default exists is not
    something this function can know, so it does not guess. ``probe_copy_capability`` answers
    that by trying it, and ``choose_strategy`` falls back to batched INSERT when the answer is
    no.

    This used to be typed ``str | None`` with a docstring promising None "when no role is
    available", and no code path that returned it. Had one ever been added, the caller's
    f-string would have emitted the literal ``COPY ... None FORMAT AS PARQUET``.
    """
    configured = settings().redshift_copy_iam_role.strip()
    if configured:
        return f"IAM_ROLE '{configured}'"
    return "IAM_ROLE default"


def probe_copy_capability(bucket_uri: str) -> tuple[bool, str]:
    """Find out whether COPY can actually authenticate, without loading anything.

    Runs a COPY against a deliberately absent key. A missing-file error means the credentials
    were accepted and only the object was absent; a permission or role error means they were
    not. Cheap, and far more honest than assuming.
    """
    clause = copy_credentials_clause()
    probe_uri = f"{bucket_uri.rstrip('/')}/__copy_capability_probe__/does-not-exist.parquet"
    try:
        with cursor(autocommit=True, schema=False) as cur:
            cur.execute("CREATE TEMP TABLE copy_probe (x VARCHAR(1))")
            cur.execute(f"COPY copy_probe FROM '{probe_uri}' {clause} FORMAT AS PARQUET")
    except Exception as exc:
        message = str(exc)
        lowered = message.lower()
        if "cannot find default iam role" in lowered:
            return False, (
                "no default IAM role is attached to this Redshift cluster, so COPY cannot "
                "authenticate to S3. Attach one in the console and set "
                "AIMTERNET_REDSHIFT_COPY_IAM_ROLE, or let the loader use batched INSERT."
            )
        if "no such" in lowered or "not found" in lowered or "manifest" in lowered:
            return True, f"credentials accepted ({clause}); probe object absent as expected"
        if "iam_role" in lowered or "access denied" in lowered or "permission" in lowered:
            return False, f"{clause} rejected: {message[:300]}"
        return False, f"COPY unavailable: {message[:300]}"
    return True, f"credentials accepted ({clause})"
