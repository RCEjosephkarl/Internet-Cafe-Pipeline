"""PostgreSQL connections.

Two roles, deliberately: the pipeline and the API connect read-write; ``notebooks/db_lens``
connects through a role that cannot write. The read-only session is additionally pinned with
``default_transaction_read_only``, so an exploratory query in a notebook cannot mutate
operational data even by accident.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from aimternet.config.settings import settings


@contextmanager
def connection(*, read_only: bool = False, autocommit: bool = False) -> Iterator[Any]:
    """A configured psycopg2 connection with ``search_path`` already set.

    Commits on clean exit, rolls back on exception. The caller never has to remember which.
    """
    cfg = settings()
    conn = psycopg2.connect(cfg.postgres_dsn(read_only=read_only))
    try:
        conn.autocommit = autocommit
        with conn.cursor() as cur:
            cur.execute(f'SET search_path TO "{cfg.pg_schema}", public')
            if read_only:
                cur.execute("SET default_transaction_read_only = on")
        if not autocommit:
            conn.commit()
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


@contextmanager
def cursor(*, read_only: bool = False, dict_rows: bool = False) -> Iterator[Any]:
    """A cursor inside a managed transaction."""
    factory = psycopg2.extras.RealDictCursor if dict_rows else None
    with connection(read_only=read_only) as conn, conn.cursor(cursor_factory=factory) as cur:
        yield cur


def fetch_all(sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    """Convenience read used by the metrics API and the lens notebooks."""
    with cursor(read_only=True, dict_rows=True) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]
