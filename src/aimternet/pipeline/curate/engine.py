"""DuckDB session used by the Silver and Gold transformations.

DuckDB rather than pyarrow, for three reasons documented in CLAUDE.md: pyarrow SIGABRTs at
interpreter shutdown on this host, DuckDB writes true ``decimal128`` Parquet where fastparquet
silently downcasts money to float64, and it reads Bronze straight from S3 so nothing is staged
on a disk that already holds 4.4 GB of source data (D3).

Transformations are SQL. That is deliberate: the work is projection, typing and aggregation,
and SQL says that more clearly than a DataFrame pipeline would -- and it streams, so a 6.3M
row telemetry pass never materialises.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import duckdb

from aimternet.config.settings import settings

log = logging.getLogger(__name__)

# Lineage carried from Bronze all the way to Gold (spec §6.1 Stage D).
LINEAGE_COLUMNS = """
    regexp_replace(filename, '^.*/', '')          AS source_file,
    CAST(? AS VARCHAR)                            AS run_id,
    now()                                         AS ingested_at_utc
"""


@contextmanager
def duck(*, memory_limit: str = "4GB", threads: int = 4) -> Iterator[duckdb.DuckDBPyConnection]:
    """A DuckDB connection wired for S3, with a memory cap so it spills rather than dies."""
    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit='{memory_limit}'")
        con.execute(f"SET threads={threads}")
        con.execute("SET preserve_insertion_order=false")  # lets big writes stream
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        # Credentials come from the standard chain: instance role first, then environment.
        # No key ever appears in this repo (spec §8).
        con.execute("CREATE OR REPLACE SECRET aimternet_s3 (TYPE s3, PROVIDER credential_chain)")
        yield con
    finally:
        con.close()


def bronze_uri(dataset: str, pattern: str = "*/*.csv") -> str:
    cfg = settings()
    return f"s3://{cfg.require_bucket()}/{cfg.s3_bronze_prefix}/{dataset}/{pattern}"


def layer_uri(layer: str, dataset: str) -> str:
    cfg = settings()
    prefix = {"silver": cfg.s3_silver_prefix, "gold": cfg.s3_gold_prefix}[layer]
    return f"s3://{cfg.require_bucket()}/{prefix}/{dataset}"


def write_parquet(
    con: duckdb.DuckDBPyConnection,
    select_sql: str,
    destination: str,
    *,
    partition_by: tuple[str, ...] = (),
    params: list[Any] | None = None,
) -> None:
    """Write a query straight to Parquet + Snappy, partitioned if asked.

    ``OVERWRITE_OR_IGNORE`` makes a rerun replace the partition rather than append to it,
    which is what keeps the curate step idempotent.
    """
    options = ["FORMAT PARQUET", "COMPRESSION SNAPPY"]
    if partition_by:
        options.append(f"PARTITION_BY ({', '.join(partition_by)})")
        options.append("OVERWRITE_OR_IGNORE")
        target = destination
    else:
        # Without PARTITION_BY, DuckDB writes a single object at exactly this path. Naming
        # the file keeps every dataset a *directory*, so one glob reads any of them and
        # Redshift/consumers do not need to know which datasets happen to be partitioned.
        target = f"{destination.rstrip('/')}/part-0.parquet"
    con.execute(f"COPY ({select_sql}) TO '{target}' ({', '.join(options)})", params or [])


def count_parquet(con: duckdb.DuckDBPyConnection, uri: str) -> int:
    """Row count for a written dataset, used by reconciliation."""
    pattern = f"{uri.rstrip('/')}/**/*.parquet"
    try:
        row = con.execute(f"SELECT count(*) FROM read_parquet('{pattern}')").fetchone()
    except duckdb.IOException:
        return 0
    return int(row[0]) if row else 0
