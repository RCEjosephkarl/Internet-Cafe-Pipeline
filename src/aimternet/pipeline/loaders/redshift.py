"""Phase 6 — load Gold into Redshift (spec §6.5).

Two loading strategies, chosen at run time rather than assumed:

* **COPY from S3** when the cluster can authenticate to the bucket. Fastest, and what a
  production build would use.
* **Batched INSERT from the Gold Parquet** otherwise, read through DuckDB.

This cluster has no IAM role attached -- a COPY probe returns *"Cannot find default IAM role
on this cluster"* and the IAM user cannot call ``redshift:DescribeClusters`` to attach one --
so the INSERT path is what actually runs here. Inline access keys are prohibited (§6.5), so
that is the honest remaining option, and the run report says which path was taken rather than
implying COPY worked.

Every table loads through a staging table and a MERGE-equivalent, so rerunning adds zero
facts (acceptance item 14).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from aimternet.config.settings import settings
from aimternet.db import redshift
from aimternet.pipeline.curate.engine import duck, layer_uri

log = logging.getLogger(__name__)

DDL_PATH = Path(__file__).resolve().parents[2] / "db" / "redshift_ddl" / "schema.sql"

# Gold dataset -> (target table, primary key columns used to de-duplicate on merge)
TABLES: dict[str, tuple[str, tuple[str, ...]]] = {
    "dim_member": ("dim_member", ("member_key",)),
    "dim_workstation": ("dim_workstation", ("workstation_key",)),
    "dim_date": ("dim_date", ("date_id",)),
    "dim_time": ("dim_time", ("time_id",)),
    "dim_concession_item": ("dim_concession_item", ("item_key",)),
    "fact_rental": ("fact_rental", ("rental_id",)),
    "fact_concession_sale": ("fact_concession_sale", ("purchase_id",)),
    "fact_concession_line_item": ("fact_concession_line_item", ("order_item_id",)),
    "fact_points_activity": ("fact_points_activity", ("ledger_id",)),
    "fact_workstation_event": ("fact_workstation_event", ("event_id",)),
    "agg_workstation_utilization_hourly": (
        "agg_workstation_utilization_hourly",
        ("workstation_id", "utilization_date", "hour_utc"),
    ),
}

# Partition columns exist only in the Parquet layout; they are not warehouse columns.
_PARTITION_COLUMNS = {"rental_date", "purchase_date", "ledger_date", "event_date", "telemetry_date"}



@dataclass
class RedshiftLoadReport:
    strategy: str = ""
    rows: dict[str, int] = field(default_factory=dict)
    duration_seconds: float = 0.0
    notes: list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"Redshift load — strategy: {self.strategy}",
            "",
            f"  {'table':40s} {'rows':>12s}",
            f"  {'-' * 40} {'-' * 12}",
        ]
        for table in sorted(self.rows):
            lines.append(f"  {table:40s} {self.rows[table]:12,d}")
        lines.append(f"  {'-' * 40} {'-' * 12}")
        lines.append(f"  {'TOTAL':40s} {sum(self.rows.values()):12,d}")
        lines.append(f"  elapsed: {self.duration_seconds:.1f}s")
        for note in self.notes:
            lines.append(f"  note: {note}")
        return "\n".join(lines)


def split_statements(sql: str) -> list[str]:
    """Split a SQL script into statements.

    Comments are stripped first, because a prose semicolon inside a ``--`` comment would
    otherwise split a statement in half -- which it did, the first time this ran.
    """
    without_comments = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
    return [s.strip() for s in without_comments.split(";") if s.strip()]


def apply_ddl() -> list[str]:
    """Create the schema and every table. Idempotent -- all CREATE ... IF NOT EXISTS."""
    schema = redshift.ensure_schema()
    sql = DDL_PATH.read_text(encoding="utf-8").replace("${SCHEMA}", schema)
    statements = split_statements(sql)
    applied = []
    with redshift.connection(autocommit=True) as conn, conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
            first_line = statement.strip().splitlines()[0][:70]
            applied.append(first_line)
    return applied


def choose_strategy() -> tuple[str, str]:
    """Decide between COPY and INSERT by testing, not by assuming."""
    cfg = settings()
    available, detail = redshift.probe_copy_capability(f"s3://{cfg.require_bucket()}")
    return ("copy" if available else "insert"), detail


def _target_columns(table: str) -> list[str]:
    with redshift.cursor() as cur:
        cur.execute(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position""",
            (settings().redshift_schema, table),
        )
        return [row[0] for row in cur.fetchall()]


def _load_table_via_insert(dataset: str, table: str, keys: tuple[str, ...], run_id: str) -> int:
    """Stage the Gold Parquet, then merge it in one transaction.

    Staging plus a delete-then-insert inside a single transaction is the pattern Redshift
    documents in place of MERGE on older clusters, and it has the property that matters:
    running it twice leaves the same rows.
    """
    columns = _target_columns(table)
    if not columns:
        raise RuntimeError(f"{table} does not exist in Redshift; run the DDL first")

    with duck() as con:
        uri = f"{layer_uri('gold', dataset)}/**/*.parquet"
        available = {
            name
            for name, *_ in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{uri}')").fetchall()
        }
        selected = [c for c in columns if c in available and c not in _PARTITION_COLUMNS]
        missing = [c for c in columns if c not in available and c not in _PARTITION_COLUMNS]
        if missing:
            log.debug("%s: columns absent from Gold, left NULL: %s", table, missing)
        rows = con.execute(
            f"SELECT {', '.join(selected)} FROM read_parquet('{uri}')"
        ).fetchall()

    if not rows:
        return 0

    staging = f"stg_{table}"
    key_join = " AND ".join(f"t.{k} = s.{k}" for k in keys)

    # Multi-row VALUES rather than executemany: redshift_connector's executemany issues one
    # round trip per row, which turns 260,400 rows into 260,400 network hops. Batching keeps
    # the parameter count under the wire-protocol limit of 32,767 per statement.
    rows_per_statement = max(1, min(1_000, 30_000 // max(len(selected), 1)))
    column_list = ", ".join(selected)
    row_placeholder = "(" + ", ".join(["%s"] * len(selected)) + ")"

    with redshift.connection() as conn, conn.cursor() as cur:
        cur.execute(f"SET search_path TO {settings().redshift_schema}, public")
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
        cur.execute(f"CREATE TEMP TABLE {staging} (LIKE {table})")

        for start in range(0, len(rows), rows_per_statement):
            chunk = rows[start : start + rows_per_statement]
            values = ", ".join([row_placeholder] * len(chunk))
            flat: list[Any] = [value for row in chunk for value in row]
            cur.execute(f"INSERT INTO {staging} ({column_list}) VALUES {values}", flat)

        # Delete-then-insert in one transaction: the MERGE equivalent, and the reason a
        # second run adds nothing rather than duplicating every fact.
        delete_join = key_join.replace("t.", f"{table}.")
        cur.execute(f"DELETE FROM {table} USING {staging} s WHERE {delete_join}")
        cur.execute(
            f"INSERT INTO {table} ({column_list}) SELECT {column_list} FROM {staging}"
        )
        cur.execute(f"DROP TABLE IF EXISTS {staging}")

    return len(rows)


def _load_table_via_copy(dataset: str, table: str, keys: tuple[str, ...], run_id: str) -> int:
    """COPY the Gold Parquet through a staging table, then merge."""
    cfg = settings()
    clause = redshift.copy_credentials_clause()
    uri = f"s3://{cfg.require_bucket()}/{cfg.s3_gold_prefix}/{dataset}/"
    staging = f"stg_{table}"
    key_join = " AND ".join(f"{table}.{k} = s.{k}" for k in keys)

    with redshift.connection() as conn, conn.cursor() as cur:
        cur.execute(f"SET search_path TO {cfg.redshift_schema}, public")
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
        cur.execute(f"CREATE TEMP TABLE {staging} (LIKE {table})")
        cur.execute(f"COPY {staging} FROM '{uri}' {clause} FORMAT AS PARQUET")
        cur.execute(f"DELETE FROM {table} USING {staging} s WHERE {key_join}")
        cur.execute(f"INSERT INTO {table} SELECT * FROM {staging}")
        cur.execute(f"SELECT count(*) FROM {staging}")
        loaded = int(cur.fetchone()[0])
        cur.execute(f"DROP TABLE IF EXISTS {staging}")
    return loaded


def load_all(run_id: str = "", *, datasets: list[str] | None = None) -> RedshiftLoadReport:
    started = time.time()
    report = RedshiftLoadReport()
    run_id = run_id or f"redshift-{int(started)}"

    apply_ddl()
    strategy, detail = choose_strategy()
    report.strategy = strategy
    report.notes.append(detail)
    log.info("redshift load strategy: %s (%s)", strategy, detail)

    loader = _load_table_via_copy if strategy == "copy" else _load_table_via_insert
    for dataset, (table, keys) in TABLES.items():
        if datasets and dataset not in datasets:
            continue
        loaded = loader(dataset, table, keys, run_id)
        report.rows[table] = loaded
        log.info("redshift %-40s %8d row(s)", table, loaded)

    report.duration_seconds = time.time() - started
    return report


def table_counts() -> dict[str, int]:
    counts = {}
    with redshift.cursor() as cur:
        for _dataset, (table, _keys) in TABLES.items():
            cur.execute(f"SELECT count(*) FROM {table}")
            counts[table] = int(cur.fetchone()[0])
    return counts


def query(sql: str) -> list[dict[str, Any]]:
    return redshift.fetch_all(sql)
