"""RDS -> S3 export (spec §6.7 ``rds_to_s3_incremental``).

This is the hop that keeps the D2 resolution honest. The 840 backfilled members are created
once, by the RDS loader, under the configured policy. Rather than re-deriving them in SQL for
the warehouse -- which would be the same business rule implemented twice (§3) -- the resolved
operational state is exported here and Gold reads that.

Incremental by watermark on ``updated_at``, so the scheduled version only moves what changed
-- but what it *writes* is always a full snapshot. Gold reads ``<table>_operational`` as the
current state of the operational store, so an export that left a delta there would silently
shrink a dimension: the first incremental run after the bootstrap replaced
``members_operational`` with the 4 rows the API had touched, and ``dim_member`` went from
1,200 members to 8 versions without anything failing. The delta is now merged onto the
previous snapshot by primary key before it is written back.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

import duckdb
import pandas as pd

from aimternet.db.session import connection, fetch_all
from aimternet.pipeline.curate.engine import count_parquet, duck, layer_uri, write_parquet

log = logging.getLogger(__name__)

# Tables exported from the operational store: the column carrying their watermark, and the
# primary key the delta is merged on. Both are needed -- the watermark decides what to fetch,
# the key decides what it replaces.
EXPORTS: dict[str, tuple[str, str]] = {
    "members": ("updated_at", "member_id"),
    "workstations": ("updated_at", "workstation_id"),
    "concession_items": ("updated_at", "item_sku"),
    "rental_transactions": ("updated_at", "rental_id"),
    "concession_purchases": ("updated_at", "purchase_id"),
}


@dataclass
class ExportReport:
    rows: dict[str, int] = field(default_factory=dict)          # rows moved this run
    snapshot_rows: dict[str, int] = field(default_factory=dict)  # rows in Silver afterwards
    watermarks: dict[str, str] = field(default_factory=dict)
    incremental: bool = True

    def summary(self) -> str:
        mode = "incremental" if self.incremental else "full"
        lines = [f"RDS -> S3 export ({mode}):", f"  {'table':26s} {'moved':>10s} {'snapshot':>10s}"]
        for table in sorted(self.rows):
            lines.append(
                f"  {table:26s} {self.rows[table]:10,d} {self.snapshot_rows.get(table, 0):10,d}"
            )
        return "\n".join(lines)


def get_watermark(pipeline: str) -> datetime | None:
    rows = fetch_all(
        "SELECT watermark_value FROM pipeline_watermark WHERE pipeline_name = %s", (pipeline,)
    )
    return datetime.fromisoformat(rows[0]["watermark_value"]) if rows else None


def set_watermark(pipeline: str, value: datetime, run_id: str) -> None:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_watermark (pipeline_name, watermark_value, run_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (pipeline_name) DO UPDATE SET
                watermark_value = EXCLUDED.watermark_value,
                updated_at      = now(),
                run_id          = EXCLUDED.run_id
            """,
            (pipeline, value.isoformat(), run_id),
        )


def _snapshot_uri(table: str) -> str:
    return layer_uri("silver", f"{table}_operational")


def _merge_sql(table: str, key: str, con: duckdb.DuckDBPyConnection) -> str:
    """Delta over previous snapshot, keyed by ``key``.

    Returns a SELECT producing the *whole* table as it now stands. When no snapshot exists
    yet -- the first run, or a bucket that has never been written -- the delta is the whole
    table and there is nothing to merge onto.
    """
    pattern = f"{_snapshot_uri(table).rstrip('/')}/**/*.parquet"
    try:
        con.execute(f"SELECT 1 FROM read_parquet('{pattern}') LIMIT 1")
    except duckdb.IOException:
        return "SELECT * FROM export_frame"

    # Column order comes from the delta, so a snapshot written by an older schema cannot
    # scramble the columns on a UNION.
    return f"""
        SELECT * FROM export_frame
        UNION ALL BY NAME
        SELECT * FROM read_parquet('{pattern}') previous
        WHERE previous.{key} NOT IN (SELECT {key} FROM export_frame)
    """


def export(run_id: str, *, full: bool = False) -> ExportReport:
    """Copy operational tables into Silver as ``<table>_operational``.

    Whatever the mode, Silver ends up holding a complete snapshot: an incremental run merges
    its delta onto the previous one rather than replacing it.
    """
    report = ExportReport(incremental=not full)
    now = datetime.now(UTC)

    with duck() as con:
        for table, (watermark_column, key) in EXPORTS.items():
            pipeline = f"rds_to_s3:{table}"
            since = None if full else get_watermark(pipeline)
            where = f"WHERE {watermark_column} > %s" if since else ""
            params = (since,) if since else ()

            frame = pd.DataFrame(fetch_all(f"SELECT * FROM {table} {where}", params or None))
            report.rows[table] = len(frame)
            if frame.empty:
                # Nothing changed, so the snapshot already on S3 is still correct. Leaving it
                # untouched is both cheaper and safer than rewriting it identically.
                report.snapshot_rows[table] = count_parquet(con, _snapshot_uri(table))
                log.info("rds->s3 %s: nothing new since %s", table, since)
                continue

            con.register("export_frame", frame)
            select_sql = "SELECT * FROM export_frame" if full else _merge_sql(table, key, con)
            # Read the merge fully before overwriting the file it reads from.
            con.execute(f"CREATE OR REPLACE TEMP TABLE export_merged AS {select_sql}")
            write_parquet(con, "SELECT * FROM export_merged", _snapshot_uri(table))
            merged = con.execute("SELECT count(*) FROM export_merged").fetchone()
            report.snapshot_rows[table] = int(merged[0]) if merged else 0
            con.execute("DROP TABLE export_merged")
            con.unregister("export_frame")

            set_watermark(pipeline, now, run_id)
            report.watermarks[table] = now.isoformat()
            log.info(
                "rds->s3 %s: %d row(s) moved, snapshot now %d row(s)",
                table, len(frame), report.snapshot_rows[table],
            )

    return report
