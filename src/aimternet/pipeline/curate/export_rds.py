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
from datetime import datetime

import duckdb
import pandas as pd

from aimternet.db.session import connection, fetch_all
from aimternet.pipeline.curate.engine import (
    count_parquet,
    duck,
    layer_uri,
    merge_onto_snapshot,
    write_parquet,
)

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
    # Append-only, and the only two with no `updated_at` at all: a ledger entry and an order
    # line are facts about a moment, never restated. `created_at` is therefore both the insert
    # time and the last-change time, which is exactly what a watermark needs.
    #
    # These were absent for the POC's whole life, and nothing noticed, because the test that
    # checks every snapshot has a reader can only see snapshots that exist. A table that is
    # never exported has no snapshot to declare unread -- so the gap it leaves is invisible to
    # the check built to find exactly that gap. fact_points_activity and
    # fact_concession_line_item were Bronze-only as a result: a POS sale would have reached
    # the warehouse with no lines and no points.
    "concession_order_items": ("created_at", "order_item_id"),
    "member_points_ledger": ("created_at", "ledger_id"),
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
    """Delta over the previous snapshot for ``table``, keyed by ``key``.

    The merge itself lives in ``curate.engine``: the DynamoDB export needs exactly the same
    thing, and two implementations of "write a snapshot, not a delta" is how F7 got to happen
    a second time in a sibling module.
    """
    return merge_onto_snapshot(
        con, delta="export_frame", destination=_snapshot_uri(table), key=key
    )


def export(run_id: str, *, full: bool = False) -> ExportReport:
    """Copy operational tables into Silver as ``<table>_operational``.

    Whatever the mode, Silver ends up holding a complete snapshot: an incremental run merges
    its delta onto the previous one rather than replacing it.
    """
    report = ExportReport(incremental=not full)
    # The database's clock, not this process's. The watermark is compared against
    # `updated_at`/`created_at`, which Postgres stamps with its own `now()`; taking the
    # bound from the worker instead means any skew between the two silently skips rows
    # written inside it. For `rental_transactions` a later check-out re-touches `updated_at`
    # and the row heals, but `member_points_ledger` and `concession_order_items` are
    # append-only -- nothing ever updates them again, so a row skipped once is skipped
    # forever. One clock removes the question.
    now = fetch_all("SELECT now() AS now")[0]["now"]

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
