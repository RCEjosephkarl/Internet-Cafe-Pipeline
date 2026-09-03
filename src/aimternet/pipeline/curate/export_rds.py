"""RDS -> S3 export (spec §6.7 ``rds_to_s3_incremental``).

This is the hop that keeps the D2 resolution honest. The 840 backfilled members are created
once, by the RDS loader, under the configured policy. Rather than re-deriving them in SQL for
the warehouse -- which would be the same business rule implemented twice (§3) -- the resolved
operational state is exported here and Gold reads that.

Incremental by watermark on ``updated_at``, so the scheduled version only moves what changed.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pandas as pd

from aimternet.db.session import connection, fetch_all
from aimternet.pipeline.curate.engine import duck, layer_uri, write_parquet

log = logging.getLogger(__name__)

# Tables exported from the operational store, with the column that carries their watermark.
EXPORTS: dict[str, str] = {
    "members": "updated_at",
    "workstations": "updated_at",
    "concession_items": "updated_at",
    "rental_transactions": "updated_at",
    "concession_purchases": "updated_at",
}


@dataclass
class ExportReport:
    rows: dict[str, int] = field(default_factory=dict)
    watermarks: dict[str, str] = field(default_factory=dict)
    incremental: bool = True

    def summary(self) -> str:
        mode = "incremental" if self.incremental else "full"
        lines = [f"RDS -> S3 export ({mode}):"]
        for table in sorted(self.rows):
            lines.append(f"  {table:26s} {self.rows[table]:10,d} rows")
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


def export(run_id: str, *, full: bool = False) -> ExportReport:
    """Copy operational tables into Silver as ``<table>_operational``."""
    report = ExportReport(incremental=not full)
    now = datetime.now(UTC)

    with duck() as con:
        for table, watermark_column in EXPORTS.items():
            pipeline = f"rds_to_s3:{table}"
            since = None if full else get_watermark(pipeline)
            where = f"WHERE {watermark_column} > %s" if since else ""
            params = (since,) if since else ()

            frame = pd.DataFrame(fetch_all(f"SELECT * FROM {table} {where}", params or None))
            report.rows[table] = len(frame)
            if frame.empty:
                log.info("rds->s3 %s: nothing new since %s", table, since)
                continue

            con.register("export_frame", frame)
            write_parquet(
                con,
                "SELECT * FROM export_frame",
                layer_uri("silver", f"{table}_operational"),
            )
            con.unregister("export_frame")
            set_watermark(pipeline, now, run_id)
            report.watermarks[table] = now.isoformat()
            log.info("rds->s3 %s: %d row(s) exported", table, len(frame))

    return report
