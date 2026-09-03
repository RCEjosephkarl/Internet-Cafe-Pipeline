"""Per-file load checkpoints, so an interrupted load resumes instead of restarting.

Keyed on ``(pipeline, source_file, checksum)``. Including the checksum means a file whose
contents changed is reloaded rather than skipped -- the checkpoint records "these exact bytes
were loaded", not "this path was seen".
"""

from __future__ import annotations

from aimternet.db.session import connection


def completed(pipeline: str) -> dict[str, str]:
    """source_file -> checksum for everything this pipeline has already finished."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT source_file, checksum FROM load_checkpoint WHERE pipeline = %s", (pipeline,)
        )
        return dict(cur.fetchall())


def mark(
    pipeline: str, source_file: str, checksum: str, records: int, run_id: str
) -> None:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO load_checkpoint (pipeline, source_file, checksum, records_loaded, run_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (pipeline, source_file, checksum) DO UPDATE SET
                records_loaded = EXCLUDED.records_loaded,
                completed_at   = now(),
                run_id         = EXCLUDED.run_id
            """,
            (pipeline, source_file, checksum, records, run_id),
        )


def total_records(pipeline: str) -> int:
    with connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(sum(records_loaded), 0) FROM load_checkpoint WHERE pipeline = %s",
            (pipeline,),
        )
        return int(cur.fetchone()[0])


def clear(pipeline: str) -> int:
    """Forget a pipeline's checkpoints so the next run reloads everything."""
    with connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM load_checkpoint WHERE pipeline = %s", (pipeline,))
        return int(cur.rowcount)
