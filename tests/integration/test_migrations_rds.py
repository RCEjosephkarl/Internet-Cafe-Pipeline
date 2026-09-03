"""Migrations against the real RDS instance. Marked `rds`; excluded from `make test`."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.rds


def test_migrations_apply_rollback_and_reapply() -> None:
    """Exercise the full up/down/up cycle in a scratch schema, never the live one.

    An earlier version of this test ran against the configured schema. It rolled back
    load_checkpoint while a DynamoDB load was running in another process, and the loader --
    which had already written millions of items - lost every checkpoint it tried to record.
    A rollback test is destructive by definition, so it gets its own namespace and drops it
    afterwards.
    """
    import uuid

    from aimternet.db.migrate import applied_versions, discover_migrations, downgrade, upgrade
    from aimternet.db.session import connection

    scratch = f"aimternet_migrate_test_{uuid.uuid4().hex[:8]}"
    total = len(discover_migrations())
    try:
        applied = upgrade(schema=scratch)
        assert len(applied) == total
        assert len(applied_versions(scratch)) == total

        reverted = downgrade(steps=2, schema=scratch)
        assert len(reverted) == 2
        assert len(applied_versions(scratch)) == total - 2

        reapplied = upgrade(schema=scratch)
        assert len(reapplied) == 2
        assert len(applied_versions(scratch)) == total
    finally:
        with connection() as conn, conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{scratch}" CASCADE')


def test_the_live_schema_is_fully_migrated() -> None:
    """Non-destructive counterpart: the configured schema is up to date."""
    from aimternet.db.migrate import status

    assert all(row["applied"] for row in status())


def test_read_only_role_cannot_write() -> None:
    from aimternet.db.session import cursor

    with cursor(read_only=True) as cur:
        cur.execute("SELECT current_setting('default_transaction_read_only')")
        assert cur.fetchone()[0] == "on"

    with pytest.raises(Exception, match="read-only"), cursor(read_only=True) as cur:
        cur.execute("CREATE TABLE must_not_exist (x int)")


def test_neighbouring_schemas_are_untouched() -> None:
    """This RDS instance is shared. The build must stay inside its own schema."""
    from aimternet.db.session import cursor

    with cursor() as cur:
        cur.execute(
            """SELECT count(*) FROM information_schema.tables
               WHERE table_schema = 'simple_oltp'"""
        )
        assert cur.fetchone()[0] > 0, "unrelated coursework schema went missing"
