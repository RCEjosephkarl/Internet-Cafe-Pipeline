"""Migrations against the real RDS instance. Marked `rds`; excluded from `make test`."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.rds


def test_migrations_apply_rollback_and_reapply() -> None:
    from aimternet.db.migrate import downgrade, status, upgrade

    upgrade()
    assert all(row["applied"] for row in status())

    reverted = downgrade(steps=2)
    assert len(reverted) == 2

    reapplied = upgrade()
    assert len(reapplied) == 2
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
