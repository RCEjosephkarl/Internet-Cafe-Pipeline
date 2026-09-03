"""Migration hygiene that does not need a database.

The expensive integration checks (apply, roll back, re-apply against real RDS) live in
tests/integration and carry the `rds` marker.
"""

from __future__ import annotations

import re

import pytest

from aimternet.db.migrate import MigrationError, discover_migrations


def test_every_migration_has_a_rollback() -> None:
    """discover_migrations raises if a .down.sql is missing; this asserts the outcome."""
    for migration in discover_migrations():
        assert migration.down_path.is_file(), f"{migration.version} cannot be rolled back"


def test_versions_are_unique_and_ordered() -> None:
    versions = [m.version for m in discover_migrations()]
    assert versions == sorted(versions)
    assert len(versions) == len(set(versions))


def test_migrations_never_hardcode_the_schema_or_a_secret() -> None:
    """Spec §8: no hardcoded namespaces, hostnames or passwords anywhere."""
    forbidden = re.compile(
        r"aimternet_oltp|aimternet_ro\b|rds\.amazonaws\.com|PASSWORD\s+'", re.IGNORECASE
    )
    for migration in discover_migrations():
        for path in (migration.up_path, migration.down_path):
            text = path.read_text()
            # ${...} placeholders are how the schema and role get in; literals are not.
            stripped = re.sub(r"\$\{[A-Z_]+\}", "", text)
            found = forbidden.search(stripped)
            assert found is None, f"{path.name} hardcodes {found.group(0)!r}"


def test_substitution_covers_every_placeholder() -> None:
    """A placeholder with no substitution would reach PostgreSQL as literal '${X}'."""
    from aimternet.db.migrate import _substitutions

    known = set(_substitutions("test_schema"))
    for migration in discover_migrations():
        for path in (migration.up_path, migration.down_path):
            used = set(re.findall(r"\$\{([A-Z_]+)\}", path.read_text()))
            unknown = used - known
            assert not unknown, f"{path.name} uses unsupported placeholders: {unknown}"


def _strip_sql_comments(sql: str) -> str:
    """Drop `--` comments so prose about floats is not mistaken for a float column."""
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def test_open_rentals_are_guarded_by_partial_unique_indexes() -> None:
    """Spec §7.1: double check-in must be impossible at the storage layer, not just in code."""
    ddl = next(m for m in discover_migrations() if m.name == "initial_schema").up_path.read_text()
    for index in ("one_open_rental_per_workstation", "one_open_rental_per_member"):
        statement = re.search(
            rf"CREATE\s+UNIQUE\s+INDEX\s+{index}\b(.*?);", ddl, re.IGNORECASE | re.DOTALL
        )
        assert statement is not None, f"{index} must be a CREATE UNIQUE INDEX"
        assert "WHERE session_end_utc IS NULL" in statement.group(1), f"{index} must be partial"


def test_money_columns_are_never_floating_point() -> None:
    """Spec §5: no float columns for money in any schema."""
    for migration in discover_migrations():
        ddl = _strip_sql_comments(migration.up_path.read_text()).upper()
        for banned in (r"\bFLOAT\b", r"\bDOUBLE PRECISION\b", r"\bREAL\b"):
            found = re.search(banned, ddl)
            assert found is None, f"{migration.name} declares a {found.group(0)} column"


def test_bad_filename_is_rejected(tmp_path) -> None:
    (tmp_path / "not_a_migration.sql").write_text("SELECT 1")
    with pytest.raises(MigrationError, match="NNNN_name"):
        discover_migrations(tmp_path)


def test_missing_down_file_is_rejected(tmp_path) -> None:
    (tmp_path / "0001_thing.up.sql").write_text("SELECT 1")
    with pytest.raises(MigrationError, match=r"no \.down\.sql"):
        discover_migrations(tmp_path)
