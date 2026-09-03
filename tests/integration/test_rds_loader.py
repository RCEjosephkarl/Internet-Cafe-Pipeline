"""RDS loader against the real database. Marked `rds`; excluded from `make test`."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.rds

EXPECTED = {
    "workstations": 175,
    "concession_items": 10,
    "members": 1200,          # 360 from source + 840 backfilled under D2
    "rental_transactions": 28_287,
    "concession_purchases": 21_077,
    "concession_order_items": 29_672,
    "member_points_ledger": 55_514,
}


def test_row_counts_match_the_source() -> None:
    from aimternet.pipeline.loaders.rds import table_counts

    assert table_counts() == EXPECTED


def test_the_d2_cohort_is_present_flagged_and_exactly_840() -> None:
    """§1.3: "Silently making 840 rows appear is a failure." They are marked and countable."""
    from aimternet.db.session import fetch_all

    rows = fetch_all(
        """SELECT source_system, count(*) AS n, min(member_id) AS lo, max(member_id) AS hi
           FROM members WHERE is_backfilled GROUP BY 1"""
    )
    assert len(rows) == 1
    assert rows[0]["source_system"] == "DERIVED_FROM_TRANSACTIONS"
    assert rows[0]["n"] == 840
    assert (rows[0]["lo"], rows[0]["hi"]) == ("M-1001", "M-1840")


def test_no_orphan_foreign_keys_remain() -> None:
    from aimternet.db.session import fetch_all

    checks = {
        "rentals": "SELECT count(*) AS n FROM rental_transactions r "
                   "LEFT JOIN members m USING (member_id) WHERE m.member_id IS NULL",
        "purchases": "SELECT count(*) AS n FROM concession_purchases p "
                     "LEFT JOIN members m USING (member_id) WHERE m.member_id IS NULL",
        "order_items": "SELECT count(*) AS n FROM concession_order_items i "
                       "LEFT JOIN concession_purchases p USING (purchase_id) "
                       "WHERE p.purchase_id IS NULL",
        "ledger": "SELECT count(*) AS n FROM member_points_ledger l "
                  "LEFT JOIN members m USING (member_id) WHERE m.member_id IS NULL",
    }
    for name, sql in checks.items():
        assert fetch_all(sql)[0]["n"] == 0, f"{name} has orphan references"


def test_walk_in_purchases_have_a_null_rental_not_an_empty_string() -> None:
    from aimternet.db.session import fetch_all

    rows = fetch_all("SELECT count(*) AS n FROM concession_purchases WHERE rental_id IS NULL")
    assert rows[0]["n"] == 5293
    empty = fetch_all("SELECT count(*) AS n FROM concession_purchases WHERE rental_id = ''")
    assert empty[0]["n"] == 0


def test_money_columns_are_numeric_not_floating_point() -> None:
    from aimternet.config.settings import settings
    from aimternet.db.session import fetch_all

    rows = fetch_all(
        """SELECT table_name, column_name, data_type
           FROM information_schema.columns
           WHERE table_schema = %s
             AND table_name = ANY(%s)
             AND (column_name LIKE '%%amount%%' OR column_name LIKE '%%price%%'
                  OR column_name LIKE '%%rate%%' OR column_name LIKE '%%_value%%')""",
        (settings().pg_schema, list(EXPECTED)),
    )
    assert rows, "expected money columns to exist"
    for row in rows:
        assert row["data_type"] == "numeric", f"{row['table_name']}.{row['column_name']}"


def test_reloading_creates_no_duplicates() -> None:
    """Acceptance item 14, for the RDS hop."""
    from aimternet.pipeline.loaders.rds import load_all, table_counts

    before = table_counts()
    load_all(run_id="pytest-idempotency")
    assert table_counts() == before
