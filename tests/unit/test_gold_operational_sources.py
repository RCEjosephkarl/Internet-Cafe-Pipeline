"""The transactional facts must draw on both origins, not just Bronze.

`fact_rental` was built from `silver/rental_transactions` -- the Bronze-derived dataset --
and nothing else. Bronze is immutable after the bootstrap, so no rental rung up at the till
could ever appear in it. The rows reached RDS, were exported to Silver, and stopped: the
dashboard's "bootstrap batch only" caveat was the honest description of a real gap.

Same shape as `test_gold_event_source.py`, and for the same reason: these drive the real SQL
against local Parquet, so no S3, no RDS, and no AWS credentials.

The overlap is what makes these different from the event ones. RDS was loaded from the same
source files Bronze came from, so every bootstrap row is in *both* origins -- the union has
to drop 28,287 duplicate rentals on every real build, not zero.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from aimternet.pipeline.curate import gold

BRONZE_RENTAL = {
    "rental_id": "SESS-000001",
    "member_id": "M-1001",
    "workstation_id": "PC-001",
    "session_start_utc": "2026-07-01T09:00:00+00:00",
    "session_end_utc": "2026-07-01T11:00:00+00:00",
    "duration_hours": Decimal("2.00"),
    "base_hourly_rate": Decimal("50.00"),
    "member_tier_applied": "Standard",
    "tier_discount_pct": Decimal("0.0000"),
    "final_hourly_rate": Decimal("50.00"),
    "gross_rental_amount": Decimal("100.00"),
    "points_redeemed": 0,
    "points_credit_value": Decimal("0.00"),
    "net_amount_paid": Decimal("100.00"),
    "points_accrued": 10,
    "payment_method": "Cash",
    "source_file": "rental_transactions.csv",
}


def _pos_rental(**overrides) -> dict:
    """A POS rental as the RDS export actually leaves it.

    `source_file` is NULL -- these never touched a file -- and the money columns arrive at
    whatever width DuckDB inferred from the pandas frame, which is the point of several of
    these tests.
    """
    row = {
        **BRONZE_RENTAL,
        "rental_id": "SESS-20260903-0001-0001",
        "member_id": "M-1841",
        "workstation_id": "PC-002",
        "session_start_utc": "2026-09-03T12:00:00+00:00",
        "session_end_utc": "2026-09-03T13:00:00+00:00",
        "source_file": None,
    }
    row.update(overrides)
    return row


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def _local_layer(tmp_path, monkeypatch):
    """Point every layer_uri at tmp_path so nothing touches S3."""
    monkeypatch.setattr(gold, "layer_uri", lambda layer, dataset: str(tmp_path / dataset))


def _write(tmp_path: Path, con, dataset: str, rows: list[dict], cast: str = "") -> None:
    target = tmp_path / dataset
    target.mkdir(parents=True, exist_ok=True)
    con.register("seed", pd.DataFrame(rows))
    projection = cast or "*"
    path = target / "p.parquet"
    con.execute(f"COPY (SELECT {projection} FROM seed) TO '{path}' (FORMAT PARQUET)")
    con.unregister("seed")


#: The types `silver.py` actually produces. The Bronze branch of the union is unCAST -- it is
#: the reference the operational branch is cast *to* -- so a fixture that let pandas infer
#: BIGINT where Silver holds INTEGER would be testing the wrong reference and would report a
#: type drift that production does not have.
_SILVER_RENTAL_TYPES = """
    rental_id, member_id, workstation_id,
    CAST(session_start_utc AS TIMESTAMPTZ)     AS session_start_utc,
    CAST(session_end_utc AS TIMESTAMPTZ)       AS session_end_utc,
    CAST(duration_hours AS DECIMAL(6,2))       AS duration_hours,
    CAST(base_hourly_rate AS DECIMAL(12,2))    AS base_hourly_rate,
    member_tier_applied,
    CAST(tier_discount_pct AS DECIMAL(5,4))    AS tier_discount_pct,
    CAST(final_hourly_rate AS DECIMAL(12,2))   AS final_hourly_rate,
    CAST(gross_rental_amount AS DECIMAL(12,2)) AS gross_rental_amount,
    CAST(points_redeemed AS INTEGER)           AS points_redeemed,
    CAST(points_credit_value AS DECIMAL(12,2)) AS points_credit_value,
    CAST(net_amount_paid AS DECIMAL(12,2))     AS net_amount_paid,
    CAST(points_accrued AS INTEGER)            AS points_accrued,
    payment_method,
    CAST(source_file AS VARCHAR)               AS source_file
"""


#: The snapshot fixtures below hold a single API-written row, so `source_file` is all-NULL and
#: pandas would type it as a numeric. The real snapshots carry the bootstrap rows alongside,
#: which makes the column VARCHAR by accident; spelling it out here keeps the fixture from
#: depending on that accident -- and the CAST in gold.py is what makes it not matter either way.
_PURCHASE_SNAPSHOT_TYPES = """
    purchase_id, member_id, rental_id, total_amount, points_accrued, payment_method,
    purchased_at_utc, CAST(source_file AS VARCHAR) AS source_file
"""

_LEDGER_SNAPSHOT_TYPES = """
    ledger_id, member_id, source_reference_id, transaction_type, points_delta,
    resulting_balance, created_at_utc, CAST(source_file AS VARCHAR) AS source_file
"""


def _rentals(tmp_path, con, operational: list[dict] | None, cast: str = ""):
    _write(tmp_path, con, "rental_transactions", [BRONZE_RENTAL], _SILVER_RENTAL_TYPES)
    if operational is not None:
        _write(tmp_path, con, "rental_transactions_operational", operational, cast)
    sql, contributed = gold.operational_source(con, "rental_transactions")
    frame = con.execute(sql).fetchdf().sort_values("rental_id").reset_index(drop=True)
    return frame, contributed


def test_bronze_only_when_the_export_has_never_run(tmp_path, con) -> None:
    """A fresh bucket must build a warehouse, not fail for want of a snapshot."""
    frame, contributed = _rentals(tmp_path, con, operational=None)

    assert contributed == 0
    assert list(frame["rental_id"]) == ["SESS-000001"]


def test_pos_rentals_reach_gold(tmp_path, con) -> None:
    """The gap this closed: a rental taken at the till used to stop at Silver."""
    frame, contributed = _rentals(tmp_path, con, operational=[_pos_rental()])

    assert contributed == 1
    assert len(frame) == 2
    pos = frame[frame["rental_id"] == "SESS-20260903-0001-0001"].iloc[0]
    assert pos["member_id"] == "M-1841"
    # Lineage still says where it came from, even though there is no file to name.
    assert pos["source_file"] == "rds:api"


def test_the_bootstrap_overlap_is_counted_once_and_bronze_wins(tmp_path, con) -> None:
    """Every bootstrap row is in both origins; the union must drop one copy, and keep Bronze.

    The values differ deliberately. A test that only counted rows would still pass with the
    dedupe keyed the wrong way round, and Gold feeds a delete-then-insert merge, so which
    copy survives is not cosmetic.
    """
    frame, contributed = _rentals(
        tmp_path,
        con,
        operational=[_pos_rental(rental_id="SESS-000001", net_amount_paid=Decimal("999.00"))],
    )

    assert contributed == 0, "a row already in Bronze contributes nothing"
    assert len(frame) == 1
    assert frame.iloc[0]["net_amount_paid"] == Decimal("100.00")
    assert frame.iloc[0]["source_file"] == "rental_transactions.csv"


def test_an_open_rental_is_excluded_until_it_closes(tmp_path, con) -> None:
    """Someone still sitting at the machine has no price yet.

    The API prices a session at check-out, so an open rental's money columns are NULL by
    construction. Letting it in would put a NULL-revenue, NULL-payment-method row into every
    average and every payment-mix chart the warehouse feeds.
    """
    open_rental = _pos_rental(
        rental_id="SESS-OPEN",
        session_end_utc=None,
        gross_rental_amount=None,
        net_amount_paid=None,
        points_accrued=None,
        payment_method=None,
    )
    frame, contributed = _rentals(tmp_path, con, operational=[open_rental, _pos_rental()])

    assert contributed == 1, "only the closed POS rental counts"
    assert "SESS-OPEN" not in list(frame["rental_id"])
    assert len(frame) == 2


def test_the_closed_rental_arrives_on_the_next_build(tmp_path, con) -> None:
    """The exclusion is a deferral, not a drop."""
    frame, contributed = _rentals(
        tmp_path, con, operational=[_pos_rental(rental_id="SESS-OPEN")]
    )

    assert contributed == 1
    assert "SESS-OPEN" in list(frame["rental_id"])


def test_inferred_column_widths_do_not_leak_into_gold(tmp_path, con) -> None:
    """The snapshot's types are inferred per run; Gold's must not be.

    The export writes the snapshot from a pandas frame, so DuckDB picks a decimal width from
    whatever that frame happened to hold -- DECIMAL(5,2) was observed live, which caps at
    999.99 -- and one open rental's NULL points_accrued widens the whole column to float64.
    Inheriting either into the warehouse is how a money column silently changes type between
    two builds of the same data.
    """
    narrow = """
        rental_id, member_id, workstation_id, session_start_utc, session_end_utc,
        CAST(duration_hours AS DECIMAL(3,2))      AS duration_hours,
        CAST(base_hourly_rate AS DECIMAL(5,2))    AS base_hourly_rate,
        member_tier_applied, tier_discount_pct,
        CAST(final_hourly_rate AS DECIMAL(5,2))   AS final_hourly_rate,
        CAST(gross_rental_amount AS DECIMAL(5,2)) AS gross_rental_amount,
        CAST(points_redeemed AS BIGINT)           AS points_redeemed,
        CAST(points_credit_value AS DECIMAL(5,2)) AS points_credit_value,
        CAST(net_amount_paid AS DECIMAL(5,2))     AS net_amount_paid,
        CAST(points_accrued AS DOUBLE)            AS points_accrued,
        payment_method, source_file
    """
    _write(tmp_path, con, "rental_transactions", [BRONZE_RENTAL], _SILVER_RENTAL_TYPES)
    _write(tmp_path, con, "rental_transactions_operational", [_pos_rental()], narrow)
    sql, _ = gold.operational_source(con, "rental_transactions")

    described = con.execute(f"DESCRIBE SELECT * FROM ({sql})").fetchall()
    types = {name: kind for name, kind, *_ in described}
    assert types["gross_rental_amount"] == "DECIMAL(12,2)"
    assert types["net_amount_paid"] == "DECIMAL(12,2)"
    assert types["duration_hours"] == "DECIMAL(6,2)"
    assert types["points_accrued"] == "INTEGER", "a float must never reach a points column"
    assert types["points_redeemed"] == "INTEGER"

    # Checked in SQL, not through fetchdf(): pandas converts DECIMAL on the way out, so a
    # DataFrame would show float64 even for a column Parquet stores as decimal128(12,2).
    # What matters is the type DuckDB writes, which is what `typeof` reports.
    values = con.execute(
        f"SELECT typeof(net_amount_paid), net_amount_paid FROM ({sql}) ORDER BY rental_id"
    ).fetchall()
    assert {kind for kind, _ in values} == {"DECIMAL(12,2)"}
    assert all(isinstance(amount, Decimal) for _, amount in values)


def test_the_ledger_column_is_renamed_to_the_silver_name(tmp_path, con) -> None:
    """RDS keeps `resulting_balance`; Silver renamed it to record that it is not trusted (F5).

    Gold reads the Silver name, so the projection has to bridge the two or the union fails
    outright -- which is the good case. The bad case is someone "fixing" it by selecting the
    RDS name in both branches, quietly reintroducing the column F5 said not to compute with.
    """
    bronze = {
        "ledger_id": "LED-1", "member_id": "M-1001", "source_reference_id": "SESS-000001",
        "transaction_type": "RENTAL_ACCRUAL", "points_delta": 10,
        "resulting_balance_source": 10, "created_at_utc": "2026-07-01T11:00:00+00:00",
        "source_file": "member_points_ledger.csv",
    }
    pos = {
        "ledger_id": "LED-API-1", "member_id": "M-1841", "source_reference_id": "SESS-API",
        "transaction_type": "CONCESSION_ACCRUAL", "points_delta": 5,
        "resulting_balance": 5, "created_at_utc": "2026-09-03T12:00:00+00:00",
        "source_file": None,
    }
    _write(tmp_path, con, "member_points_ledger", [bronze])
    _write(tmp_path, con, "member_points_ledger_operational", [pos], _LEDGER_SNAPSHOT_TYPES)

    sql, contributed = gold.operational_source(con, "member_points_ledger")
    frame = con.execute(sql).fetchdf().sort_values("ledger_id").reset_index(drop=True)

    assert contributed == 1
    assert "resulting_balance" not in frame.columns
    assert list(frame["resulting_balance_source"]) == [10, 5]  # LED-1, then LED-API-1


def test_a_walk_in_purchase_survives_the_union(tmp_path, con) -> None:
    """A purchase with no rental is a walk-in, and `is_walk_in` keys off a NULL rental_id."""
    bronze = {
        "purchase_id": "PUR-1", "member_id": "M-1001", "rental_id": "SESS-000001",
        "total_amount": Decimal("50.00"), "points_accrued": 5, "payment_method": "Cash",
        "purchased_at_utc": "2026-07-01T10:00:00+00:00",
        "source_file": "concession_purchases.csv",
    }
    pos = {**bronze, "purchase_id": "PUR-API-1", "rental_id": None, "source_file": None}
    _write(tmp_path, con, "concession_purchases", [bronze])
    _write(tmp_path, con, "concession_purchases_operational", [pos], _PURCHASE_SNAPSHOT_TYPES)

    sql, contributed = gold.operational_source(con, "concession_purchases")
    frame = con.execute(sql).fetchdf().sort_values("purchase_id").reset_index(drop=True)

    assert contributed == 1
    walk_in = frame[frame["purchase_id"] == "PUR-API-1"].iloc[0]
    assert walk_in["rental_id"] is None


def test_an_empty_snapshot_contributes_nothing(tmp_path, con) -> None:
    """The export writes nothing at all before the first POS row, so the prefix may be bare."""
    target = tmp_path / "rental_transactions_operational"
    target.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""COPY (SELECT NULL::VARCHAR AS rental_id WHERE false)
            TO '{target / "p.parquet"}' (FORMAT PARQUET)"""
    )
    _write(tmp_path, con, "rental_transactions", [BRONZE_RENTAL], _SILVER_RENTAL_TYPES)

    sql, contributed = gold.operational_source(con, "rental_transactions")

    assert contributed == 0
    assert len(con.execute(sql).fetchdf()) == 1


@pytest.mark.parametrize("fact", sorted(gold._OPERATIONAL_FACTS))
def test_both_branches_project_the_same_columns(fact: str) -> None:
    """`UNION ALL BY NAME` matches on names, so the two branches must agree on them.

    Derived from one projection rather than written twice, which is the only way they stay
    equal; this pins that the derivation actually works for every fact.
    """
    _bronze, _snapshot, _key, projection, _predicate = gold._OPERATIONAL_FACTS[fact]
    names = gold._projected_names(projection)

    assert names[-1] == "source_file", "lineage must survive the union"
    assert len(names) == len(set(names)), f"{fact} projects a duplicate column name"
    assert all(name.isidentifier() for name in names), f"{fact} projection did not parse: {names}"


def test_reconcile_counts_the_same_rows_gold_writes() -> None:
    """Gold's eligibility rule and reconciliation's expectation must be the same rule.

    They were not, for one build: `fact_rental` excludes open rentals and the expectation
    counted them, so `gold_rows:fact_rental` failed by exactly the number of customers
    sitting at a machine. A check that cannot tell "someone is playing right now" from "the
    build dropped rows" is the frozen-literal failure again, just noisier.
    """
    from aimternet.pipeline import reconcile

    for fact, (bronze_dataset, _snapshot, _key) in reconcile.UNIONED_FACTS.items():
        assert reconcile._snapshot_predicate(fact) == gold.snapshot_predicate(bronze_dataset)

    assert gold.snapshot_predicate("rental_transactions") == "session_end_utc IS NOT NULL"
    # The event snapshot has no rule and no entry; it must not raise.
    assert gold.snapshot_predicate("workstation_events") == ""
