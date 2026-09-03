"""Redshift warehouse tests. Marked `redshift`; excluded from `make test`.

Acceptance item 8: "Redshift is queryable and returns sensible revenue and utilization
numbers." These check both halves — the counts reconcile to Gold, and the numbers a business
would actually ask for come back correct.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

pytestmark = pytest.mark.redshift

EXPECTED = {
    "dim_member": 2_235,            # SCD2: versions, not members
    "dim_workstation": 175,
    "dim_date": 365,
    "dim_time": 1_440,
    "dim_concession_item": 10,
    "fact_rental": 28_287,
    "fact_concession_sale": 21_077,
    "fact_concession_line_item": 29_672,
    "fact_points_activity": 55_514,
    "fact_workstation_event": 58_218,
    "agg_workstation_utilization_hourly": 175 * 24 * 62,
}


@pytest.fixture(scope="module")
def schema() -> str:
    from aimternet.config.settings import settings

    return settings().redshift_schema


def test_every_table_matches_gold() -> None:
    from aimternet.pipeline.loaders.redshift import table_counts

    assert table_counts() == EXPECTED


def test_no_money_column_is_floating_point(schema: str) -> None:
    """Spec §5: no float columns for money in any schema — the warehouse included."""
    from aimternet.db import redshift

    rows = redshift.fetch_all(
        f"""SELECT table_name, column_name, data_type FROM information_schema.columns
            WHERE table_schema = '{schema}' AND data_type IN ('double precision', 'real')"""
    )
    assert rows == []


def test_revenue_by_zone_is_sensible(schema: str) -> None:
    from aimternet.db import redshift

    rows = redshift.fetch_all(
        f"""SELECT zone_classification, count(*) AS rentals, sum(net_amount_paid) AS revenue
            FROM {schema}.fact_rental GROUP BY 1"""
    )
    assert len(rows) == 3
    by_zone = {r["zone_classification"]: r for r in rows}
    assert set(by_zone) == {"Standard Zone", "VIP Esports Zone", "Streamer Pods"}
    assert sum(r["rentals"] for r in rows) == 28_287
    assert all(Decimal(r["revenue"]) > 0 for r in rows)
    # Standard Zone has 100 of the 175 workstations, so it should host the most rentals.
    assert max(rows, key=lambda r: r["rentals"])["zone_classification"] == "Standard Zone"


def test_utilisation_is_a_percentage_and_peaks_in_the_evening(schema: str) -> None:
    """Utilisation must stay in [0, 100] despite the F6 cadence change, and an internet
    cafe should be busiest in the evening, not at dawn."""
    from aimternet.db import redshift

    rows = redshift.fetch_all(
        f"""SELECT hour_utc, CAST(avg(utilization_pct) AS DECIMAL(5,2)) AS avg_util
            FROM {schema}.agg_workstation_utilization_hourly GROUP BY 1 ORDER BY 1"""
    )
    assert len(rows) == 24
    assert all(0 <= Decimal(r["avg_util"]) <= 100 for r in rows)

    busiest_utc = max(rows, key=lambda r: Decimal(r["avg_util"]))["hour_utc"]
    busiest_manila = (int(busiest_utc) + 8) % 24
    assert 17 <= busiest_manila <= 23, f"peak at {busiest_manila}:00 Manila looks wrong"


def test_scd2_dimension_joins_to_facts(schema: str) -> None:
    from aimternet.db import redshift

    rows = redshift.fetch_all(
        f"""SELECT count(*) AS n FROM {schema}.fact_rental f
            JOIN {schema}.dim_member d ON d.member_id = f.member_id AND d.is_current"""
    )
    assert rows[0]["n"] == 28_287, "every rental must join exactly one current member version"


def test_the_d2_cohort_is_visible_in_the_warehouse(schema: str) -> None:
    from aimternet.db import redshift

    rows = redshift.fetch_all(
        f"""SELECT count(DISTINCT member_id) AS n FROM {schema}.dim_member WHERE is_backfilled"""
    )
    assert rows[0]["n"] == 840


def test_concession_margin_is_positive_in_every_category(schema: str) -> None:
    from aimternet.db import redshift

    rows = redshift.fetch_all(
        f"""SELECT i.category, sum(l.line_margin) AS margin
            FROM {schema}.fact_concession_line_item l
            JOIN {schema}.dim_concession_item i USING (item_sku)
            GROUP BY 1"""
    )
    assert len(rows) == 4
    assert all(Decimal(r["margin"]) > 0 for r in rows)


def test_neighbouring_schemas_are_untouched(schema: str) -> None:
    """This cluster is shared. The build must stay inside its own schema."""
    from aimternet.db import redshift

    rows = redshift.fetch_all(
        """SELECT count(*) AS n FROM pg_tables WHERE schemaname IN
           ('bus_ticketing', 'krusty_krab_olap')"""
    )
    assert rows[0]["n"] > 0, "unrelated coursework schemas went missing"


@pytest.mark.slow
def test_a_second_load_adds_no_facts() -> None:
    """Acceptance item 14, for the Redshift hop. Staging plus delete-then-insert means a
    rerun replaces rows rather than appending them."""
    from aimternet.pipeline.loaders.redshift import load_all, table_counts

    before = table_counts()
    load_all(run_id="pytest-idempotency", datasets=["dim_workstation", "fact_concession_sale"])
    after = table_counts()
    assert after == before
