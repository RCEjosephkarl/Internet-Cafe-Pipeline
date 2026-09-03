"""Silver and Gold against the real S3 layers. Marked `aws`; excluded from `make test`."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.aws

SILVER_EXPECTED = {
    "workstations": 175,
    "concession_items": 10,
    "dim_date": 365,
    "dim_time": 1_440,
    "members": 360,
    "rental_transactions": 28_287,
    "concession_purchases": 21_077,
    "concession_order_items": 29_672,
    "member_points_ledger": 55_514,
    "workstation_events": 58_218,
    "telemetry": 6_300_000,
}

GOLD_EXPECTED = {
    "dim_workstation": 175,
    "dim_date": 365,
    "dim_time": 1_440,
    "dim_concession_item": 10,
    "fact_rental": 28_287,
    "fact_concession_sale": 21_077,
    "fact_concession_line_item": 29_672,
    "fact_points_activity": 55_514,
    # fact_workstation_event is derived, not frozen -- see _events_expected().
    # 175 workstations x 24 hours x 62 days
    "agg_workstation_utilization_hourly": 260_400,
}


def _events_expected() -> int:
    """Bronze events plus whatever the POS has emitted since.

    Not a literal. fact_workstation_event is the one fact with two sources -- the 58,218
    Bronze events and the API-emitted ones the DynamoDB export leaves in
    `workstation_events_operational` -- so a constant here fails on any bucket where the cafe
    has been open. Pinning it to the Bronze count is also what let those events go missing:
    the number that "proved" Gold was correct was the number that could not see them.
    """
    from aimternet.pipeline.curate.engine import count_parquet, duck, layer_uri

    with duck() as connection:
        operational = count_parquet(
            connection, layer_uri("silver", "workstation_events_operational")
        )
    return 58_218 + operational


@pytest.fixture(scope="module")
def con():
    from aimternet.pipeline.curate.engine import duck

    with duck() as connection:
        yield connection


@pytest.mark.parametrize("dataset", sorted(SILVER_EXPECTED))
def test_silver_reconciles_to_source(con, dataset: str) -> None:
    from aimternet.pipeline.curate.engine import count_parquet, layer_uri

    assert count_parquet(con, layer_uri("silver", dataset)) == SILVER_EXPECTED[dataset]


@pytest.mark.parametrize("dataset", sorted(GOLD_EXPECTED))
def test_gold_reconciles_to_silver(con, dataset: str) -> None:
    from aimternet.pipeline.curate.engine import count_parquet, layer_uri

    assert count_parquet(con, layer_uri("gold", dataset)) == GOLD_EXPECTED[dataset]


def test_the_api_emitted_events_reach_gold(con) -> None:
    """Every event in the operational snapshot must be in fact_workstation_event.

    The export wrote its delta over that snapshot hourly and Gold read Bronze alone, so POS
    sessions stopped at Silver. Asserted against the live layers, not a constant.
    """
    from aimternet.pipeline.curate.engine import count_parquet, layer_uri

    assert count_parquet(con, layer_uri("gold", "fact_workstation_event")) == _events_expected()


def test_no_float_columns_survive_into_silver_or_gold(con) -> None:
    """Spec §5: no float columns for money in any schema, including Parquet."""
    from aimternet.pipeline.curate.engine import layer_uri

    offenders = []
    for layer, datasets in (("silver", SILVER_EXPECTED), ("gold", GOLD_EXPECTED)):
        for dataset in datasets:
            uri = f"{layer_uri(layer, dataset)}/**/*.parquet"
            for name, dtype, *_ in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{uri}')"
            ).fetchall():
                if dtype in {"FLOAT", "DOUBLE"}:
                    offenders.append(f"{layer}.{dataset}.{name}")
    assert offenders == []


def test_money_survives_as_exact_decimal(con) -> None:
    from decimal import Decimal

    from aimternet.pipeline.curate.engine import layer_uri

    row = con.execute(
        f"""SELECT gross_rental_amount, points_credit_value, net_amount_paid
            FROM read_parquet('{layer_uri('gold', 'fact_rental')}/**/*.parquet')
            WHERE points_redeemed = 100 LIMIT 1"""
    ).fetchone()
    assert all(isinstance(v, Decimal) for v in row)
    assert row[0] - row[1] == row[2], "gross - credit must equal net, exactly"


def test_dim_member_scd2_has_one_current_row_per_member(con) -> None:
    from aimternet.pipeline.curate.engine import layer_uri

    uri = f"{layer_uri('gold', 'dim_member')}/**/*.parquet"
    members, current = con.execute(
        f"""SELECT count(DISTINCT member_id),
                   count(*) FILTER (WHERE is_current) FROM read_parquet('{uri}')"""
    ).fetchone()
    assert members == 1200, "360 from source plus the 840 D2 backfills"
    assert current == members, "SCD2 must expose exactly one current version per member"


def test_dim_member_versions_do_not_overlap(con) -> None:
    from aimternet.pipeline.curate.engine import layer_uri

    uri = f"{layer_uri('gold', 'dim_member')}/**/*.parquet"
    overlaps = con.execute(
        f"""SELECT count(*) FROM (
              SELECT valid_to_utc,
                     lead(valid_from_utc) OVER (PARTITION BY member_id ORDER BY valid_from_utc) nxt
              FROM read_parquet('{uri}')
            ) WHERE nxt IS NOT NULL AND valid_to_utc IS DISTINCT FROM nxt"""
    ).fetchone()[0]
    assert overlaps == 0


def test_the_d2_cohort_reaches_gold_still_flagged(con) -> None:
    from aimternet.pipeline.curate.engine import layer_uri

    uri = f"{layer_uri('gold', 'dim_member')}/**/*.parquet"
    backfilled = con.execute(
        f"SELECT count(DISTINCT member_id) FROM read_parquet('{uri}') WHERE is_backfilled"
    ).fetchone()[0]
    assert backfilled == 840


def test_utilisation_is_a_ratio_so_the_cadence_change_does_not_distort_it(con) -> None:
    """F6: 12 readings/hour for 55 days, 120/hour for the last 7. A ratio absorbs that."""
    from aimternet.pipeline.curate.engine import layer_uri

    uri = f"{layer_uri('gold', 'agg_workstation_utilization_hourly')}/**/*.parquet"
    cadences = dict(
        con.execute(f"SELECT readings, count(*) FROM read_parquet('{uri}') GROUP BY 1").fetchall()
    )
    assert set(cadences) == {12, 120}
    assert cadences[12] == 175 * 24 * 55
    assert cadences[120] == 175 * 24 * 7

    lo, hi = con.execute(
        f"SELECT min(utilization_pct), max(utilization_pct) FROM read_parquet('{uri}')"
    ).fetchone()
    assert 0 <= lo <= hi <= 100


def test_raw_telemetry_never_reaches_gold(con) -> None:
    """§6.5: aggregate to hourly in Gold; the 6.3M raw grain stays in Silver."""
    from aimternet.pipeline.curate.engine import count_parquet, layer_uri

    assert count_parquet(con, layer_uri("gold", "telemetry")) == 0
