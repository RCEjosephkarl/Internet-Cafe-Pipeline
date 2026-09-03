"""Read-only query helpers behind ``notebooks/db_lens.ipynb``.

Everything here is SELECT. The OLTP side connects through the ``aimternet_ro`` role inside a
``READ ONLY`` transaction, so the notebook is safe by construction rather than by convention.

The canned queries are the ones worth having at hand when something looks wrong: what is on
the floor right now, where the money came from, which members are backfilled stubs.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from aimternet.config.settings import settings
from aimternet.db import redshift, session


def oltp(sql: str, params: tuple[Any, ...] | None = None) -> pd.DataFrame:
    """Run a read-only query against RDS and return a DataFrame."""
    return pd.DataFrame(session.fetch_all(sql, params))


def olap(sql: str, params: tuple[Any, ...] | None = None) -> pd.DataFrame:
    """Run a query against Redshift and return a DataFrame."""
    return pd.DataFrame(redshift.fetch_all(sql, params))


def oltp_tables() -> pd.DataFrame:
    """Every table in the operational schema with its live row count."""
    return oltp(
        """
        SELECT c.relname AS table_name,
               c.reltuples::BIGINT AS estimated_rows,
               pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind = 'r'
        ORDER BY c.relname
        """,
        (settings().pg_schema,),
    )


def oltp_exact_counts() -> pd.DataFrame:
    """Exact counts for the business tables. Slower than the estimate, but true."""
    tables = [
        "workstations",
        "concession_items",
        "members",
        "rental_transactions",
        "concession_purchases",
        "concession_order_items",
        "member_points_ledger",
    ]
    union = " UNION ALL ".join(
        f"SELECT '{t}' AS table_name, count(*) AS rows FROM {t}" for t in tables
    )
    return oltp(f"SELECT * FROM ({union}) t ORDER BY table_name")


def olap_tables() -> pd.DataFrame:
    return olap(
        f"""
        SELECT tablename AS table_name
        FROM pg_tables WHERE schemaname = '{settings().redshift_schema}'
        ORDER BY tablename
        """
    )


def open_rentals() -> pd.DataFrame:
    """Who is on the floor right now — rentals with no check-out."""
    return oltp(
        """
        SELECT r.rental_id, r.member_id, m.first_name, m.last_name, m.current_tier,
               r.workstation_id, w.zone_classification, r.session_start_utc
        FROM rental_transactions r
        JOIN members m      ON m.member_id      = r.member_id
        JOIN workstations w ON w.workstation_id = r.workstation_id
        WHERE r.session_end_utc IS NULL
        ORDER BY r.session_start_utc
        """
    )


def workstation_status() -> pd.DataFrame:
    return oltp(
        """
        SELECT zone_classification, status, count(*) AS workstations
        FROM workstations GROUP BY 1, 2 ORDER BY 1, 2
        """
    )


def revenue_by_zone(limit_days: int = 14) -> pd.DataFrame:
    """Rental revenue by zone for the most recent days present in the data."""
    return oltp(
        """
        SELECT w.zone_classification,
               date(r.session_start_utc) AS day,
               count(*)                  AS rentals,
               sum(r.net_amount_paid)    AS net_revenue
        FROM rental_transactions r
        JOIN workstations w ON w.workstation_id = r.workstation_id
        WHERE r.session_end_utc IS NOT NULL
        GROUP BY 1, 2
        ORDER BY 2 DESC, 1
        LIMIT %s
        """,
        (limit_days * 3,),
    )


def backfilled_members() -> pd.DataFrame:
    """The D2 cohort: members that exist only because transactions referenced them."""
    return oltp(
        """
        SELECT source_system, is_backfilled, count(*) AS members,
               min(member_id) AS first_id, max(member_id) AS last_id
        FROM members GROUP BY 1, 2 ORDER BY 1, 2
        """
    )


def points_balance_drift(limit: int = 20) -> pd.DataFrame:
    """Members whose ledger does not add up to their stored balance.

    The ledger is the audit trail; the balance column is a cache of it. When they disagree,
    the cache is wrong and the ledger is the truth.
    """
    return oltp(
        """
        SELECT m.member_id, m.current_points_balance AS stored_balance,
               COALESCE(sum(l.points_delta), 0)      AS ledger_sum,
               m.current_points_balance - COALESCE(sum(l.points_delta), 0) AS drift
        FROM members m
        LEFT JOIN member_points_ledger l ON l.member_id = m.member_id
        GROUP BY m.member_id, m.current_points_balance
        HAVING m.current_points_balance <> COALESCE(sum(l.points_delta), 0)
        ORDER BY abs(m.current_points_balance - COALESCE(sum(l.points_delta), 0)) DESC
        LIMIT %s
        """,
        (limit,),
    )


def top_members(limit: int = 10) -> pd.DataFrame:
    return oltp(
        """
        SELECT m.member_id, m.first_name, m.last_name, m.current_tier,
               m.lifetime_spend_amount, m.current_points_balance,
               count(r.rental_id) AS rentals
        FROM members m
        LEFT JOIN rental_transactions r ON r.member_id = m.member_id
        GROUP BY 1, 2, 3, 4, 5, 6
        ORDER BY m.lifetime_spend_amount DESC
        LIMIT %s
        """,
        (limit,),
    )
