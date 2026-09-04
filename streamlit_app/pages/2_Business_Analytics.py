"""Business Analytics — money, membership, and one member at a time.

Three blocks, in the order a manager asks the questions: what did we take, who is
spending it, and what has this particular customer done.

Sourcing is mixed and each block says which. Today's totals and everything about a single
member come from RDS, so they match the till exactly. The trends come from Redshift, which
now includes POS activity but trails the till by roughly one pipeline pass.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
from lib.api_client import (
    get_member_summary,
    get_members_leaderboard,
    get_members_overview,
    get_points,
    get_points_history,
    get_revenue_by_zone,
    get_revenue_today,
    get_revenue_trend,
)
from lib.charts import series, show
from lib.formatting import num, pct, peso
from lib.time_window import render_sidebar

st.set_page_config(page_title="Business Analytics", page_icon="📊", layout="wide")
window = render_sidebar()
days = int(window["days"])

st.title("Business Analytics")

# ----------------------------------------------------------------------------------- money

st.header("Money")

st.subheader("Today's revenue")
st.caption("Live — RDS")
today = get_revenue_today()
if today.get("source") == "unavailable":
    st.warning(f"Unavailable: {today.get('error', 'unknown error')}")
else:
    cols = st.columns(3)
    cols[0].metric(
        "Rental", peso(today.get("rental", {}).get("amount", 0)),
        help=f"{today.get('rental', {}).get('transactions', 0)} transactions",
    )
    cols[1].metric(
        "Concession", peso(today.get("concession", {}).get("amount", 0)),
        help=f"{today.get('concession', {}).get('transactions', 0)} transactions",
    )
    cols[2].metric("Total", peso(today.get("total", 0)))

st.subheader("Revenue trend")
trend = get_revenue_trend(days)
if trend.get("source") == "unavailable":
    st.warning(f"Unavailable: {trend.get('error', 'unknown error')}")
else:
    daily = pd.DataFrame(trend.get("daily", []))
    if not daily.empty:
        for col in ("rental_revenue", "concession_revenue", "gross_profit"):
            daily[col] = daily[col].astype(float)
        stacked = daily.melt(
            id_vars=["day"], value_vars=["rental_revenue", "concession_revenue"],
            var_name="stream", value_name="revenue",
        )
        stacked["stream"] = stacked["stream"].map(
            {"rental_revenue": "Rental", "concession_revenue": "Concession"}
        )
        fig = px.area(
            stacked, x="day", y="revenue", color="stream",
            category_orders={"stream": ["Rental", "Concession"]},
            color_discrete_sequence=series(2),
            labels={"revenue": "PHP", "day": "", "stream": ""},
        )
        show(fig)

        # Profit is a different quantity from revenue and would need a second y-scale on the
        # chart above. It gets its own, on one axis.
        fig = px.line(
            daily, x="day", y="gross_profit", markers=True,
            color_discrete_sequence=series(1),
            labels={"gross_profit": "PHP", "day": ""}, title="Gross profit",
        )
        show(fig)

    mix = pd.DataFrame(trend.get("payment_mix", []))
    if not mix.empty:
        mix["amount"] = mix["amount"].astype(float)
        mix = mix.sort_values("amount")
        # A bar, not a pie: these are close values, and a pie makes close values
        # indistinguishable at exactly the moment the difference matters.
        fig = px.bar(
            mix, x="amount", y="payment_method", orientation="h",
            color_discrete_sequence=series(1),
            labels={"amount": "PHP", "payment_method": ""}, title="Payment method mix",
        )
        show(fig)
    win = trend.get("window", {})
    st.caption(f"Warehouse ({win.get('start_date', '—')} → {win.get('end_date', '—')})")

st.subheader("Revenue by zone")
by_zone = get_revenue_by_zone(days)
if by_zone.get("source") == "unavailable":
    st.warning(f"Unavailable: {by_zone.get('error', 'unknown error')}")
else:
    zdf = pd.DataFrame(by_zone.get("zones", []))
    if not zdf.empty:
        zdf["net_revenue"] = zdf["net_revenue"].astype(float)
        fig = px.bar(
            zdf, x="zone_classification", y="net_revenue",
            color_discrete_sequence=series(1),
            labels={"net_revenue": "PHP", "zone_classification": ""},
        )
        show(fig)
        st.dataframe(zdf, width="stretch", hide_index=True)
    win = by_zone.get("window", {})
    st.caption(f"Warehouse ({win.get('start_date', '—')} → {win.get('end_date', '—')})")

# ------------------------------------------------------------------------------ membership

st.header("Membership activity")

st.subheader("Members")
st.caption("Live — RDS (registrations up to this second, not bootstrap-only)")
overview = get_members_overview(days)
if overview.get("source") == "unavailable":
    st.warning(f"Unavailable: {overview.get('error', 'unknown error')}")
else:
    cols = st.columns(3)
    cols[0].metric("New members in window", num(overview.get("new_members")))
    cols[1].metric("Active members", num(overview.get("active_members")))
    cols[2].metric("Backfilled (D2 stubs)", num(overview.get("backfilled_members")))
    tier_df = pd.DataFrame(overview.get("by_tier", []))
    if not tier_df.empty:
        tier_df["members"] = tier_df["members"].astype(int)
        fig = px.bar(
            tier_df, x="tier", y="members", color_discrete_sequence=series(1),
            labels={"members": "Members", "tier": ""}, title="Tier distribution",
        )
        show(fig)

st.subheader("Points")
st.caption("Today's totals live from RDS; the history below is the warehouse")
points_today = get_points()
if points_today.get("source") != "unavailable":
    cols = st.columns(2)
    cols[0].metric("Issued today", num(points_today.get("issued_today")))
    cols[1].metric("Redeemed today", num(points_today.get("redeemed_today")))

history = get_points_history(days)
if history.get("source") == "unavailable":
    st.warning(f"Unavailable: {history.get('error', 'unknown error')}")
else:
    cols = st.columns(3)
    cols[0].metric("Redemption rate", pct(history.get("redemption_rate_pct", 0)))
    cols[1].metric("Outstanding liability", peso(history.get("outstanding_liability", 0)))
    cols[2].metric("Outstanding points", num(history.get("outstanding")))
    hdf = pd.DataFrame(history.get("daily", []))
    if not hdf.empty:
        # Summed across transaction types rather than dashed per type: the old chart drew one
        # line per (kind, transaction_type) pair, and dashing reads as "projection" rather
        # than "a different ledger reason". The reasons are in the table underneath.
        rolled = hdf.groupby("day", as_index=False)[["issued", "redeemed"]].sum()
        melted = rolled.melt(
            id_vars=["day"], value_vars=["issued", "redeemed"],
            var_name="kind", value_name="points",
        )
        melted["kind"] = melted["kind"].str.title()
        fig = px.line(
            melted, x="day", y="points", color="kind", markers=True,
            category_orders={"kind": ["Issued", "Redeemed"]},
            color_discrete_sequence=series(2),
            labels={"points": "Points", "day": "", "kind": ""},
        )
        show(fig)
        st.dataframe(hdf, width="stretch", hide_index=True)
    win = history.get("window", {})
    st.caption(f"Warehouse ({win.get('start_date', '—')} → {win.get('end_date', '—')})")

# ------------------------------------------------------------------------------ per member

st.header("Per member")

st.subheader("Engagement leaderboard")
leaderboard = get_members_leaderboard(days, 25)
member_ids: list[str] = []
if leaderboard.get("source") == "unavailable":
    st.warning(f"Unavailable: {leaderboard.get('error', 'unknown error')}")
else:
    members = leaderboard.get("members", [])
    if members:
        member_ids = [m["member_id"] for m in members]
        top = members[0]
        st.metric(
            "Top engagement score",
            f"{top.get('engagement_score', '—')} — {top.get('first_name', '')} "
            f"{top.get('last_name', '')}",
        )
        st.dataframe(pd.DataFrame(members), width="stretch", hide_index=True)
    else:
        st.info("No members with activity in this window.")
    win = leaderboard.get("window", {})
    st.caption(f"Warehouse ({win.get('start_date', '—')} → {win.get('end_date', '—')})")

st.subheader("Member summary")
st.caption("Live — RDS. A rental or sale rung up seconds ago is already here.")
choice = st.selectbox(
    "Member", member_ids or ["—"],
    help="The leaderboard's members, or type any card number below.",
)
typed = st.text_input("…or a card number", placeholder="M-1555").strip()
member_id = typed or (choice if choice != "—" else "")

if not member_id:
    st.info("Pick a member above, or type a card number.")
else:
    detail = get_member_summary(member_id)
    if detail.get("source") == "unavailable":
        st.warning(f"Unavailable: {detail.get('error', 'unknown error')}")
    elif not detail.get("found"):
        st.warning(f"No member {member_id}.")
    else:
        member = detail["member"]
        totals = detail["totals"]
        st.markdown(
            f"**{member['first_name']} {member['last_name']}** · {member['current_tier']}"
            + ("  ·  backfilled D2 stub" if member.get("is_backfilled") else "")
        )
        cols = st.columns(4)
        cols[0].metric("Lifetime spend", peso(member["lifetime_spend_amount"]))
        cols[1].metric("Points balance", num(member["current_points_balance"]))
        cols[2].metric("Rentals", num(totals["rentals"]), help=f"{totals['hours']} hours")
        cols[3].metric("Purchases", num(totals["purchases"]))

        cols = st.columns(3)
        cols[0].metric("Rental spend", peso(totals["rental_spend"]))
        cols[1].metric("Concession spend", peso(totals["concession_spend"]))
        cols[2].metric(
            "Redeemable now", peso(totals["redeemable_value"]),
            help=f"{totals['redeemable_units']} x 100 points",
        )

        tabs = st.tabs(["Rentals", "Purchases", "Points ledger"])
        for tab, key in zip(
            tabs, ("rentals", "purchases", "points_ledger"), strict=True
        ):
            rows = pd.DataFrame(detail.get(key, []))
            with tab:
                if rows.empty:
                    st.info("Nothing recorded.")
                else:
                    st.dataframe(rows, width="stretch", hide_index=True)
