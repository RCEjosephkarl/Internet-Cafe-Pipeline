"""Descriptive Analytics — revenue, points, member composition.

Mixed sourcing, card by card: today's totals are live from RDS, trends and breakdowns are a
Redshift warehouse snapshot (bootstrap batch — see CLAUDE.md invariant #9). Each subheader
says which.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
from lib.api_client import (
    get_members_overview,
    get_points,
    get_points_history,
    get_revenue_by_zone,
    get_revenue_today,
    get_revenue_trend,
)
from lib.formatting import num, pct, peso
from lib.time_window import render_sidebar

st.set_page_config(page_title="Descriptive Analytics", page_icon="📊", layout="wide")
window = render_sidebar()
days = int(window["days"])

st.title("Descriptive Analytics")

st.subheader("Today's revenue")
st.caption("Live — RDS")
today = get_revenue_today()
if today.get("source") == "unavailable":
    st.warning(f"Unavailable: {today.get('error', 'unknown error')}")
else:
    cols = st.columns(3)
    cols[0].metric("Rental", peso(today.get("rental", {}).get("amount", 0)))
    cols[1].metric("Concession", peso(today.get("concession", {}).get("amount", 0)))
    cols[2].metric("Total", peso(today.get("total", 0)))

st.subheader("Revenue by zone")
by_zone = get_revenue_by_zone(days)
if by_zone.get("source") == "unavailable":
    st.warning(f"Unavailable: {by_zone.get('error', 'unknown error')}")
else:
    zdf = pd.DataFrame(by_zone.get("zones", []))
    if not zdf.empty:
        zdf["net_revenue"] = zdf["net_revenue"].astype(float)
        fig = px.bar(zdf, x="zone_classification", y="net_revenue")
        st.plotly_chart(fig, use_container_width=True)
    win = by_zone.get("window", {})
    st.caption(f"⚠️ Warehouse snapshot ({win.get('start_date', '—')} → {win.get('end_date', '—')})")

st.subheader("Revenue trend")
trend = get_revenue_trend(days)
if trend.get("source") == "unavailable":
    st.warning(f"Unavailable: {trend.get('error', 'unknown error')}")
else:
    daily = pd.DataFrame(trend.get("daily", []))
    if not daily.empty:
        for col in ("rental_revenue", "concession_revenue", "gross_profit"):
            daily[col] = daily[col].astype(float)
        fig = px.area(daily, x="day", y=["rental_revenue", "concession_revenue"])
        st.plotly_chart(fig, use_container_width=True)

    mix = pd.DataFrame(trend.get("payment_mix", []))
    if not mix.empty:
        mix["amount"] = mix["amount"].astype(float)
        fig2 = px.pie(mix, names="payment_method", values="amount", title="Payment method mix")
        st.plotly_chart(fig2, use_container_width=True)
    win = trend.get("window", {})
    st.caption(f"⚠️ Warehouse snapshot ({win.get('start_date', '—')} → {win.get('end_date', '—')})")

st.subheader("Points")
st.caption("Today's totals live from RDS; history below is a warehouse snapshot")
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
        melted = hdf.melt(
            id_vars=["day", "transaction_type"], value_vars=["issued", "redeemed"],
            var_name="kind", value_name="points",
        )
        fig = px.line(melted, x="day", y="points", color="kind", line_dash="transaction_type")
        st.plotly_chart(fig, use_container_width=True)
    win = history.get("window", {})
    st.caption(f"⚠️ Warehouse snapshot ({win.get('start_date', '—')} → {win.get('end_date', '—')})")

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
        fig = px.bar(tier_df, x="tier", y="members")
        st.plotly_chart(fig, use_container_width=True)
