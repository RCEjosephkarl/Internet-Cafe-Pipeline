"""Data Science — derived analytics and scoring.

Descriptive math (weighted sums, normalized ratios) computed in
``aimternet.api.metrics_scoring`` — no trained model anywhere in this section. Every card is
Redshift-sourced, so every card is a warehouse snapshot: the bootstrap batch only, not POS
activity since bootstrap (CLAUDE.md invariant #9).
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
from lib.api_client import (
    get_efficiency_revenue_per_hour,
    get_members_leaderboard,
    get_members_tier_migration,
    get_workstations_health_score,
)
from lib.time_window import render_sidebar

st.set_page_config(page_title="Data Science", page_icon="🔬", layout="wide")
window = render_sidebar()
days = int(window["days"])

st.title("Data Science")
st.caption(
    "Derived analytics and scoring — descriptive math, not machine learning. Redshift "
    "warehouse snapshot: bootstrap batch only."
)

st.subheader("Member engagement leaderboard")
leaderboard = get_members_leaderboard(days, 25)
if leaderboard.get("source") == "unavailable":
    st.warning(f"Unavailable: {leaderboard.get('error', 'unknown error')}")
else:
    members = leaderboard.get("members", [])
    if members:
        top = members[0]
        st.metric(
            "Top engagement score",
            f"{top.get('engagement_score', '—')} — {top.get('first_name', '')} "
            f"{top.get('last_name', '')}",
        )
        st.dataframe(pd.DataFrame(members), use_container_width=True, hide_index=True)
    else:
        st.info("No members with activity in this window.")
    win = leaderboard.get("window", {})
    st.caption(f"⚠️ Warehouse snapshot ({win.get('start_date', '—')} → {win.get('end_date', '—')})")

st.subheader("Tier migration")
tier_migration = get_members_tier_migration(days)
if tier_migration.get("source") == "unavailable":
    st.warning(f"Unavailable: {tier_migration.get('error', 'unknown error')}")
else:
    dist = pd.DataFrame(tier_migration.get("current_distribution", []))
    if not dist.empty:
        dist["members"] = dist["members"].astype(int)
        fig = px.bar(dist, x="tier", y="members", title="Current tier distribution")
        st.plotly_chart(fig, use_container_width=True)
    rates = pd.DataFrame(tier_migration.get("conversion_rates", []))
    if not rates.empty:
        st.dataframe(rates, use_container_width=True, hide_index=True)
    else:
        st.info("No tier transitions recorded.")

st.subheader("PC health score")
health = get_workstations_health_score(days)
if health.get("source") == "unavailable":
    st.warning(f"Unavailable: {health.get('error', 'unknown error')}")
else:
    fleet_avg = health.get("fleet_avg_health_score")
    st.metric("Fleet avg health score", fleet_avg if fleet_avg is not None else "—")
    ws = pd.DataFrame(health.get("workstations", []))
    if not ws.empty:
        st.dataframe(ws, use_container_width=True, hide_index=True)
    win = health.get("window", {})
    st.caption(f"⚠️ Warehouse snapshot ({win.get('start_date', '—')} → {win.get('end_date', '—')})")

st.subheader("Revenue per occupied hour, by zone")
efficiency = get_efficiency_revenue_per_hour(days)
if efficiency.get("source") == "unavailable":
    st.warning(f"Unavailable: {efficiency.get('error', 'unknown error')}")
else:
    zdf = pd.DataFrame(efficiency.get("zones", []))
    if not zdf.empty:
        zdf["revenue_per_occupied_hour"] = zdf["revenue_per_occupied_hour"].astype(float)
        fig = px.bar(zdf, x="zone", y="revenue_per_occupied_hour")
        st.plotly_chart(fig, use_container_width=True)
    win = efficiency.get("window", {})
    st.caption(f"⚠️ Warehouse snapshot ({win.get('start_date', '—')} → {win.get('end_date', '—')})")
