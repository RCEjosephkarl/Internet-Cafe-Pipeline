"""AIMternet-Cafe operations dashboard — Streamlit entry point.

Three sections live under ``pages/``: PC Telemetry, Descriptive Analytics, Data Science. This
page is the landing page plus the shared sidebar time-window control every page re-renders.
Talks to the operational API over HTTP only (``lib/api_client.py``) — no database credentials
live in this process. See CLAUDE.md.
"""

from __future__ import annotations

import streamlit as st
from lib.api_client import get_summary
from lib.formatting import num, pct
from lib.time_window import render_sidebar

st.set_page_config(page_title="AIMternet-Cafe Dashboard", page_icon="🖥️", layout="wide")

render_sidebar()

st.title("AIMternet-Cafe Operations Dashboard")
st.caption(
    "Streamlit talks to the operational API over HTTP only — it holds no database "
    "credentials of its own."
)

summary = get_summary()
if summary.get("source") == "unavailable":
    st.warning(f"Live summary unavailable: {summary.get('error', 'unknown error')}")
else:
    cols = st.columns(4)
    cols[0].metric("Active rentals", num(summary.get("active_rentals")))
    cols[1].metric("Occupancy", pct(summary.get("occupancy_pct", 0)))
    cols[2].metric("Available PCs", num(summary.get("available")))
    cols[3].metric("Members", num(summary.get("members")))

st.markdown(
    """
Use the sidebar to pick a time window, then open a section from the left:

- **PC Telemetry** — live fleet status, hardware health, utilization (RDS + DynamoDB, live).
- **Descriptive Analytics** — revenue, points, member composition (mixed: today's numbers are
  live from RDS, trends are a Redshift warehouse snapshot).
- **Data Science** — derived scoring: member engagement leaderboard, tier migration, PC health
  ranking, revenue efficiency (Redshift warehouse snapshot — descriptive math, not ML).

Every Redshift-sourced card is labeled and carries its own `window` — the warehouse holds a
fixed 62-day bootstrap batch, so "live" only ever applies to the RDS-sourced cards.
"""
)
