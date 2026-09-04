"""AIMternet-Cafe operations dashboard — Streamlit entry point.

Two sections live under ``pages/``: PC Telemetry and Business Analytics. This page is the
landing page plus the shared sidebar time-window control every page re-renders.
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

- **PC Telemetry** — the floor and the fleet: per-peripheral connectivity, compute and network
  health (live, from RDS and DynamoDB), the utilization heatmap, and workstation events.
- **Business Analytics** — money, membership activity, and a per-member summary. Today's
  numbers and anything about a single member are live from RDS; the trends come from Redshift.

Redshift sees the till: a rental or sale rung up on the POS reaches these cards on the next
pipeline pass. Each stage now triggers the next off the data it produced rather than waiting
for a clock slot, and the exports run every 15 minutes, so a Redshift-sourced card trails an
RDS-sourced one by about that. Every Redshift-sourced card is labeled and carries its own
`window`. "Refresh data" in the sidebar clears this dashboard's cache when you know a pass
has just landed.
"""
)
