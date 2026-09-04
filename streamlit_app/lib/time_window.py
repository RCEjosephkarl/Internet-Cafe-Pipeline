"""Shared sidebar time-window control.

Resolves "Last N days" against the warehouse's own max date
(``GET /v1/metrics/data-window``), never wall-clock ``datetime.now()`` — the dataset is a
fixed 2026-07-01..2026-08-31 batch regardless of what day it is when the dashboard runs.
Every page calls ``render_sidebar()`` once and passes the returned ``days`` to its
Redshift-backed API calls.
"""

from __future__ import annotations

import streamlit as st

from lib.api_client import get_data_window

PRESETS: dict[str, int] = {
    "Last 7 days": 7,
    "Last 30 days": 30,
    "Last 60 days": 60,
    "All data (62 days)": 62,
}


def render_sidebar() -> dict[str, object]:
    """Render the preset selector and the warehouse-snapshot notice. Returns
    ``{"days": int, "data_window": dict}`` for the page to use."""
    data_window = get_data_window()

    st.sidebar.subheader("Time window")
    label = st.sidebar.selectbox("Preset", list(PRESETS.keys()), index=1)
    days = PRESETS[label]

    warehouse_min = data_window.get("warehouse_min_date_utc")
    warehouse_max = data_window.get("warehouse_max_date_utc")
    if data_window.get("source") == "redshift" and warehouse_max:
        st.sidebar.caption(f"Warehouse data: {warehouse_min} → {warehouse_max}")
        st.sidebar.caption(
            "⚠️ Redshift-sourced cards reflect the bootstrap batch only, not POS activity "
            "since bootstrap (CLAUDE.md invariant #9)."
        )
    else:
        st.sidebar.caption(
            "⚠️ Warehouse date range unavailable — Redshift-sourced cards may be empty."
        )

    return {"days": days, "data_window": data_window}
