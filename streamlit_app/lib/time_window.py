"""Shared sidebar time-window control.

Resolves "Last N days" against the warehouse's own max date
(``GET /v1/metrics/data-window``), never wall-clock ``datetime.now()``. The warehouse is a
fixed 2026-07-01..2026-08-31 batch with POS activity layered on top of it, so a window
measured from today would mostly miss the batch. Every page calls ``render_sidebar()`` once
and passes the returned ``days`` to its Redshift-backed API calls.
"""

from __future__ import annotations

from datetime import date, datetime

import streamlit as st

from lib.api_client import get_data_window

#: Fixed presets. The "all data" entry is appended at render time from the window the API
#: reports -- the span used to be exactly 62 days and is not any more, because it grows every
#: day the cafe is open.
PRESETS: dict[str, int] = {
    "Last 7 days": 7,
    "Last 30 days": 30,
    "Last 60 days": 60,
}


def _as_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


def render_sidebar() -> dict[str, object]:
    """Render the preset selector and the warehouse-freshness notice. Returns
    ``{"days": int, "data_window": dict}`` for the page to use."""
    data_window = get_data_window()

    warehouse_min = _as_date(data_window.get("warehouse_min_date_utc"))
    warehouse_max = _as_date(data_window.get("warehouse_max_date_utc"))
    bootstrap_max = _as_date(data_window.get("bootstrap_max_date_utc"))

    presets = dict(PRESETS)
    if warehouse_min and warehouse_max:
        span = (warehouse_max - warehouse_min).days + 1
        presets[f"All data ({span} days)"] = span

    st.sidebar.subheader("Time window")
    labels = list(presets)
    label = st.sidebar.selectbox("Preset", labels, index=min(1, len(labels) - 1))
    days = presets[label]

    if data_window.get("source") == "redshift" and warehouse_max:
        st.sidebar.caption(f"Warehouse data: {warehouse_min} → {warehouse_max}")
        pos_rows = int(data_window.get("pos_rows") or 0)
        if pos_rows:
            st.sidebar.caption(
                f"Includes {pos_rows:,} POS rental(s) since the bootstrap batch. The exports "
                "run every 15 minutes and each stage triggers the next, so these cards trail "
                "the till by about that."
            )
        else:
            st.sidebar.caption(
                "No POS activity has reached the warehouse yet — these cards are the "
                "bootstrap batch. The exports run every 15 minutes."
            )
        # The batch and the POS rows need not be contiguous: the cafe may simply not have
        # been used for a few days in between. A short window landing inside that gap is
        # empty for an honest reason, and saying so is the difference between a reader
        # trusting an empty chart and filing a bug against it.
        if bootstrap_max and (warehouse_max - bootstrap_max).days > 1:
            quiet = (warehouse_max - bootstrap_max).days
            st.sidebar.caption(
                f"⚠️ {quiet} day(s) between the end of the batch ({bootstrap_max}) and the "
                "latest POS activity — short windows may span a mostly-empty stretch."
            )
        st.sidebar.caption(
            "Utilization and hardware cards stop at the batch: raw telemetry has no POS "
            "write path."
        )
    else:
        st.sidebar.caption(
            "⚠️ Warehouse date range unavailable — Redshift-sourced cards may be empty."
        )

    # The API caches each warehouse answer for 60s and this client caches on top of it, so a
    # reader who knows a pipeline pass has just landed would otherwise have to wait out both
    # for no reason. Clearing the client cache and rerunning is the whole fix; the API's own
    # 60s is short enough to wait out.
    st.sidebar.divider()
    if st.sidebar.button("Refresh data", width="stretch"):
        st.cache_data.clear()
        st.rerun()

    return {"days": days, "data_window": data_window}
