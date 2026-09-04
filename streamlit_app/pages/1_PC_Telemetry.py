"""PC Telemetry — fleet status and hardware health.

Mostly live: RDS for floor status, DynamoDB for telemetry. The utilization trend and heatmap
are the exception — they come from the Redshift Gold aggregate, so they carry the warehouse
snapshot caveat like every other Redshift-sourced card.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
from lib.api_client import (
    get_summary,
    get_telemetry_fleet_health,
    get_utilization_heatmap,
    get_utilization_hourly,
    get_workstations_status,
)
from lib.formatting import num, pct
from lib.time_window import render_sidebar

st.set_page_config(page_title="PC Telemetry", page_icon="🖥️", layout="wide")
window = render_sidebar()
days = int(window["days"])

st.title("PC Telemetry")
st.caption("Live — RDS + DynamoDB, except the trend/heatmap charts (Redshift, labeled below).")

summary = get_summary()
if summary.get("source") != "unavailable":
    cols = st.columns(4)
    cols[0].metric("Active rentals", num(summary.get("active_rentals")))
    cols[1].metric("Occupancy", pct(summary.get("occupancy_pct", 0)))
    cols[2].metric("Available", num(summary.get("available")))
    cols[3].metric("Occupied", num(summary.get("occupied")))

st.subheader("Fleet health")
fleet_health = get_telemetry_fleet_health()
if fleet_health.get("source") == "unavailable":
    st.warning(f"Telemetry unavailable: {fleet_health.get('error', 'unknown error')}")
else:
    fleet = fleet_health.get("fleet", {})
    cols = st.columns(5)
    cols[0].metric("Sampled PCs", num(fleet.get("sampled_workstations")))
    cols[1].metric("Avg CPU temp", f"{fleet.get('avg_cpu_temp_c', '—')} °C")
    cols[2].metric("Avg GPU temp", f"{fleet.get('avg_gpu_temp_c', '—')} °C")
    cols[3].metric("Thermal alerts", num(fleet.get("thermal_alert_count")))
    cols[4].metric("Stale readings", num(fleet.get("stale_reading_count")))

st.subheader("Workstations by zone")
status = get_workstations_status()
if status.get("source") == "unavailable":
    st.warning(f"Status unavailable: {status.get('error', 'unknown error')}")
else:
    by_zone = pd.DataFrame(status.get("by_zone", []))
    if not by_zone.empty:
        st.dataframe(by_zone, use_container_width=True, hide_index=True)

st.subheader("Hourly utilization trend")
hourly = get_utilization_hourly(days)
if hourly.get("source") == "unavailable":
    st.warning(f"Utilization unavailable: {hourly.get('error', 'unknown error')}")
else:
    hours_df = pd.DataFrame(hourly.get("hours", []))
    if not hours_df.empty:
        hours_df["avg_utilization_pct"] = hours_df["avg_utilization_pct"].astype(float)
        fig = px.line(hours_df, x="hour_utc", y="avg_utilization_pct", markers=True)
        st.plotly_chart(fig, use_container_width=True)
    win = hourly.get("window", {})
    st.caption(
        f"⚠️ Warehouse snapshot ({win.get('start_date', '—')} → {win.get('end_date', '—')}) "
        "— bootstrap batch, not live."
    )

st.subheader("Utilization heatmap — workstation x hour of day")
heatmap = get_utilization_heatmap(days)
if heatmap.get("source") == "unavailable":
    st.warning(f"Heatmap unavailable: {heatmap.get('error', 'unknown error')}")
else:
    cells = pd.DataFrame(heatmap.get("cells", []))
    if not cells.empty:
        cells["avg_utilization_pct"] = cells["avg_utilization_pct"].astype(float)
        pivot = cells.pivot_table(
            index="workstation_id", columns="hour_utc",
            values="avg_utilization_pct", aggfunc="mean",
        )
        fig = px.imshow(pivot, aspect="auto", labels={"color": "Utilization %"})
        st.plotly_chart(fig, use_container_width=True)
    win = heatmap.get("window", {})
    st.caption(
        f"⚠️ Warehouse snapshot ({win.get('start_date', '—')} → {win.get('end_date', '—')}) "
        "— bootstrap batch, not live."
    )

st.subheader("Hardware health ranking")
if fleet_health.get("source") != "unavailable":
    ws_df = pd.DataFrame(fleet_health.get("workstations", []))
    if not ws_df.empty:
        ws_df["cpu_temp_c"] = ws_df["cpu_temp_c"].astype(float)
        ws_df = ws_df.sort_values("cpu_temp_c", ascending=False)
        st.dataframe(ws_df, use_container_width=True, hide_index=True)
