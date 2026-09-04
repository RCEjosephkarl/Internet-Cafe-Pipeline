"""PC Telemetry — fleet status, peripherals, hardware and workstation events.

Mostly live: RDS for floor status, DynamoDB for the latest telemetry reading per machine and
for the newest events. The utilization heatmap and the event summary are the exceptions — they
come from Redshift, so they carry the warehouse's own window rather than the till's clock.

The page is grouped by *what broke*, not by where the number came from: peripherals, compute,
network, events. A person walking the floor is looking for one of those four.
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
from lib.api_client import (
    get_events_recent,
    get_events_summary,
    get_summary,
    get_telemetry_fleet_health,
    get_utilization_heatmap,
    get_workstations_status,
)
from lib.charts import SEQUENTIAL, STATUS, series, show
from lib.formatting import num, pct
from lib.time_window import render_sidebar

st.set_page_config(page_title="PC Telemetry", page_icon="🖥️", layout="wide")
window = render_sidebar()
days = int(window["days"])

#: Fixed display order, so the colour a peripheral gets never moves between charts.
PERIPHERALS = ("keyboard", "mouse", "headset")

st.title("PC Telemetry")
st.caption("Live — RDS + DynamoDB, except the heatmap and event summary (Redshift, labeled).")

summary = get_summary()
if summary.get("source") != "unavailable":
    cols = st.columns(4)
    cols[0].metric("Active rentals", num(summary.get("active_rentals")))
    cols[1].metric("Occupancy", pct(summary.get("occupancy_pct", 0)))
    cols[2].metric("Available", num(summary.get("available")))
    cols[3].metric("Occupied", num(summary.get("occupied")))

st.subheader("Floor")
st.caption("Live — RDS")
status = get_workstations_status()
if status.get("source") == "unavailable":
    st.warning(f"Status unavailable: {status.get('error', 'unknown error')}")
else:
    by_zone = pd.DataFrame(status.get("by_zone", []))
    if not by_zone.empty:
        by_zone["workstations"] = by_zone["workstations"].astype(int)
        order = sorted(by_zone["status"].unique())
        fig = px.bar(
            by_zone, x="zone_classification", y="workstations", color="status",
            barmode="stack", category_orders={"status": order},
            color_discrete_sequence=series(len(order)),
            labels={"workstations": "Workstations", "zone_classification": "", "status": ""},
        )
        show(fig)

fleet_health = get_telemetry_fleet_health()
fleet = fleet_health.get("fleet", {})
readings = pd.DataFrame(fleet_health.get("workstations", []))
unavailable = fleet_health.get("source") == "unavailable"
if unavailable:
    st.warning(f"Telemetry unavailable: {fleet_health.get('error', 'unknown error')}")

# ---------------------------------------------------------------------------- peripherals

st.subheader("Peripherals")
st.caption("Live — DynamoDB, latest reading per workstation")
if not unavailable and not readings.empty:
    peripherals = pd.DataFrame(fleet.get("peripherals", []))
    if not peripherals.empty:
        # One tile per peripheral rather than one "peripherals OK" number: a fleet missing
        # twelve headsets and a fleet missing twelve mice are different jobs for whoever has
        # to walk the floor, and a single count cannot tell them apart.
        cols = st.columns(len(peripherals))
        for col, row in zip(cols, peripherals.to_dict("records"), strict=False):
            missing = int(row["disconnected"])
            col.metric(
                row["peripheral"].title(),
                f"{int(row['connected'])} connected",
                delta=None if not missing else f"{missing} disconnected",
                delta_color="inverse",
            )

    missing_rows = readings[
        ~readings[[f"{name}_connected" for name in PERIPHERALS]].all(axis=1)
    ]
    if missing_rows.empty:
        st.success(
            "Every workstation reports all three peripherals connected. This is one instant "
            "— the latest reading per machine. Disconnections over time are in **Workstation "
            "events** below, as `PERIPHERAL_ALERT`."
        )
    else:
        long = missing_rows.melt(
            id_vars=["workstation_id", "zone"],
            value_vars=[f"{name}_connected" for name in PERIPHERALS],
            var_name="peripheral", value_name="connected",
        )
        long = long[~long["connected"]]
        long["peripheral"] = long["peripheral"].str.removesuffix("_connected").str.title()
        by_zone = long.groupby(["zone", "peripheral"], as_index=False).size()
        fig = px.bar(
            by_zone, x="zone", y="size", color="peripheral", barmode="group",
            category_orders={"peripheral": [p.title() for p in PERIPHERALS]},
            color_discrete_sequence=series(len(PERIPHERALS)),
            labels={"size": "Disconnected", "zone": "", "peripheral": ""},
        )
        show(fig)
        st.dataframe(
            missing_rows[
                ["workstation_id", "zone", "status", "missing_peripherals", "timestamp_utc"]
            ],
            width="stretch", hide_index=True,
        )

# -------------------------------------------------------------------------------- compute

st.subheader("Compute")
st.caption("Live — DynamoDB")
if not unavailable and not readings.empty:
    cols = st.columns(5)
    cols[0].metric("Sampled PCs", num(fleet.get("sampled_workstations")))
    cols[1].metric("Avg CPU load", f"{fleet.get('avg_cpu_load_pct', '—')} %")
    cols[2].metric("Avg CPU temp", f"{fleet.get('avg_cpu_temp_c', '—')} °C")
    cols[3].metric("Avg GPU load", f"{fleet.get('avg_gpu_load_pct', '—')} %")
    cols[4].metric("Avg GPU temp", f"{fleet.get('avg_gpu_temp_c', '—')} °C")

    hot = readings.copy()
    for col in ("cpu_temp_c", "gpu_temp_c"):
        hot[col] = hot[col].astype(float)
    hot = hot.sort_values("cpu_temp_c", ascending=False).head(15)
    # Two temperatures share one scale (°C), so they belong on one axis. Load is a
    # percentage and gets its own tiles above rather than a second y-axis.
    temps = hot.melt(
        id_vars=["workstation_id"], value_vars=["cpu_temp_c", "gpu_temp_c"],
        var_name="sensor", value_name="temp_c",
    )
    temps["sensor"] = temps["sensor"].map({"cpu_temp_c": "CPU", "gpu_temp_c": "GPU"})
    fig = px.bar(
        temps, x="workstation_id", y="temp_c", color="sensor", barmode="group",
        category_orders={"sensor": ["CPU", "GPU"]},
        color_discrete_sequence=series(2),
        labels={"temp_c": "°C", "workstation_id": "", "sensor": ""},
        title="Hottest 15 workstations",
    )
    show(fig)
    thermal = int(fleet.get("thermal_alert_count") or 0)
    st.caption(
        f"🔴 {thermal} thermal alert(s)" if thermal else "No workstation is over its thermal "
        "threshold."
    )

# -------------------------------------------------------------------------------- network

st.subheader("Network")
st.caption("Live — DynamoDB")
if not unavailable and not readings.empty:
    net = readings.copy()
    for col in ("latency_ping_ms", "packet_loss_pct"):
        net[col] = net[col].astype(float)
    cols = st.columns(3)
    cols[0].metric("Avg latency", f"{net['latency_ping_ms'].mean():.1f} ms")
    cols[1].metric("Max packet loss", f"{net['packet_loss_pct'].max():.2f} %")
    cols[2].metric("Network alerts", num(fleet.get("network_alert_count")))

    worst = net.sort_values("latency_ping_ms", ascending=False).head(15)
    fig = px.bar(
        worst, x="workstation_id", y="latency_ping_ms",
        color_discrete_sequence=series(1),
        labels={"latency_ping_ms": "ms", "workstation_id": ""},
        title="Highest latency, 15 worst",
    )
    show(fig)
    st.dataframe(
        net[net["has_network_alert"]][
            ["workstation_id", "zone", "latency_ping_ms", "packet_loss_pct", "timestamp_utc"]
        ],
        width="stretch", hide_index=True,
    )

# ------------------------------------------------------------------------------ utilization

st.subheader("Utilization heatmap — workstation by hour of day")
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
        # Magnitude, so one hue light→dark. The per-hour average a line chart would show is
        # this matrix's column mean — it was on this page twice, and the second copy is gone.
        fig = px.imshow(
            pivot, aspect="auto", color_continuous_scale=list(SEQUENTIAL),
            labels={"color": "Utilization %", "x": "Hour (UTC)", "y": ""},
        )
        show(fig)
    win = heatmap.get("window", {})
    st.caption(
        f"Warehouse ({win.get('start_date', '—')} → {win.get('end_date', '—')}) "
        "— telemetry-derived, so this stops at the bootstrap batch."
    )

# --------------------------------------------------------------------------------- events

st.subheader("Workstation events")
events = get_events_summary(days)
if events.get("source") == "unavailable":
    st.warning(f"Event summary unavailable: {events.get('error', 'unknown error')}")
else:
    by_type = pd.DataFrame(events.get("by_type", []))
    if not by_type.empty:
        cols = st.columns(len(by_type))
        for col, row in zip(cols, by_type.to_dict("records"), strict=False):
            col.metric(
                row["event_type"].replace("_", " ").title(),
                num(row["events"]),
                help=f"across {row['workstations']} workstations",
            )

    daily = pd.DataFrame(events.get("daily", []))
    if not daily.empty:
        daily["events"] = daily["events"].astype(int)
        order = sorted(daily["event_type"].unique())
        fig = px.bar(
            daily, x="day", y="events", color="event_type", barmode="stack",
            category_orders={"event_type": order},
            color_discrete_sequence=series(len(order)),
            labels={"events": "Events", "day": "", "event_type": ""},
        )
        show(fig)

    alerts = pd.DataFrame(events.get("alerts", []))
    if not alerts.empty:
        # value_name must not collide with the `alerts` total column already on the frame.
        melted = alerts.melt(
            id_vars=["workstation_id"], value_vars=["hardware_alerts", "peripheral_alerts"],
            var_name="kind", value_name="count",
        )
        melted["kind"] = melted["kind"].map(
            {"hardware_alerts": "Hardware", "peripheral_alerts": "Peripheral"}
        )
        fig = px.bar(
            melted, x="workstation_id", y="count", color="kind", barmode="stack",
            category_orders={"kind": ["Hardware", "Peripheral"]},
            color_discrete_sequence=[STATUS["critical"], STATUS["warning"]],
            labels={"count": "Alerts", "workstation_id": "", "kind": ""},
            title="Most alerts in the window",
        )
        show(fig)
    win = events.get("window", {})
    st.caption(f"Warehouse ({win.get('start_date', '—')} → {win.get('end_date', '—')})")

recent = get_events_recent(25)
if recent.get("source") == "unavailable":
    st.warning(f"Recent events unavailable: {recent.get('error', 'unknown error')}")
else:
    recent_df = pd.DataFrame(recent.get("events", []))
    if not recent_df.empty:
        st.caption(
            "Live — DynamoDB. A check-in rung up on the POS seconds ago is already here; the "
            "summary above waits for the next pipeline pass."
        )
        st.dataframe(recent_df.head(40), width="stretch", hide_index=True)
