/* AIMternet-Cafe dashboard.
 *
 * Reads the metrics API and nothing else - no database driver, no credentials, no SQL
 * (spec §3, §7.4). Refreshes on a timer; a failed refresh shows a status rather than
 * blanking the page, because a dashboard that goes empty when one endpoint hiccups is
 * worse than one that shows slightly stale numbers.
 */

const API = "/v1/metrics";
const REFRESH_MS = 15000;

const $ = (id) => document.getElementById(id);
const peso = (v) => "₱" + Number(v).toLocaleString("en-PH", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const num = (v) => Number(v).toLocaleString("en-US");

async function get(path) {
  const response = await fetch(`${API}${path}`, { headers: { Accept: "application/json" } });
  if (!response.ok) throw new Error(`${path} -> ${response.status}`);
  return response.json();
}

function table(headers, rows) {
  if (!rows.length) return '<p class="muted">No data.</p>';
  const head = headers.map((h) => `<th class="${h.num ? "num" : ""}">${h.label}</th>`).join("");
  const body = rows
    .map((row) => "<tr>" + headers.map((h) => `<td class="${h.num ? "num" : ""}">${h.render(row)}</td>`).join("") + "</tr>")
    .join("");
  return `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function tile(label, value, note = "") {
  return `<div class="tile"><div class="label">${label}</div><div class="value">${value}</div>${note ? `<div class="note">${note}</div>` : ""}</div>`;
}

async function refresh() {
  const pill = $("status-pill");
  const problems = [];

  const settle = async (fn) => { try { return await fn(); } catch (e) { problems.push(e.message); return null; } };

  const [summary, revenue, points, status, active, telemetry, utilization, zoneRevenue] =
    await Promise.all([
      settle(() => get("/summary")),
      settle(() => get("/revenue/today")),
      settle(() => get("/points")),
      settle(() => get("/workstations/status")),
      settle(() => get("/rentals/active")),
      settle(() => get("/telemetry/recent?limit=12")),
      settle(() => get("/utilization/hourly?days=7")),
      settle(() => get("/revenue/by-zone?days=30")),
    ]);

  if (summary) {
    $("tiles").innerHTML = [
      tile("Active rentals", num(summary.active_rentals)),
      tile("Available", num(summary.available), `of ${num(summary.total_workstations)} workstations`),
      tile("Occupied", num(summary.occupied), `${summary.occupancy_pct}% occupancy`),
      tile("Members", num(summary.members), `${num(summary.members_backfilled)} backfilled (D2)`),
      tile("Revenue today", revenue ? peso(revenue.total) : "—"),
    ].join("");
  }

  if (revenue) {
    $("revenue").innerHTML = table(
      [
        { label: "Stream", render: (r) => r.name },
        { label: "Transactions", num: true, render: (r) => num(r.transactions) },
        { label: "Amount", num: true, render: (r) => peso(r.amount) },
      ],
      [
        { name: "Rentals", ...revenue.rental },
        { name: "Concessions", ...revenue.concession },
        { name: "Total", transactions: revenue.rental.transactions + revenue.concession.transactions, amount: revenue.total },
      ]
    ) + `<p class="muted" style="margin-bottom:0">Live source: ${revenue.source}</p>`;
  }

  if (points) {
    $("points").innerHTML = table(
      [
        { label: "Metric", render: (r) => r.name },
        { label: "All time", num: true, render: (r) => num(r.all) },
        { label: "Today", num: true, render: (r) => num(r.today) },
      ],
      [
        { name: "Issued", all: points.issued, today: points.issued_today },
        { name: "Redeemed", all: points.redeemed, today: points.redeemed_today },
      ]
    );
  }

  if (status) {
    $("zones").innerHTML = table(
      [
        { label: "Zone", render: (r) => r.zone_classification },
        { label: "Status", render: (r) => `<span class="chip ${r.status}">${r.status}</span>` },
        { label: "Count", num: true, render: (r) => num(r.workstations) },
      ],
      status.by_zone
    );

    $("floor").innerHTML = status.workstations
      .map((w) => `<div class="pc ${w.status}" title="${w.workstation_id} — ${w.status}${w.member_id ? " — " + w.member_id : ""}">${w.workstation_id.replace("PC-", "")}</div>`)
      .join("");
  }

  if (active) {
    $("active").innerHTML = table(
      [
        { label: "Rental", render: (r) => r.rental_id },
        { label: "Member", render: (r) => `${r.first_name} ${r.last_name} <span class="muted">(${r.current_tier})</span>` },
        { label: "PC", render: (r) => r.workstation_id },
        { label: "Since (UTC)", render: (r) => String(r.session_start_utc).replace("T", " ").slice(0, 16) },
      ],
      active
    );
  }

  if (telemetry) {
    $("telemetry").innerHTML = telemetry.source === "unavailable"
      ? `<p class="err">Unavailable: ${telemetry.error || "unknown"}</p>`
      : table(
          [
            { label: "PC", render: (r) => r.workstation_id },
            { label: "Status", render: (r) => `<span class="chip ${r.status === "OCCUPIED" ? "OCCUPIED" : "AVAILABLE"}">${r.status}</span>` },
            { label: "CPU %", num: true, render: (r) => r.cpu_load_pct },
            { label: "Temp °C", num: true, render: (r) => r.cpu_temp_c },
            { label: "Ping ms", num: true, render: (r) => r.latency_ping_ms },
          ],
          telemetry.readings
        );
  }

  if (utilization) {
    $("utilization").innerHTML = utilization.source === "unavailable"
      ? `<p class="err">Unavailable: ${utilization.error || "warehouse not loaded"}</p>`
      : table(
          [
            { label: "Hour (UTC)", render: (r) => String(r.hour_utc).padStart(2, "0") + ":00" },
            { label: "Utilisation", render: (r) => `<div class="bar"><span style="width:${Math.min(100, Number(r.avg_utilization_pct))}%"></span></div>` },
            { label: "%", num: true, render: (r) => Number(r.avg_utilization_pct).toFixed(1) },
            { label: "Readings", num: true, render: (r) => num(r.readings) },
          ],
          utilization.hours
        );
  }

  if (zoneRevenue) {
    $("zone-revenue").innerHTML = zoneRevenue.source === "unavailable"
      ? `<p class="err">Unavailable: ${zoneRevenue.error || "warehouse not loaded"}</p>`
      : table(
          [
            { label: "Zone", render: (r) => r.zone_classification },
            { label: "Rentals", num: true, render: (r) => num(r.rentals) },
            { label: "Net revenue", num: true, render: (r) => peso(r.net_revenue) },
            { label: "Avg hrs", num: true, render: (r) => Number(r.avg_hours).toFixed(2) },
          ],
          zoneRevenue.zones
        );
  }

  $("updated").textContent = "updated " + new Date().toLocaleTimeString();
  if (problems.length) {
    pill.textContent = `${problems.length} endpoint(s) failing`;
    pill.className = "bad";
  } else {
    pill.textContent = "live";
    pill.className = "ok";
  }
}

refresh();
setInterval(refresh, REFRESH_MS);
