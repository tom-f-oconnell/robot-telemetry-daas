// rtdaas web UI — vanilla JS single-page app driven entirely by the public API.
// Views: sessions, session detail, events, trials, trial detail, gestures, catalog,
// quality, contracts, jobs. Plotly (loaded from CDN in index.html) draws the charts.

const main = document.getElementById("main");
const GESTURE_COLORS = ["#534ab7", "#0f6e56", "#993c1d", "#993556", "#185fa5", "#3b6d11", "#854f0b", "#a32d2d",
  "#7f77dd", "#1d9e75", "#d85a30", "#d4537e", "#378add", "#639922", "#ba7517"];

// ------------------------------------------------------------------ helpers

// Plotly is loaded from a CDN; degrade gracefully when it is unavailable (offline demo).
const hasPlotly = () => typeof window.Plotly !== "undefined";
function plot(id, data, layout, cfg) {
  const el = document.getElementById(id);
  if (!el) return;
  if (!hasPlotly()) { el.innerHTML = '<div class="empty">Chart library (Plotly CDN) not reachable — data loaded, chart skipped.</div>'; return; }
  window.Plotly.newPlot(id, data, layout, cfg);
}
const restyle = (id, u, i) => hasPlotly() && document.getElementById(id)?.data && window.Plotly.restyle(id, u, i);
const relayout = (id, u) => hasPlotly() && document.getElementById(id)?.layout && window.Plotly.relayout(id, u);
async function api(path, params = {}) {
  const qs = Object.entries(params).filter(([, v]) => v !== undefined && v !== null && v !== "")
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join("&");
  const res = await fetch(qs ? `${path}?${qs}` : path);
  if (!res.ok) {
    let detail = res.statusText;
    try { const j = await res.json(); detail = `${j.error}: ${j.detail}`; } catch (_) { /* ignore */ }
    throw new Error(`${res.status} ${detail}`);
  }
  return res.json();
}
async function apiPost(path, body) {
  const res = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  const j = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${res.status} ${j.error || ""}: ${j.detail || ""}`);
  return j;
}
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmt = (v, d = 3) => (v === null || v === undefined || Number.isNaN(v)) ? "–" : (typeof v === "number" ? (Number.isInteger(v) ? v.toLocaleString() : v.toFixed(d)) : esc(v));
const fmtTs = (s) => s ? esc(String(s).replace("T", " ").slice(0, 19)) : "–";
const badge = (v, cls) => `<span class="badge badge-${esc(cls ?? v)}">${esc(v)}</span>`;
function toast(msg, isError = false) {
  const el = document.createElement("div");
  el.className = `toast${isError ? " error" : ""}`; el.textContent = msg;
  document.getElementById("toasts").appendChild(el);
  setTimeout(() => el.remove(), 5000);
}
function setActive(name) {
  document.querySelectorAll("[data-nav]").forEach((a) => a.classList.toggle("active", a.dataset.nav === name));
}
function table(columns, rows, { onRow, empty = "No rows." } = {}) {
  if (!rows.length) return `<div class="empty">${esc(empty)}</div>`;
  const th = columns.map((c) => `<th class="${c.num ? "num" : ""}">${esc(c.label)}</th>`).join("");
  const body = rows.map((r, i) => `<tr class="${onRow ? "clickable" : ""}" data-i="${i}">${columns.map((c) =>
    `<td class="${c.num ? "num" : ""}">${c.render ? c.render(r) : fmt(r[c.key], c.digits)}</td>`).join("")}</tr>`).join("");
  return `<div class="table-wrap"><table><thead><tr>${th}</tr></thead><tbody>${body}</tbody></table></div>`;
}
function bindRows(container, rows, onRow) {
  container.querySelectorAll("tr.clickable").forEach((tr) => tr.addEventListener("click", () => onRow(rows[Number(tr.dataset.i)])));
}
function stat(k, v, u = "") { return `<div class="stat"><div class="k">${esc(k)}</div><div class="v">${v}<span class="u">${esc(u)}</span></div></div>`; }
const PLOT_LAYOUT = { margin: { l: 50, r: 16, t: 28, b: 40 }, paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", font: { size: 12 }, legend: { orientation: "h", y: -0.2 } };
const PLOT_CFG = { displaylogo: false, responsive: true };
// Stacked time-series panels use grid pattern "coupled" (one shared x axis, a y axis per row);
// traces pick their row with yaxis "y" / "y2" / "y3" ("y1" is not a Plotly axis id).

// ------------------------------------------------------------------ views
async function viewSessions(params) {
  setActive("sessions");
  const robots = (await api("/api/robots")).robots;
  const robotOpts = robots.map((r) => `<option value="${esc(r.robot_id)}" ${params.robot_id === r.robot_id ? "selected" : ""}>${esc(r.robot_id)} (${r.n_sessions})</option>`).join("");
  main.innerHTML = `
    <h1>Sessions</h1><div class="subtitle">Economy-of-motion metrics per robot session (sum over arms). Click a row for trajectory replay and KPIs.</div>
    <div class="grid cols-4" style="margin-bottom:16px">${stat("Robots", robots.length)}${stat("Sessions", robots.reduce((a, r) => a + r.n_sessions, 0))}
      ${stat("First seen", fmtTs(robots.map((r) => r.first_seen).sort()[0]))}${stat("Last seen", fmtTs(robots.map((r) => r.last_seen).sort().slice(-1)[0]))}</div>
    <form id="f" class="row">
      <label>Robot<select name="robot_id"><option value="">all</option>${robotOpts}</select></label>
      <label>From<input type="date" name="start" value="${esc(params.start || "")}"></label>
      <label>To<input type="date" name="end" value="${esc(params.end || "")}"></label>
      <button class="primary">Filter</button>
    </form><div id="list"></div>`;
  document.getElementById("f").addEventListener("submit", (e) => { e.preventDefault(); navigate("/sessions", Object.fromEntries(new FormData(e.target))); });
  const list = document.getElementById("list");
  const { sessions } = await api("/api/sessions", { robot_id: params.robot_id, start: params.start, end: params.end, limit: 500 });
  list.innerHTML = table([
    { key: "session_id", label: "Session", render: (r) => `<span class="mono">${esc(r.session_id)}</span>` },
    { key: "robot_id", label: "Robot" }, { key: "start_ts", label: "Start", render: (r) => fmtTs(r.start_ts) },
    { key: "duration_s", label: "Duration (s)", num: true, digits: 1 }, { key: "n_arms", label: "Arms", num: true },
    { key: "path_length_m", label: "Path (m)", num: true, digits: 2 }, { key: "mean_speed_mps", label: "Mean speed (m/s)", num: true, digits: 4 },
    { key: "idle_fraction", label: "Idle", num: true, digits: 2 }, { key: "jaw_cycles", label: "Jaw cycles", num: true },
    { key: "warn_events", label: "Warn", num: true }, { key: "error_events", label: "Err", num: true }, { key: "pipeline_version", label: "Pipeline" },
  ], sessions, { onRow: true, empty: "No sessions yet — run the simulator and the bag ingest pipeline (or `make local-pipeline`)." });
  bindRows(list, sessions, (r) => navigate(`/sessions/${r.session_id}`));
}

// The simulation's box, shown only for data from a system we can simulate. `video` is what it follows
// when there is one. Returns the transport, or null if there is nothing to show.
const SIM_CARD = `<div class="card" id="sim-card" hidden>
    <div class="toolbar"><strong>da Vinci simulation</strong><span class="muted small" id="sim-status"></span>
      <span class="seg" style="margin-left:auto"><button data-view="arms" class="ghost">Arms</button><button data-view="tools" class="ghost">Instruments</button></span></div>
    <div id="sim" class="sim-view"></div>
    <div class="toolbar" style="margin-top:8px"><button id="sim-play" class="ghost" aria-label="Play or pause">▶</button>
      <input type="range" id="sim-time" min="0" max="1" step="0.01" value="0" style="flex:1"><span class="muted small mono" id="sim-clock"></span></div>
    <div class="muted small" id="sim-note" style="margin-top:6px"></div></div>`;
async function mountSimulation(url, params, video, options) {
  const card = document.getElementById("sim-card");
  if (!card) return null;
  let data;
  try { data = await api(url, params); } catch (e) { card.remove(); toast(`Simulation: ${e.message}`, true); return null; }
  if (!data.system || !document.getElementById("sim-card")) { card.remove(); return null; }
  card.hidden = false;
  const box = document.getElementById("sim");
  document.getElementById("sim-note").textContent = data.note || "";
  try {
    const { createRobotSim, runTransport } = await import("/static/robot-sim.js");
    const sim = await createRobotSim(box, data);
    if (!box.isConnected) { sim.dispose(); return null; }
    box.robotSim = sim;
    card.querySelectorAll("[data-view]").forEach((b) => b.addEventListener("click", () => sim.setView(b.dataset.view)));
    return runTransport(sim, { root: box, play: document.getElementById("sim-play"), slider: document.getElementById("sim-time"),
      clock: document.getElementById("sim-clock"), status: document.getElementById("sim-status") }, video, options);
  } catch (e) {
    box.innerHTML = '<div class="empty">3-D library (three.js CDN) or model not reachable — simulation skipped.</div>';
    console.warn("simulation:", e);
    return null;
  }
}

async function viewSessionDetail(sessionId) {
  setActive("sessions");
  const s = await api(`/api/sessions/${encodeURIComponent(sessionId)}`);
  const arms = s.arms || [];
  main.innerHTML = `
    <div class="toolbar"><a href="#/sessions">← Sessions</a></div>
    <h1 class="mono">${esc(s.session_id)}</h1>
    <div class="subtitle">${esc(s.robot_id)} · ${fmtTs(s.start_ts)} → ${fmtTs(s.end_ts)} · bag ${esc(s.bag_file || "–")} · pipeline ${esc(s.pipeline_version)}</div>
    <div class="grid cols-4">${stat("Duration", fmt(s.duration_s, 1), "s")}${stat("Path length", fmt(s.path_length_m, 2), "m")}${stat("Idle fraction", fmt(s.idle_fraction, 2))}${stat("Jaw cycles", fmt(s.jaw_cycles))}</div>
    <div class="row" style="margin-top:14px"><label>Arm<select id="arm">${arms.map((a) => `<option>${esc(a)}</option>`).join("")}</select></label>
      <label>Downsample<select id="ds"><option value="1">1×</option><option value="2">2×</option><option value="5" selected>5×</option><option value="10">10×</option></select></label>
      <button id="reproc" class="ghost">Reprocess session</button></div>
    <div class="grid cols-2"><div class="card"><h2 style="margin-top:0">3-D tool-tip trajectory</h2><div id="traj3d" class="plot tall"></div></div>
      <div class="card"><h2 style="margin-top:0">Position, speed and jaw</h2><div id="trajts" class="plot tall"></div></div></div>
    <div class="card"><div class="toolbar"><strong>Replay</strong><span class="muted small" id="replay-t"></span></div><input type="range" id="replay" min="0" max="0" value="0"></div>
    ${SIM_CARD}
    <h2>Per-arm metrics</h2>${table([
      { key: "arm", label: "Arm" }, { key: "n_samples", label: "Samples", num: true }, { key: "path_length_m", label: "Path (m)", num: true, digits: 3 },
      { key: "mean_speed_mps", label: "Mean speed", num: true, digits: 4 }, { key: "idle_fraction", label: "Idle", num: true, digits: 2 },
      { key: "jaw_cycles", label: "Jaw cycles", num: true }, { key: "warn_events", label: "Warn", num: true }, { key: "error_events", label: "Err", num: true }, { key: "instrument_swaps", label: "Swaps", num: true }], s.per_arm || [])}
    <h2>1-minute KPIs</h2><div class="card"><div id="kpi" class="plot"></div></div>
    <h2>Events</h2>${table([{ key: "ts", label: "Time", render: (r) => fmtTs(r.ts) }, { key: "arm", label: "Arm" }, { key: "severity_name", label: "Severity", render: (r) => badge(r.severity_name) },
      { key: "code", label: "Code", render: (r) => `<span class="mono">${esc(r.code)}</span>` }, { key: "message", label: "Message" }, { key: "source", label: "Source", render: (r) => badge(r.source) }], s.events || [], { empty: "No events in this session." })}`;

  document.getElementById("reproc").addEventListener("click", async () => {
    try { const j = await apiPost("/api/jobs/reprocess", { kind: "session", target: s.session_id }); toast(`${j.status}: ${j.dag_id} (${j.note})`); }
    catch (e) { toast(e.message, true); }
  });
  const kpis = (await api(`/api/sessions/${encodeURIComponent(sessionId)}/kpis`)).kpis;
  if (kpis.length) {
    const byArm = {};
    kpis.forEach((k) => { (byArm[k.arm] ||= []).push(k); });
    plot("kpi", Object.entries(byArm).map(([arm, rows]) => ({ type: "bar", name: arm, x: rows.map((r) => r.window_start), y: rows.map((r) => r.path_length_m),
      hovertemplate: "%{x}<br>path %{y:.3f} m<br>" + rows.map((r) => `idle ${r.idle_fraction.toFixed(2)} · ${r.computed_by}`).join("<br>") })),
      { ...PLOT_LAYOUT, barmode: "group", yaxis: { title: "path length / min (m)" } }, PLOT_CFG);
  } else document.getElementById("kpi").innerHTML = '<div class="empty">No KPI windows.</div>';

  let simTransport = null;
  async function drawTrajectory() {
    const arm = document.getElementById("arm").value, ds = document.getElementById("ds").value;
    const { points } = await api(`/api/sessions/${encodeURIComponent(sessionId)}/trajectory`, { arm, downsample: ds });
    if (!points.length) { document.getElementById("traj3d").innerHTML = '<div class="empty">No samples for this arm.</div>'; return; }
    const t = points.map((p) => p.ts), x = points.map((p) => p.x), y = points.map((p) => p.y), z = points.map((p) => p.z);
    plot("traj3d", [{ type: "scatter3d", mode: "lines", x, y, z, line: { color: points.map((p) => p.speed), colorscale: "Viridis", width: 3 }, name: arm },
      { type: "scatter3d", mode: "markers", x: [x[0]], y: [y[0]], z: [z[0]], marker: { size: 6, color: "#d85a30" }, name: "cursor" }],
      { ...PLOT_LAYOUT, scene: { xaxis: { title: "x (m)" }, yaxis: { title: "y (m)" }, zaxis: { title: "z (m)" } } }, PLOT_CFG);
    plot("trajts", [
      { x: t, y: x, name: "x", yaxis: "y" }, { x: t, y, name: "y", yaxis: "y" }, { x: t, y: z, name: "z", yaxis: "y" },
      { x: t, y: points.map((p) => p.speed), name: "speed (m/s)", yaxis: "y2" }, { x: t, y: points.map((p) => p.jaw_angle), name: "jaw (rad)", yaxis: "y3" }],
      { ...PLOT_LAYOUT, grid: { rows: 3, columns: 1, pattern: "coupled", roworder: "top to bottom" }, yaxis: { title: "m" }, yaxis2: { title: "m/s" }, yaxis3: { title: "rad" },
        shapes: [{ type: "line", xref: "x", x0: t[0], x1: t[0], yref: "paper", y0: 0, y1: 1, line: { color: "#d85a30", width: 1.5 } }] }, PLOT_CFG);
    const slider = document.getElementById("replay"); slider.max = points.length - 1; slider.value = 0;
    slider.oninput = () => {
      const i = Number(slider.value);
      document.getElementById("replay-t").textContent = `${t[i]} · speed ${fmt(points[i].speed, 4)} m/s · jaw ${fmt(points[i].jaw_angle, 2)} rad`;
      restyle("traj3d", { x: [[x[i]]], y: [[y[i]]], z: [[z[i]]] }, [1]);
      relayout("trajts", { "shapes[0].x0": t[i], "shapes[0].x1": t[i] });
      simTransport?.seek((Date.parse(t[i]) - Date.parse(t[0])) / 1000);
    };
  }
  document.getElementById("arm").onchange = drawTrajectory; document.getElementById("ds").onchange = drawTrajectory;
  if (arms.length) drawTrajectory();
  // No video to follow: it runs on its own clock, about 3000 samples per arm.
  const perArm = (s.n_samples || 0) / Math.max(arms.length, 1);
  simTransport = await mountSimulation(`/api/sessions/${encodeURIComponent(sessionId)}/simulation`, { downsample: Math.max(1, Math.ceil(perArm / 3000)) }, null);
}

async function viewEvents(params) {
  setActive("events");
  main.innerHTML = `<h1>Log explorer</h1><div class="subtitle">System events from every robot — session boundaries, instrument swaps, clutch, joint-limit warnings, e-stops.</div>
    <form id="f" class="row"><label>Search<input name="q" placeholder="code or message" value="${esc(params.q || "")}"></label>
      <label>Robot<input name="robot_id" value="${esc(params.robot_id || "")}" placeholder="dv_001"></label>
      <label>Min severity<select name="severity_min">${["", 0, 1, 2, 3, 4].map((v) => `<option value="${v}" ${String(params.severity_min ?? "") === String(v) ? "selected" : ""}>${v === "" ? "any" : ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"][v]}</option>`).join("")}</select></label>
      <label>From<input type="date" name="start" value="${esc(params.start || "")}"></label><label>To<input type="date" name="end" value="${esc(params.end || "")}"></label>
      <button class="primary">Search</button></form><div id="list"></div>`;
  document.getElementById("f").addEventListener("submit", (e) => { e.preventDefault(); navigate("/events", Object.fromEntries(new FormData(e.target))); });
  const { events } = await api("/api/events", { ...params, limit: 500 });
  document.getElementById("list").innerHTML = table([
    { key: "ts", label: "Time", render: (r) => fmtTs(r.ts) }, { key: "robot_id", label: "Robot" }, { key: "arm", label: "Arm" },
    { key: "severity_name", label: "Severity", render: (r) => badge(r.severity_name) }, { key: "code", label: "Code", render: (r) => `<span class="mono">${esc(r.code)}</span>` },
    { key: "message", label: "Message" }, { key: "source", label: "Source", render: (r) => badge(r.source) }], events, { empty: "No events match." });
}

async function viewTrials(params) {
  setActive("trials");
  main.innerHTML = `<h1>Procedures (JIGSAWS)</h1><div class="subtitle">Trials with synchronized da Vinci kinematics and endoscope video. Click a trial to scrub video against kinematics.</div>
    <form id="f" class="row"><label>Task<select name="task"><option value="">all</option>${["Suturing", "Knot_Tying", "Needle_Passing"].map((t) => `<option ${params.task === t ? "selected" : ""}>${t}</option>`).join("")}</select></label>
      <label>Skill<select name="skill_level"><option value="">all</option>${["novice", "intermediate", "expert"].map((t) => `<option ${params.skill_level === t ? "selected" : ""}>${t}</option>`).join("")}</select></label>
      <label>Subject<input name="subject" value="${esc(params.subject || "")}" placeholder="B"></label><button class="primary">Filter</button></form><div id="list"></div>`;
  document.getElementById("f").addEventListener("submit", (e) => { e.preventDefault(); navigate("/trials", Object.fromEntries(new FormData(e.target))); });
  const { trials } = await api("/api/trials", params);
  const list = document.getElementById("list");
  list.innerHTML = table([
    { key: "trial_id", label: "Trial", render: (r) => `<span class="mono">${esc(r.trial_id)}</span>` }, { key: "task", label: "Task" }, { key: "subject", label: "Subject" },
    { key: "skill_level", label: "Skill", render: (r) => badge(r.skill_level, "accent") }, { key: "grs_total", label: "GRS", num: true }, { key: "duration_s", label: "Duration (s)", num: true, digits: 1 },
    { key: "n_frames", label: "Frames", num: true }, { key: "n_gestures", label: "Gestures", num: true }, { key: "sample_rate_hz", label: "Hz", num: true, digits: 0 }], trials,
    { onRow: true, empty: "No trials — run `rtdaas-jigsaws synth` and the JIGSAWS ingest (or `make local-pipeline`)." });
  bindRows(list, trials, (r) => navigate(`/trials/${r.trial_id}`));
}

async function viewTrialDetail(trialId, params) {
  setActive("trials");
  const t = await api(`/api/trials/${encodeURIComponent(trialId)}`);
  const fps = t.sample_rate_hz || 30;
  main.innerHTML = `<div class="toolbar"><a href="#/trials">← Trials</a></div>
    <h1 class="mono">${esc(t.trial_id)}</h1>
    <div class="subtitle">${esc(t.task)} · subject ${esc(t.subject)} · rep ${t.repetition} · ${badge(t.skill_level, "accent")} · GRS ${fmt(t.grs_total)} · ${fmt(t.duration_s, 1)} s · ${t.n_frames} frames @ ${fps} Hz</div>
    <div class="grid cols-2"><div class="card"><video id="video" controls preload="metadata" src="/api/trials/${encodeURIComponent(t.trial_id)}/video"></video>
        <div class="toolbar" style="margin-top:8px"><strong id="cur-gesture" class="badge badge-muted">–</strong><span class="muted small" id="cur-t">t = 0.00 s · frame 0</span><button id="reproc" class="ghost" style="margin-left:auto">Reprocess trial</button></div>
        <div class="ribbon" id="ribbon"></div><input type="range" id="scrub" min="0" max="${Math.max(0, (t.n_frames || 1) - 1)}" value="0"></div>
      ${SIM_CARD}</div>
    <div class="card"><h2 style="margin-top:0">Kinematics (PSM left/right)</h2><div id="kin" class="plot tall"></div></div>
    <div class="card"><h2 style="margin-top:0">Video frame features</h2><div id="frames" class="plot"></div></div>
    <h2>Gesture segments</h2>${table([{ key: "gesture", label: "Gesture", render: (r) => badge(r.gesture, "accent") }, { key: "start_frame", label: "Start", num: true }, { key: "end_frame", label: "End", num: true },
      { key: "duration_s", label: "Duration (s)", num: true, digits: 2 }, { key: "path_length_psml", label: "Path L (m)", num: true }, { key: "path_length_psmr", label: "Path R (m)", num: true },
      { key: "mean_speed_psml", label: "Speed L", num: true, digits: 4 }, { key: "mean_speed_psmr", label: "Speed R", num: true, digits: 4 }, { key: "gripper_cycles", label: "Gripper", num: true },
      { key: "motion_energy_mean", label: "Video motion", num: true, digits: 4 }], t.gestures || [], { onRow: true, empty: "No gesture segments." })}`;

  document.getElementById("reproc").addEventListener("click", async () => {
    try { const j = await apiPost("/api/jobs/reprocess", { kind: "trial", target: t.trial_id }); toast(`${j.status}: ${j.dag_id} (${j.note})`); }
    catch (e) { toast(e.message, true); }
  });
  const video = document.getElementById("video"), scrub = document.getElementById("scrub");
  // 15 Hz is plenty to interpolate between, and keeps the response small on long trials.
  mountSimulation(`/api/trials/${encodeURIComponent(t.trial_id)}/simulation`, { downsample: (t.n_frames || 0) > 3000 ? 2 : 1 }, video);
  const gestures = t.gestures || [];
  const colorOf = (g) => GESTURE_COLORS[(parseInt(String(g).replace(/\D/g, ""), 10) || 0) % GESTURE_COLORS.length];
  const ribbon = document.getElementById("ribbon");
  const nFrames = Math.max(t.n_frames || 1, 1);
  let cursor = 0, segs = [];
  gestures.slice().sort((a, b) => a.start_frame - b.start_frame).forEach((g) => {
    if (g.start_frame - 1 > cursor) segs.push({ start: cursor, end: g.start_frame - 1, gesture: null });
    segs.push({ start: g.start_frame - 1, end: g.end_frame, gesture: g.gesture }); cursor = g.end_frame;
  });
  if (cursor < nFrames) segs.push({ start: cursor, end: nFrames, gesture: null });
  ribbon.innerHTML = segs.map((s) => `<div class="${s.gesture ? "" : "none"}" style="flex:${Math.max(s.end - s.start, 1)};background:${s.gesture ? colorOf(s.gesture) : ""}" title="${s.gesture || "unlabelled"} frames ${s.start}-${s.end}" data-f="${s.start}">${s.gesture || ""}</div>`).join("");
  ribbon.querySelectorAll("div").forEach((d) => d.addEventListener("click", () => { video.currentTime = Number(d.dataset.f) / fps; }));
  const gestureAt = (frame) => { const g = gestures.find((x) => frame + 1 >= x.start_frame && frame + 1 <= x.end_frame); return g ? g.gesture : null; };

  const { samples } = await api(`/api/trials/${encodeURIComponent(trialId)}/kinematics`, { arms: "psml,psmr", downsample: nFrames > 3000 ? 3 : 1 });
  const { frames } = await api(`/api/trials/${encodeURIComponent(trialId)}/frames`);
  const ts = samples.map((s) => s.t_s);
  const sp = (arm) => samples.map((s) => Math.hypot(s[`${arm}_vx`] || 0, s[`${arm}_vy`] || 0, s[`${arm}_vz`] || 0));
  const cursorShape = (x) => [{ type: "line", xref: "x", x0: x, x1: x, yref: "paper", y0: 0, y1: 1, line: { color: "#d85a30", width: 1.5 } }];
  if (samples.length) {
    plot("kin", [
      { x: ts, y: samples.map((s) => s.psml_x), name: "PSML x", yaxis: "y" }, { x: ts, y: samples.map((s) => s.psml_y), name: "PSML y", yaxis: "y" }, { x: ts, y: samples.map((s) => s.psml_z), name: "PSML z", yaxis: "y" },
      { x: ts, y: samples.map((s) => s.psmr_x), name: "PSMR x", yaxis: "y", line: { dash: "dot" } }, { x: ts, y: samples.map((s) => s.psmr_y), name: "PSMR y", yaxis: "y", line: { dash: "dot" } }, { x: ts, y: samples.map((s) => s.psmr_z), name: "PSMR z", yaxis: "y", line: { dash: "dot" } },
      { x: ts, y: sp("psml"), name: "speed L", yaxis: "y2" }, { x: ts, y: sp("psmr"), name: "speed R", yaxis: "y2" },
      { x: ts, y: samples.map((s) => s.psml_gripper), name: "gripper L", yaxis: "y3" }, { x: ts, y: samples.map((s) => s.psmr_gripper), name: "gripper R", yaxis: "y3" }],
      { ...PLOT_LAYOUT, grid: { rows: 3, columns: 1, pattern: "coupled", roworder: "top to bottom" }, yaxis: { title: "position (m)" }, yaxis2: { title: "speed (m/s)" }, yaxis3: { title: "gripper" }, xaxis: { title: "t (s)" }, shapes: cursorShape(0) }, PLOT_CFG);
  } else document.getElementById("kin").innerHTML = '<div class="empty">No kinematics.</div>';
  if (frames.length) {
    plot("frames", [{ x: frames.map((f) => f.t_s), y: frames.map((f) => f.motion_energy), name: "motion energy" }, { x: frames.map((f) => f.t_s), y: frames.map((f) => f.sharpness), name: "sharpness", yaxis: "y2" }],
      { ...PLOT_LAYOUT, yaxis: { title: "motion energy" }, yaxis2: { title: "sharpness", overlaying: "y", side: "right" }, xaxis: { title: "t (s)" }, shapes: cursorShape(0) }, PLOT_CFG);
  } else document.getElementById("frames").innerHTML = '<div class="empty">No frame features (video not processed).</div>';

  function setCursor(tSec) {
    const frame = Math.min(nFrames - 1, Math.max(0, Math.round(tSec * fps)));
    scrub.value = frame;
    document.getElementById("cur-t").textContent = `t = ${tSec.toFixed(2)} s · frame ${frame}`;
    const g = gestureAt(frame); const el = document.getElementById("cur-gesture");
    el.textContent = g || "unlabelled"; el.style.background = g ? colorOf(g) : ""; el.style.color = g ? "#fff" : "";
    if (samples.length) relayout("kin", { "shapes[0].x0": tSec, "shapes[0].x1": tSec });
    if (frames.length) relayout("frames", { "shapes[0].x0": tSec, "shapes[0].x1": tSec });
  }
  video.addEventListener("timeupdate", () => setCursor(video.currentTime));
  video.addEventListener("error", () => toast("Video could not be played (file missing, or codec not supported by this browser)", true));
  scrub.addEventListener("input", () => { video.currentTime = Number(scrub.value) / fps; setCursor(Number(scrub.value) / fps); });
  main.querySelectorAll("tr.clickable").forEach((tr, i) => tr.addEventListener("click", () => { video.currentTime = (gestures[i].start_frame - 1) / fps; }));
  if (params.t) { video.addEventListener("loadedmetadata", () => { video.currentTime = Number(params.t); }, { once: true }); setCursor(Number(params.t)); }
}

async function viewGestures(params) {
  setActive("gestures");
  main.innerHTML = `<h1>Gesture search</h1><div class="subtitle">Find gesture segments across trials by task, gesture code and skill level; click to jump into the trial at that frame.</div>
    <form id="f" class="row"><label>Task<select name="task"><option value="">all</option>${["Suturing", "Knot_Tying", "Needle_Passing"].map((t) => `<option ${params.task === t ? "selected" : ""}>${t}</option>`).join("")}</select></label>
      <label>Gesture<input name="gesture" value="${esc(params.gesture || "")}" placeholder="G4"></label>
      <label>Skill<select name="skill_level"><option value="">all</option>${["novice", "intermediate", "expert"].map((t) => `<option ${params.skill_level === t ? "selected" : ""}>${t}</option>`).join("")}</select></label>
      <button class="primary">Search</button></form><div id="summary" class="card compact"></div><div id="list"></div>`;
  document.getElementById("f").addEventListener("submit", (e) => { e.preventDefault(); navigate("/gestures", Object.fromEntries(new FormData(e.target))); });
  const { gestures } = await api("/api/gestures", { ...params, limit: 2000 });
  const bySkill = {};
  gestures.forEach((g) => { const s = (bySkill[g.skill_level] ||= { n: 0, d: 0, p: 0 }); s.n++; s.d += g.duration_s; s.p += (g.path_length_psml || 0) + (g.path_length_psmr || 0); });
  document.getElementById("summary").innerHTML = `<strong>${gestures.length}</strong> segments · ` + Object.entries(bySkill).map(([k, v]) => `${esc(k)}: ${v.n} (mean ${(v.d / v.n).toFixed(2)} s, path ${(v.p / v.n).toFixed(3)} m)`).join(" · ");
  const list = document.getElementById("list");
  list.innerHTML = table([{ key: "trial_id", label: "Trial", render: (r) => `<span class="mono">${esc(r.trial_id)}</span>` }, { key: "task", label: "Task" }, { key: "gesture", label: "Gesture", render: (r) => badge(r.gesture, "accent") },
    { key: "skill_level", label: "Skill" }, { key: "start_frame", label: "Start", num: true }, { key: "end_frame", label: "End", num: true }, { key: "duration_s", label: "Duration (s)", num: true, digits: 2 },
    { key: "path_length_psml", label: "Path L", num: true }, { key: "path_length_psmr", label: "Path R", num: true }, { key: "gripper_cycles", label: "Gripper", num: true }, { key: "motion_energy_mean", label: "Video motion", num: true, digits: 4 }],
    gestures, { onRow: true, empty: "No gesture segments match." });
  bindRows(list, gestures, (g) => navigate(`/trials/${g.trial_id}`, { t: ((g.start_frame - 1) / 30).toFixed(2) }));
}

async function viewCatalog() {
  setActive("catalog");
  const { tables } = await api("/api/catalog");
  main.innerHTML = `<h1>Data catalog</h1><div class="subtitle">Every published table with its owner, freshness target, contract, columns and freshness. Numbers come from the last pipeline run.</div>
    <div class="grid cols-2">${tables.map((t) => `<div class="card">
      <div class="toolbar"><strong class="mono">${esc(t.name)}</strong><span style="margin-left:auto">${t.freshness_target_hours
        ? `<span class="fresh-dot ${t.freshness_ok ? "ok" : "stale"}"></span><span class="small muted" title="The newest row should be at most ${fmt(t.freshness_target_hours, 0)} h old">${t.freshness_ok ? "fresh" : "stale"} · target ${fmt(t.freshness_target_hours, 0)} h</span>`
        : '<span class="small muted">no freshness target</span>'}</span></div>
      <p class="small" style="margin:6px 0 8px">${esc(t.description)}</p>
      <div class="kv"><span class="k">Owner</span><span>${esc(t.owner)}</span><span class="k">Contract</span><span class="mono">${esc(t.contract)}</span><span class="k">Rows</span><span>${fmt(t.row_count)}</span><span class="k">Updated</span><span>${fmtTs(t.last_updated)}</span></div>
      <details style="margin-top:8px"><summary>${(t.columns || []).length} columns</summary><table style="margin-top:6px">${(t.columns || []).map((c) => `<tr><td class="mono">${esc(c.name)}</td><td class="muted">${esc(c.type)}</td><td>${esc(c.description || "")}</td></tr>`).join("")}</table></details>
      <div style="margin-top:8px"><a href="#/quality?table_name=${encodeURIComponent(t.name)}">quality results →</a></div></div>`).join("") || '<div class="empty">Catalog is empty — run the pipeline.</div>'}</div>`;
}

async function viewQuality(params) {
  setActive("quality");
  const { runs } = await api("/api/quality/runs", { limit: 30 });
  const { results } = await api("/api/quality/results", { run_id: params.run_id, table_name: params.table_name, status: params.status, limit: 2000 });
  const counts = { pass: 0, fail: 0, error: 0, skipped: 0 }; results.forEach((r) => { counts[r.status] = (counts[r.status] || 0) + 1; });
  const blocking = results.filter((r) => r.status === "fail" && r.severity === "blocking").length;
  main.innerHTML = `<h1>Data quality</h1><div class="subtitle">Check results from quality gates and the daily DQ run. Blocking failures stop publishing.</div>
    <div class="grid cols-4">${stat("Pass", counts.pass)}${stat("Warnings", counts.fail - blocking)}${stat("Blocking failures", blocking)}${stat("Errors", counts.error)}</div>
    <h2>Runs</h2>${table([{ key: "run_ts", label: "Run", render: (r) => fmtTs(r.run_ts) }, { key: "run_id", label: "Run id", render: (r) => `<a href="#/quality?run_id=${esc(r.run_id)}" class="mono">${esc(r.run_id)}</a>` }, { key: "suite", label: "Suite" },
      { key: "partition_value", label: "Partition" }, { key: "n_checks", label: "Checks", num: true }, { key: "n_pass", label: "Pass", num: true }, { key: "n_warn_fail", label: "Warn", num: true }, { key: "n_blocking_fail", label: "Blocking", num: true, render: (r) => r.n_blocking_fail ? badge(r.n_blocking_fail, "fail") : "0" }], runs, { empty: "No runs yet." })}
    <h2>Results</h2><form id="f" class="row"><label>Run id<input name="run_id" value="${esc(params.run_id || "")}"></label><label>Table<input name="table_name" value="${esc(params.table_name || "")}" placeholder="telemetry.sessions"></label>
      <label>Status<select name="status"><option value="">all</option>${["pass", "fail", "error", "skipped"].map((s) => `<option ${params.status === s ? "selected" : ""}>${s}</option>`).join("")}</select></label><button class="primary">Filter</button></form>
    ${table([{ key: "run_ts", label: "Time", render: (r) => fmtTs(r.run_ts) }, { key: "table_name", label: "Table", render: (r) => `<span class="mono">${esc(r.table_name)}</span>` }, { key: "check_name", label: "Check" },
      { key: "severity", label: "Severity", render: (r) => badge(r.severity) }, { key: "status", label: "Status", render: (r) => badge(r.status) }, { key: "observed", label: "Observed", num: true },
      { key: "threshold", label: "Threshold" }, { key: "message", label: "Message" }], results, { empty: "No results match." })}`;
  document.getElementById("f").addEventListener("submit", (e) => { e.preventDefault(); navigate("/quality", Object.fromEntries(new FormData(e.target))); });
}

async function viewContracts() {
  setActive("contracts");
  const { contracts } = await api("/api/contracts");
  main.innerHTML = `<h1>Data contracts</h1><div class="subtitle">Kafka subjects with their Avro envelope schemas, generated from the ROS 2 message definitions. Compatibility mode: BACKWARD.</div>
    ${contracts.map((c) => `<div class="card"><div class="toolbar"><strong class="mono">${esc(c.subject)}</strong>${c.version != null ? badge(`v${c.version}`, "accent") : badge("not registered", "muted")}${c.schema_id != null ? `<span class="small muted">schema id ${c.schema_id}</span>` : ""}</div>
      <table style="margin:8px 0"><thead><tr><th>Field</th><th>Type</th></tr></thead><tbody>${(c.fields || []).map((f) => `<tr><td class="mono">${esc(f.path)}</td><td class="muted">${esc(f.type)}</td></tr>`).join("")}</tbody></table>
      <details><summary>Avro schema</summary><pre class="schema">${esc(JSON.stringify(c.schema, null, 2))}</pre></details></div>`).join("") || '<div class="empty">No contracts found.</div>'}`;
}

async function viewJobs() {
  setActive("jobs");
  const { jobs } = await api("/api/jobs", { limit: 200 });
  main.innerHTML = `<h1>Jobs</h1><div class="subtitle">Reprocess requests sent to Airflow from the UI or the API.</div>
    <div class="card compact"><form id="f" class="row" style="margin:0"><label>Kind<select name="kind"><option>date</option><option>session</option><option>trial</option></select></label><label>Target<input name="target" placeholder="2026-10-28" required></label><button class="primary">Reprocess</button></form></div>
    ${table([{ key: "requested_at", label: "Requested", render: (r) => fmtTs(r.requested_at) }, { key: "kind", label: "Kind" }, { key: "target", label: "Target", render: (r) => `<span class="mono">${esc(r.target)}</span>` },
      { key: "status", label: "Status", render: (r) => badge(r.status) }, { key: "dag_run_id", label: "DAG run", render: (r) => `<span class="mono">${esc(r.dag_run_id || "–")}</span>` }], jobs, { empty: "No jobs yet." })}`;
  document.getElementById("f").addEventListener("submit", async (e) => {
    e.preventDefault(); const body = Object.fromEntries(new FormData(e.target));
    try { const j = await apiPost("/api/jobs/reprocess", body); toast(`${j.status}: ${j.dag_id} (${j.note})`); viewJobs(); } catch (err) { toast(err.message, true); }
  });
}

// ------------------------------------------------------------------ router
function navigate(path, params = {}) {
  const qs = Object.entries(params).filter(([, v]) => v !== undefined && v !== "").map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join("&");
  location.hash = qs ? `#${path}?${qs}` : `#${path}`;
}
async function route() {
  const hash = location.hash.replace(/^#/, "") || "/sessions";
  const [path, qs] = hash.split("?");
  const params = Object.fromEntries(new URLSearchParams(qs || ""));
  const parts = path.split("/").filter(Boolean);
  try {
    if (parts[0] === "sessions" && parts[1]) await viewSessionDetail(decodeURIComponent(parts[1]));
    else if (parts[0] === "sessions") await viewSessions(params);
    else if (parts[0] === "events") await viewEvents(params);
    else if (parts[0] === "trials" && parts[1]) await viewTrialDetail(decodeURIComponent(parts[1]), params);
    else if (parts[0] === "trials") await viewTrials(params);
    else if (parts[0] === "gestures") await viewGestures(params);
    else if (parts[0] === "catalog") await viewCatalog();
    else if (parts[0] === "quality") await viewQuality(params);
    else if (parts[0] === "contracts") await viewContracts();
    else if (parts[0] === "jobs") await viewJobs();
    else navigate("/sessions");
  } catch (e) {
    main.innerHTML = `<div class="error-box">Failed to load this page: ${esc(e.message)}</div>`;
  }
}
async function health() {
  const el = document.getElementById("health-badge");
  try { const h = await api("/api/health"); el.textContent = `${h.backend} · v${h.version}`; el.className = "badge badge-ok"; }
  catch (e) { el.textContent = "API down"; el.className = "badge badge-fail"; }
}
window.addEventListener("hashchange", route);
route(); health();
