from __future__ import annotations

import re

import pytest
import yaml

pytest.importorskip("starlette", reason="serving tests need the 'api' extra")

from rtdaas.serving.app import OPENAPI_PATH, documented_routes  # noqa: E402


def test_health(client):
    r = client.get("/api/health")
    assert r.status_code == 200
    body = r.json()
    assert body["backend"] == "sqlite" and "telemetry.sessions" in body["tables"]


def test_catalog_and_contracts(client):
    tables = client.get("/api/catalog").json()["tables"]
    names = {t["name"] for t in tables}
    assert {"telemetry.sessions", "procedures.gestures", "dq.check_results"} <= names
    assert all("owner" in t and "columns" in t for t in tables)
    contracts = client.get("/api/contracts").json()["contracts"]
    assert {c["subject"] for c in contracts} == {
        "rtdaas.telemetry.joint_states-value",
        "rtdaas.telemetry.tool_pose-value",
        "rtdaas.telemetry.events-value",
        "rtdaas.telemetry.heartbeat-value",
    }
    assert all(c["schema"]["type"] == "record" for c in contracts)


def test_sessions_and_detail(client):
    robots = client.get("/api/robots").json()["robots"]
    assert len(robots) == 1 and robots[0]["n_sessions"] >= 1
    sessions = client.get("/api/sessions", params={"robot_id": robots[0]["robot_id"]}).json()[
        "sessions"
    ]
    assert sessions and sessions[0]["n_arms"] == 3
    sid = sessions[0]["session_id"]
    detail = client.get(f"/api/sessions/{sid}").json()
    assert {a["arm"] for a in detail["per_arm"]} == {"PSM1", "PSM2", "ECM"}
    assert any(e["code"] == "SESSION_START" for e in detail["events"])
    assert client.get("/api/sessions/does-not-exist").status_code == 404
    assert client.get("/api/sessions", params={"start": "not-a-date"}).status_code == 400
    assert client.get("/api/sessions", params={"limit": "0"}).status_code == 400
    assert client.get("/api/sessions", params={"start": "2027-01-01"}).json()["sessions"] == []


def test_trajectory_and_kpis(client):
    sid = client.get("/api/sessions").json()["sessions"][0]["session_id"]
    full = client.get(f"/api/sessions/{sid}/trajectory", params={"arm": "PSM1"}).json()
    ds = client.get(
        f"/api/sessions/{sid}/trajectory", params={"arm": "PSM1", "downsample": 4}
    ).json()
    assert full["count"] > 100 and abs(ds["count"] - full["count"] / 4) <= 1
    assert {"ts", "x", "y", "z", "jaw_angle", "speed"} <= set(full["points"][0])
    assert client.get(f"/api/sessions/{sid}/trajectory", params={"arm": "NOPE"}).status_code == 400
    kpis = client.get(f"/api/sessions/{sid}/kpis").json()["kpis"]
    assert kpis and kpis[0]["computed_by"] == "batch"


def test_events(client):
    r = client.get("/api/events", params={"q": "SESSION", "severity_min": 1})
    assert r.status_code == 200
    codes = {e["code"] for e in r.json()["events"]}
    assert "SESSION_START" in codes
    assert client.get("/api/events", params={"severity_min": "x"}).status_code == 400


def test_trials_kinematics_frames(client):
    trials = client.get("/api/trials", params={"task": "Suturing"}).json()["trials"]
    assert len(trials) == 1 and trials[0]["task"] == "Suturing"
    tid = trials[0]["trial_id"]
    detail = client.get(f"/api/trials/{tid}").json()
    assert detail["gestures"] and detail["n_frames"] > 0
    kin = client.get(
        f"/api/trials/{tid}/kinematics", params={"arms": "psml", "downsample": 2}
    ).json()
    assert (
        kin["arms"] == ["psml"]
        and "psml_x" in kin["samples"][0]
        and "psmr_x" not in kin["samples"][0]
    )
    assert client.get(f"/api/trials/{tid}/kinematics", params={"arms": "nope"}).status_code == 400
    frames = client.get(f"/api/trials/{tid}/frames").json()["frames"]
    assert frames and {"motion_energy", "sharpness", "mean_intensity"} <= set(frames[0])
    assert client.get("/api/trials/missing").status_code == 404


def test_video_range(client):
    tid = client.get("/api/trials").json()["trials"][0]["trial_id"]
    full = client.get(f"/api/trials/{tid}/video")
    assert full.status_code == 200 and full.headers["accept-ranges"] == "bytes"
    part = client.get(f"/api/trials/{tid}/video", headers={"Range": "bytes=0-9"})
    assert part.status_code == 206 and len(part.content) == 10
    assert part.headers["content-range"].startswith("bytes 0-9/")
    bad = client.get(f"/api/trials/{tid}/video", headers={"Range": "bytes=999999999-"})
    assert bad.status_code == 416


def test_gestures_and_quality(client):
    g = client.get("/api/gestures", params={"task": "Suturing"}).json()["gestures"]
    assert g and g[0]["task"] == "Suturing" and re.match(r"G\d+", g[0]["gesture"])
    runs = client.get("/api/quality/runs").json()["runs"]
    assert runs and runs[0]["n_checks"] > 0
    results = client.get("/api/quality/results", params={"status": "pass"}).json()["results"]
    assert results and all(r["status"] == "pass" for r in results)
    assert client.get("/api/quality/results", params={"status": "maybe"}).status_code == 400


def test_reprocess_without_airflow(client):
    sid = client.get("/api/sessions").json()["sessions"][0]["session_id"]
    r = client.post("/api/jobs/reprocess", json={"kind": "session", "target": sid})
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "queued_local" and body["dag_id"] == "telemetry_backfill"
    assert body["conf"]["start_date"] == "2026-10-28"
    assert (
        client.post("/api/jobs/reprocess", json={"kind": "date", "target": "yesterday"}).status_code
        == 400
    )
    assert (
        client.post("/api/jobs/reprocess", json={"kind": "trial", "target": "nope"}).status_code
        == 404
    )
    assert client.post("/api/jobs/reprocess", content=b"not json").status_code == 400
    jobs = client.get("/api/jobs").json()["jobs"]
    assert jobs and jobs[0]["target"] == sid


def test_meta_routes(client):
    assert client.get("/").status_code == 200
    assert client.get("/docs").status_code == 200
    assert client.get("/openapi.json").json()["openapi"].startswith("3.0")
    m = client.get("/metrics")
    assert m.status_code == 200 and "rtdaas_api_requests_total" in m.text
    assert client.get("/static/app.js").status_code == 200
    assert client.get("/api/nope").status_code == 404


def test_openapi_parity(client):
    doc = yaml.safe_load(OPENAPI_PATH.read_text())
    documented = {(m, p) for p, ops in doc["paths"].items() for m in ops}
    mounted = {(m, re.sub(r"\{[^}]+\}", "{id}", p)) for m, p in documented_routes(client.app)}
    documented = {(m, re.sub(r"\{[^}]+\}", "{id}", p)) for m, p in documented}
    assert mounted == documented
