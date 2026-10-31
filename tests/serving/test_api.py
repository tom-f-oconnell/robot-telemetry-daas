from __future__ import annotations

import io
import re
from types import SimpleNamespace

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


def test_live_does_not_touch_the_backend(client, monkeypatch):
    backend = client.app.state.backend
    monkeypatch.setattr(backend, "health", lambda: pytest.fail("/api/live called the backend"))
    assert client.get("/api/live").json() == {"status": "ok"}


def test_backend_routes_do_not_run_on_the_event_loop(client):
    """A blocking call in an ``async def`` handler would stall every request, probes included."""
    import inspect

    endpoints = {r.path: r.endpoint for r in client.app.routes if hasattr(r, "endpoint")}
    for path in ("/api/health", "/api/catalog", "/api/sessions", "/api/events", "/api/trials"):
        assert not inspect.iscoroutinefunction(endpoints[path]), path


def test_resolve_media_object_store_uri(tmp_path):
    from rtdaas.serving.app import resolve_media

    video = tmp_path / "jigsaws" / "Suturing" / "video" / "Suturing_B001_capture1.mp4"
    video.parent.mkdir(parents=True)
    video.write_bytes(b"\0")
    uri = "s3a://rtdaas-raw/jigsaws/Suturing/video/Suturing_B001_capture1.mp4"
    assert resolve_media(uri, tmp_path) == video
    assert resolve_media("s3://rtdaas-raw/jigsaws/missing.mp4", tmp_path) is None
    assert resolve_media("s3a://rtdaas-raw/../../etc/passwd", tmp_path) is None
    assert resolve_media("s3a://rtdaas-raw", tmp_path) is None


class FakeS3:
    """The parts of a boto3 S3 client the video route uses, with its 404 and Range behaviour."""

    def __init__(self, objects):
        self.objects = objects
        self.gets = []

    def head_object(self, Bucket, Key):
        from botocore.exceptions import ClientError

        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"ContentLength": len(self.objects[(Bucket, Key)])}

    def get_object(self, Bucket, Key, Range):
        from botocore.response import StreamingBody

        self.gets.append(Range)
        start, end = map(int, re.fullmatch(r"bytes=(\d+)-(\d+)", Range).groups())
        data = self.objects[(Bucket, Key)][start : end + 1]
        return {"Body": StreamingBody(io.BytesIO(data), len(data))}


VIDEO_BYTES = bytes(range(100))
VIDEO_URI = "s3a://rtdaas-raw/jigsaws/Suturing/video/Suturing_B001_capture1.mp4"


@pytest.fixture
def object_store_app(tmp_path, monkeypatch):
    """The API on Kubernetes: a trial whose video is in the bucket, and nothing under MEDIA_ROOT."""
    from starlette.testclient import TestClient

    from rtdaas.config import Settings
    from rtdaas.serving import app as app_module

    fake = FakeS3({("rtdaas-raw", VIDEO_URI.split("rtdaas-raw/")[1]): VIDEO_BYTES})
    monkeypatch.setattr(app_module, "make_s3_client", lambda settings: fake)
    paths = {"T1": VIDEO_URI, "gone": "s3a://rtdaas-raw/jigsaws/gone.mp4", "local": "gone.mp4"}
    backend = SimpleNamespace(trial=lambda t: {"trial_id": t, "video_path": paths.get(t)})
    settings = Settings(serving_backend="sqlite", media_root=str(tmp_path))
    with TestClient(app_module.create_app(settings, backend=backend)) as c:
        yield c, fake, tmp_path


def test_video_streams_from_object_store(object_store_app):
    client, _, _ = object_store_app
    full = client.get("/api/trials/T1/video")
    assert full.status_code == 200 and full.content == VIDEO_BYTES
    assert full.headers["accept-ranges"] == "bytes"
    assert full.headers["content-length"] == "100"
    assert full.headers["content-type"] == "video/mp4"


@pytest.mark.parametrize(
    "header,content_range,expected",
    [
        ("bytes=10-19", "bytes 10-19/100", VIDEO_BYTES[10:20]),
        ("bytes=95-", "bytes 95-99/100", VIDEO_BYTES[95:]),
        ("bytes=-5", "bytes 95-99/100", VIDEO_BYTES[95:]),
        ("bytes=90-500", "bytes 90-99/100", VIDEO_BYTES[90:]),
    ],
)
def test_video_object_store_range(object_store_app, header, content_range, expected):
    client, _, _ = object_store_app
    part = client.get("/api/trials/T1/video", headers={"Range": header})
    assert part.status_code == 206 and part.content == expected
    assert part.headers["content-range"] == content_range
    assert part.headers["content-length"] == str(len(expected))
    assert part.headers["content-type"] == "video/mp4"


def test_video_object_store_unsatisfiable_range(object_store_app):
    client, _, _ = object_store_app
    bad = client.get("/api/trials/T1/video", headers={"Range": "bytes=100-"})
    assert bad.status_code == 416 and bad.headers["content-range"] == "bytes */100"


def test_video_object_store_head_does_not_download(object_store_app):
    client, fake, _ = object_store_app
    head = client.head("/api/trials/T1/video")
    assert head.status_code == 200 and head.headers["content-length"] == "100"
    assert fake.gets == []


def test_video_missing_everywhere_is_404(object_store_app):
    client, _, _ = object_store_app
    assert client.get("/api/trials/gone/video").status_code == 404
    assert client.get("/api/trials/local/video").status_code == 404


def test_video_prefers_local_mirror(object_store_app):
    client, fake, media_root = object_store_app
    mirrored = media_root / VIDEO_URI.split("rtdaas-raw/")[1]
    mirrored.parent.mkdir(parents=True)
    mirrored.write_bytes(b"local")
    assert client.get("/api/trials/T1/video").content == b"local"
    assert fake.gets == []


def test_video_object_store_failure_is_502(object_store_app, monkeypatch):
    from botocore.exceptions import ConnectTimeoutError

    client, fake, _ = object_store_app

    def unreachable(**kwargs):
        raise ConnectTimeoutError(endpoint_url="http://minio:9000")

    monkeypatch.setattr(fake, "head_object", unreachable)
    response = client.get("/api/trials/T1/video")
    assert response.status_code == 502 and response.json()["error"] == "object_store_error"


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


def test_to_iso_handles_dates_and_timestamps():
    import datetime as dt

    from rtdaas.serving.backends._sqlbase import to_iso

    assert to_iso(dt.date(2026, 10, 29)) == "2026-10-29"
    assert to_iso(dt.datetime(2026, 10, 29, 12, tzinfo=dt.UTC)).startswith("2026-10-29T12:00:00")
    assert to_iso(3) == 3
