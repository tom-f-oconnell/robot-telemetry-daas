"""``SERVE_JIGSAWS=false``: no JIGSAWS data from any route, and what labels the rest when it is on."""

from __future__ import annotations

import json
import re

import pytest
import yaml

pytest.importorskip("starlette", reason="serving tests need the 'api' extra")

from starlette.testclient import TestClient  # noqa: E402

from rtdaas.config import Settings  # noqa: E402
from rtdaas.serving.app import OPENAPI_PATH, STATIC_DIR, create_app, documented_routes  # noqa: E402
from rtdaas.serving.jigsaws import JIGSAWS_TABLES, without_jigsaws_openapi  # noqa: E402

# Anything here in a response means JIGSAWS data got out. The fixture holds one trial per task.
LEAK = re.compile(
    r"Suturing|Knot_Tying|Needle_Passing|procedures|skill_level|grs_total|gesture", re.I
)
ONLY_JIGSAWS_ROUTES = (
    "/api/trials",
    "/api/trials/Suturing_B001",
    "/api/trials/Suturing_B001/kinematics",
    "/api/trials/Suturing_B001/frames",
    "/api/trials/Suturing_B001/video",
    "/api/trials/Suturing_B001/simulation",
    "/api/gestures",
)


def _client(fixture_db, serve_jigsaws):
    settings = Settings(
        serving_backend="sqlite",
        serving_sqlite_path=str(fixture_db["db"]),
        media_root=str(fixture_db["root"]),
        airflow_api_url="http://127.0.0.1:1/api/v1",
        serve_jigsaws=serve_jigsaws,
    )
    return TestClient(create_app(settings))


@pytest.fixture
def hidden(fixture_db):
    with _client(fixture_db, False) as c:
        yield c


@pytest.fixture
def shown(fixture_db):
    with _client(fixture_db, True) as c:
        yield c


@pytest.mark.parametrize("path", ONLY_JIGSAWS_ROUTES)
def test_routes_that_only_serve_jigsaws_are_gone(shown, hidden, path):
    assert shown.get(path).status_code != 404 or path.endswith("/video")
    assert hidden.get(path).status_code == 404


def test_no_route_serves_jigsaws_data_when_it_is_off(shown, hidden):
    """A sweep of everything documented, so a route added later is caught without a new test."""
    session = hidden.get("/api/sessions").json()["sessions"][0]
    doc = hidden.get("/openapi.json").json()
    checked = 0
    for path, ops in doc["paths"].items():
        # Data routes only: the page's markup names its hidden links, and /metrics holds counters
        # from other tests in this process.
        if "get" not in ops or not (path.startswith("/api/") or path == "/openapi.json"):
            continue
        url = path.replace("{session_id}", session["session_id"]).replace(
            "{robot_id}", session["robot_id"]
        )
        response = hidden.get(url)
        assert response.status_code < 500, url
        assert not LEAK.search(response.text), (url, LEAK.search(response.text).group(0))
        checked += 1
    assert checked > 12
    # The sweep would pass on nothing at all: the same responses do hold it when switched on.
    assert LEAK.search(shown.get("/api/catalog").text)
    assert LEAK.search(shown.get("/api/quality/results", params={"limit": 2000}).text)


def test_catalog_keeps_telemetry_tables_only(shown, hidden):
    def names(client):
        return {t["name"] for t in client.get("/api/catalog").json()["tables"]}

    assert set(JIGSAWS_TABLES) <= names(shown)
    assert not set(JIGSAWS_TABLES) & names(hidden)
    assert {"telemetry.sessions", "marts.robot_daily", "dq.check_results"} <= names(hidden)


def test_health_hides_jigsaws_tables(shown, hidden):
    assert "procedures.trials" in shown.get("/api/health").json()["tables"]
    tables = hidden.get("/api/health").json()["tables"]
    assert "telemetry.sessions" in tables and not set(JIGSAWS_TABLES) & set(tables)


def test_quality_hides_jigsaws_checks_however_it_is_asked(shown, hidden):
    runs = shown.get("/api/quality/runs").json()["runs"]
    procedures = next(r for r in runs if r["suite"] == "procedures")
    assert "procedures" not in {r["suite"] for r in hidden.get("/api/quality/runs").json()["runs"]}
    url = "/api/quality/results"
    by_table = hidden.get(url, params={"table_name": "procedures.trials"}).json()["results"]
    by_run = hidden.get(url, params={"run_id": procedures["run_id"]}).json()["results"]
    assert by_table == []
    assert not {r["table_name"] for r in by_run} & set(JIGSAWS_TABLES)  # a run can span suites
    assert hidden.get(url, params={"table_name": "telemetry.sessions"}).json()["results"]


def test_a_full_page_of_quality_runs_is_not_cut_short_by_hiding(hidden, shown):
    """Filtered in the query, not after its LIMIT, or a page could come back nearly empty."""
    limit = 1
    visible = hidden.get("/api/quality/runs", params={"limit": limit}).json()["runs"]
    assert len(visible) == limit and visible[0]["suite"] != "procedures"


def test_jobs_hide_trial_requests_and_refuse_new_ones(shown, hidden):
    body = {"kind": "trial", "target": "Suturing_B001"}
    assert shown.post("/api/jobs/reprocess", json=body).status_code == 202
    assert any(j["kind"] == "trial" for j in shown.get("/api/jobs").json()["jobs"])
    assert all(j["kind"] != "trial" for j in hidden.get("/api/jobs").json()["jobs"])
    refused = hidden.post("/api/jobs/reprocess", json=body)
    assert refused.status_code == 400 and "trial" not in refused.json()["detail"]
    date = {"kind": "date", "target": "2026-10-28"}
    assert hidden.post("/api/jobs/reprocess", json=date).status_code == 202


def test_openapi_describes_what_is_served_in_both_modes(shown, hidden):
    for client in (shown, hidden):
        doc = client.get("/openapi.json").json()
        documented = {
            (m, re.sub(r"\{[^}]+\}", "{id}", p)) for p, ops in doc["paths"].items() for m in ops
        }
        mounted = {(m, re.sub(r"\{[^}]+\}", "{id}", p)) for m, p in documented_routes(client.app)}
        assert mounted == documented


def test_openapi_does_not_mention_it_when_off():
    doc = without_jigsaws_openapi(yaml.safe_load(OPENAPI_PATH.read_text()))
    assert not re.search(r"Suturing|procedures|/api/trials|/api/gestures", json.dumps(doc))
    assert "JIGSAWS procedures" not in doc["info"]["description"]
    assert "Trial" not in doc["components"]["schemas"]


def test_features_tell_the_ui(shown, hidden):
    on = shown.get("/api/features").json()
    assert on["jigsaws"] is True and set(on["jigsaws_tables"]) == set(JIGSAWS_TABLES)
    assert hidden.get("/api/features").json() == {"jigsaws": False, "jigsaws_tables": []}


def test_catalog_labels_each_table_when_on(shown):
    tables = {t["name"]: t for t in shown.get("/api/catalog").json()["tables"]}
    assert tables["procedures.trials"]["dataset"] == "jigsaws"
    assert tables["marts.gesture_skill_summary"]["dataset"] == "jigsaws"
    assert tables["dq.check_results"]["dataset"] == "mixed"
    assert "dataset" not in tables["telemetry.sessions"]


def test_jigsaws_tables_are_exactly_the_procedure_tables_and_their_mart():
    assert set(JIGSAWS_TABLES) == {
        "procedures.trials",
        "procedures.kinematics",
        "procedures.gestures",
        "procedures.frame_features",
        "marts.gesture_skill_summary",
    }


def _nav():
    html = (STATIC_DIR / "index.html").read_text()
    return {m[1]: m[0] for m in re.findall(r"<a ([^>]*data-nav=\"(\w+)\"[^>]*)>", html)}


def test_nav_marks_pages_by_what_they_show():
    nav = _nav()
    for page in ("trials", "gestures"):
        assert 'data-jigsaws="only"' in nav[page]
        assert "hidden" in nav[page]  # stays out of sight until the API says otherwise
    for page in ("catalog", "quality", "jobs"):
        assert 'data-jigsaws="mixed"' in nav[page] and "hidden" not in nav[page]
    for page in ("sessions", "events", "kafka", "contracts"):
        assert "data-jigsaws" not in nav[page]


@pytest.mark.parametrize(
    "value,expected", [("false", False), ("0", False), ("off", False), ("true", True), ("1", True)]
)
def test_the_setting_reads_from_the_environment(value, expected):
    assert Settings.from_env({"SERVE_JIGSAWS": value}).serve_jigsaws is expected


def test_it_is_on_by_default():
    assert Settings.from_env({}).serve_jigsaws is True


def test_a_mistyped_setting_stops_startup_rather_than_guessing():
    with pytest.raises(ValueError):
        Settings.from_env({"SERVE_JIGSAWS": "maybe"})
