from __future__ import annotations

import pytest

pytest.importorskip("starlette", reason="serving tests need the 'api' extra")


@pytest.fixture(scope="session")
def fixture_db(tmp_path_factory):
    """A small but real SQLite mirror: 1 simulated robot + 1 synthetic JIGSAWS trial per task."""
    from rtdaas.jigsaws.synth import generate_dataset
    from rtdaas.local.pipeline import run as run_pipeline
    from rtdaas.sim.cli import main as sim_main

    root = tmp_path_factory.mktemp("serving")
    bags = root / "bags"
    assert sim_main(["run", "--robots", "1", "--duration", "12", "--rate", "20", "--seed", "3",
                     "--mcap-out", str(bags), "--no-realtime", "--start-time", "2026-10-28T10:00:00Z"]) == 0
    generate_dataset(root / "jigsaws", n_trials_per_task=1, seed=3, duration_s=2.0)
    db = root / "rtdaas.sqlite"
    counts = run_pipeline(bags, root / "jigsaws", db)
    assert counts["telemetry.sessions"] >= 1 and counts["procedures.trials"] == 3
    return {"db": db, "root": root}


@pytest.fixture(scope="session")
def client(fixture_db):
    from starlette.testclient import TestClient

    from rtdaas.config import Settings
    from rtdaas.serving.app import create_app

    settings = Settings(serving_backend="sqlite", serving_sqlite_path=str(fixture_db["db"]),
                        media_root=str(fixture_db["root"]), airflow_api_url="http://127.0.0.1:1/api/v1")
    app = create_app(settings)
    with TestClient(app) as c:
        yield c
