from __future__ import annotations

import json
import sqlite3

import pandas as pd
import pytest

from rtdaas.jigsaws.synth import generate_dataset
from rtdaas.local.cli import main as local_main
from rtdaas.local.pipeline import run
from rtdaas.quality.cli import main as quality_main
from rtdaas.quality.schemas import SPEC_SCHEMAS, sqlite_table_name
from rtdaas.sim.cli import main as sim_main


@pytest.fixture(scope="module")
def pipeline_out(tmp_path_factory):
    root = tmp_path_factory.mktemp("local")
    assert sim_main(["run", "--robots", "2", "--duration", "70", "--rate", "20", "--seed", "11",
                     "--mcap-out", str(root / "bags"), "--no-realtime", "--start-time", "2026-10-28T09:00:00Z"]) == 0
    generate_dataset(root / "jigsaws", n_trials_per_task=1, seed=11, duration_s=2.0)
    db = root / "rtdaas.sqlite"
    counts = run(root / "bags", root / "jigsaws", db, partition_value="2026-10-28")
    return root, db, counts


def _tables(db):
    with sqlite3.connect(db) as c:
        return {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def test_all_spec_tables_written(pipeline_out):
    root, db, counts = pipeline_out
    names = _tables(db)
    for spec_table in SPEC_SCHEMAS:
        assert sqlite_table_name(spec_table) in names, spec_table
    assert {"catalog", "jobs"} <= names
    assert counts["telemetry.tool_pose_raw"] == 2 * 3 * 70 * 20
    assert counts["telemetry.sessions"] == 6  # 2 robots x 3 arms, one session each
    assert counts["telemetry.kpi_1m"] == 12  # 70 s spans two 1-minute windows per robot x arm
    assert counts["procedures.trials"] == 3 and counts["procedures.gestures"] > 0
    assert counts["quality"]["blocking_failures"] is False
    assert counts["catalog"] >= 11


def test_sqlite_conventions(pipeline_out):
    root, db, _ = pipeline_out
    with sqlite3.connect(db) as c:
        js = pd.read_sql('SELECT * FROM "telemetry__joint_states_raw" LIMIT 1', c)
        assert isinstance(json.loads(js["joint_names"][0]), list) and len(json.loads(js["position"][0])) == 6
        ts = js["ts"][0]
        assert ts.endswith("+00:00") and ts[10] == "T"
        kin = pd.read_sql('SELECT * FROM "procedures__kinematics" LIMIT 1', c)
        assert {"psml_x", "psmr_gripper", "mtml_r33", "trial_id", "frame_idx", "gesture"} <= set(kin.columns)
        sessions = pd.read_sql('SELECT * FROM "telemetry__sessions"', c)
        assert set(sessions["arm"]) == {"PSM1", "PSM2", "ECM"}
        assert (sessions["session_id"].str.startswith("dv-00")).all()
        assert sessions.loc[sessions["arm"] == "ECM", "jaw_cycles"].eq(0).all()
        assert (sessions["pipeline_version"] == "0.1.0").all()
        dq = pd.read_sql('SELECT * FROM "dq__check_results"', c)
        assert set(dq["suite"]) == {"telemetry", "procedures", "streaming"} and (dq["partition_value"] == "2026-10-28").any()
        cat = pd.read_sql("SELECT * FROM catalog", c)
        assert set(cat["name"]) >= set(SPEC_SCHEMAS) and (cat["row_count"] >= 0).all()


def test_rerun_is_idempotent(pipeline_out):
    root, db, counts = pipeline_out
    again = run(root / "bags", root / "jigsaws", db, partition_value="2026-10-28", with_video=False)
    for k in ("telemetry.tool_pose_raw", "telemetry.sessions", "telemetry.kpi_1m", "procedures.trials", "procedures.gestures"):
        assert again[k] == counts[k], k


def test_cli_and_quality_cli(pipeline_out, tmp_path, repo_root):
    root, db, _ = pipeline_out
    out = tmp_path / "cli.sqlite"
    rc = local_main(["pipeline", "--jigsaws", str(root / "jigsaws"), "--out", str(out), "--no-video",
                     "--suite", str(repo_root / "quality" / "checks" / "procedures.yml")])
    assert rc == 0 and "procedures__gestures" in _tables(out)
    rc = quality_main(["run", "--suite", str(repo_root / "quality" / "checks" / "procedures.yml"), "--backend", "sqlite", "--db", str(out), "--out-table"])
    assert rc in (0, 2)
    with sqlite3.connect(out) as c:
        n = c.execute('SELECT COUNT(*) FROM "dq__check_results"').fetchone()[0]
    assert n > 0


def test_pipeline_without_bags_or_jigsaws(tmp_path):
    counts = run(None, None, tmp_path / "empty.sqlite", suites=[])
    assert counts["catalog"] >= 0 and "telemetry.sessions" not in counts
