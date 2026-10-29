from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
import pytest

from rtdaas.quality.adapters import PandasAdapter, SqliteAdapter
from rtdaas.quality.engine import CheckResult, has_blocking_failures, load_suite, run_suite, to_frame
from rtdaas.quality.schemas import SPEC_SCHEMAS

NOW = dt.datetime(2026, 10, 29, 12, 0, tzinfo=dt.timezone.utc)


def _pose(n=600, hz=50.0, start="2026-10-29T10:00:00Z", robot="dv-001"):
    ts = pd.Timestamp(start) + pd.to_timedelta(np.arange(n) / hz, unit="s")
    return pd.DataFrame({
        "robot_id": robot, "arm": "PSM1", "ts": ts, "recv_ts": ts, "x": np.linspace(0, 0.1, n), "y": 0.0, "z": -0.1,
        "qx": 0.0, "qy": 0.0, "qz": 0.0, "qw": 1.0, "jaw_angle": 0.0, "speed": 0.01, "source": "bag", "bag_file": "b.mcap",
    })


def _suite(checks, table="telemetry.tool_pose_raw", partition_column=None):
    t = {"table": table, "checks": checks}
    if partition_column:
        t["partition_column"] = partition_column
    return {"suite": "t", "tables": [t]}


def _run(checks, frame, **kw):
    return run_suite(_suite(checks, **kw), PandasAdapter({"telemetry.tool_pose_raw": frame}), now=NOW)


def _status(results, name):
    return next(r for r in results if r.check_name == name).status


def test_not_null_row_count_unique():
    good = _pose()
    bad = good.copy()
    bad.loc[0, "ts"] = pd.NaT
    bad = pd.concat([bad, bad.iloc[[1]]], ignore_index=True)  # duplicate key
    checks = [
        {"name": "nn", "type": "not_null", "column": "ts", "severity": "blocking"},
        {"name": "rc", "type": "row_count", "min": 100, "max": 10_000, "severity": "blocking"},
        {"name": "uq", "type": "unique", "columns": ["robot_id", "arm", "ts"], "severity": "blocking"},
    ]
    ok = _run(checks, good)
    assert {r.status for r in ok} == {"pass"} and not has_blocking_failures(ok)
    res = _run(checks, bad)
    assert _status(res, "nn") == "fail" and _status(res, "uq") == "fail" and _status(res, "rc") == "pass"
    assert has_blocking_failures(res)
    assert _status(_run([{"name": "rc", "type": "row_count", "min": 10_000, "severity": "warning"}], good), "rc") == "fail"


def test_value_range_with_tolerated_fraction():
    df = _pose()
    df.loc[:2, "speed"] = 5.0  # 3 of 600 out of range
    checks = [{"name": "vr", "type": "value_range", "column": "speed", "min": 0, "max": 2.0, "max_violation_fraction": 0.01, "severity": "warning"}]
    assert _status(_run(checks, df), "vr") == "pass"
    checks[0]["max_violation_fraction"] = 0.001
    r = _run(checks, df)
    assert _status(r, "vr") == "fail" and next(x for x in r if x.check_name == "vr").observed == pytest.approx(3 / 600)


def test_sample_rate_and_freshness():
    df = _pose(hz=50.0)
    sr = {"name": "sr", "type": "sample_rate", "ts_column": "ts", "group_by": ["robot_id", "arm"], "expected_hz": 50, "tolerance_fraction": 0.1, "severity": "warning"}
    assert _status(_run([sr], df), "sr") == "pass"
    assert _status(_run([{**sr, "expected_hz": 100}], df), "sr") == "fail"
    fr = {"name": "fr", "type": "freshness", "ts_column": "ts", "max_age_hours": 3, "severity": "warning"}
    assert _status(_run([fr], df), "fr") == "pass"  # data at 10:00, now 12:00
    assert _status(_run([{**fr, "max_age_hours": 1}], df), "fr") == "fail"


def test_schema_match_against_spec():
    df = _pose()
    chk = [{"name": "sc", "type": "schema_match", "expected": "spec", "severity": "blocking"}]
    assert _status(_run(chk, df), "sc") == "pass"
    missing = df.drop(columns=["speed"])
    r = _run(chk, missing)
    assert _status(r, "sc") == "fail" and "speed" in next(x for x in r if x.check_name == "sc").message
    extra = df.assign(extra_col=1)
    assert _status(_run(chk, extra), "sc") == "pass"
    assert set(SPEC_SCHEMAS["telemetry.tool_pose_raw"]) == set(df.columns)


def test_sql_check_and_partition_filter():
    df = pd.concat([_pose(start="2026-10-28T10:00:00Z"), _pose(start="2026-10-29T10:00:00Z")], ignore_index=True)
    # sql checks filter via the {partition} placeholder (the engine substitutes {table} and {partition})
    checks = [{"name": "cnt", "type": "sql", "query": "SELECT COUNT(*) FROM {table} WHERE substr(ts, 1, 10) = '{partition}'",
               "op": "==", "threshold": 600, "severity": "blocking"}]
    adapter = PandasAdapter({"telemetry.tool_pose_raw": df})
    res = run_suite(_suite(checks, partition_column="ts"), adapter, partition_value="2026-10-29", now=NOW)
    assert _status(res, "cnt") == "pass" and res[0].partition_value == "2026-10-29"
    whole = [{"name": "cnt", "type": "sql", "query": "SELECT COUNT(*) FROM {table}", "op": "==", "threshold": 600, "severity": "blocking"}]
    assert _status(run_suite(_suite(whole), adapter, now=NOW), "cnt") == "fail"  # 1200 rows without a filter


def test_unknown_table_and_type_are_errors_not_exceptions():
    res = run_suite({"suite": "x", "tables": [{"table": "nope", "checks": [{"name": "a", "type": "not_null", "column": "c"}]}]}, PandasAdapter({}), now=NOW)
    assert res[0].status == "error"
    res = _run([{"name": "b", "type": "does_not_exist"}], _pose())
    assert res[0].status == "error"


def test_to_frame_and_sqlite_adapter(tmp_path):
    results = [CheckResult("s", "telemetry.sessions", "c1", "blocking", "pass", 1.0, ">= 1", None, "")]
    frame = to_frame(results, run_id="abc", run_ts=NOW)
    assert list(frame.columns) == ["run_id", "suite", "table_name", "check_name", "severity", "status", "observed", "threshold", "partition_value", "run_ts", "message"]
    from rtdaas.local.sqlite_store import SqliteStore

    store = SqliteStore(tmp_path / "m.sqlite")
    store.write("telemetry.tool_pose_raw", _pose())
    adapter = SqliteAdapter(tmp_path / "m.sqlite")
    assert set(adapter.columns("telemetry.tool_pose_raw")) >= {"robot_id", "ts", "speed"}
    assert len(adapter.frame("telemetry.tool_pose_raw", "ts", "2026-10-29")) == 600
    assert len(adapter.frame("telemetry.tool_pose_raw", "ts", "2026-10-28")) == 0
    assert adapter.sql('SELECT COUNT(*) FROM "telemetry__tool_pose_raw"') == 600


def test_real_suites_load_and_reference_spec_tables(repo_root):
    for name in ("telemetry", "procedures", "streaming"):
        suite = load_suite(repo_root / "quality" / "checks" / f"{name}.yml")
        assert suite["suite"] == name
        for t in suite["tables"]:
            assert t["table"] in SPEC_SCHEMAS, t["table"]
            for c in t["checks"]:
                assert c["severity"] in {"blocking", "warning"} and c["type"] in {
                    "not_null", "row_count", "value_range", "unique", "sample_rate", "schema_match", "freshness", "sql"}
