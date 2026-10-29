from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from rtdaas.metrics.kinematics import (
    jaw_cycles,
    kpi_windows,
    path_length,
    session_id,
    session_metrics,
    speeds,
    split_sessions,
)


def _line(n=101, speed=0.02, hz=50.0, start="2026-10-28T10:00:00Z"):
    """Straight line along x at constant speed."""
    t0 = pd.Timestamp(start)
    ts = t0 + pd.to_timedelta(np.arange(n) / hz, unit="s")
    x = np.arange(n) * speed / hz
    return pd.DataFrame({"ts": ts, "x": x, "y": 0.0, "z": 0.0, "jaw_angle": 0.0})


def test_constant_speed_line():
    df = _line()
    v = speeds(df)
    assert v[0] == 0 and np.allclose(v[1:], 0.02)
    assert path_length(df) == pytest.approx(0.02 * 100 / 50.0)  # 2 s at 0.02 m/s


def test_jaw_cycles_counts_upward_crossings():
    jaw = np.array([0.0, 0.05, 0.2, 0.3, 0.05, 0.0, 0.15, 0.0, 0.2])
    assert jaw_cycles(pd.Series(jaw)) == 3
    assert jaw_cycles(pd.Series([0.5, 0.5, 0.5])) == 0


def test_session_metrics_idle_fraction_and_events():
    moving = _line(n=100)
    idle = _line(n=100, speed=0.0, start="2026-10-28T10:00:02Z")
    idle["x"] = moving["x"].iloc[-1]  # stay where the moving segment ended
    pose = pd.concat([moving, idle], ignore_index=True)
    events = pd.DataFrame(
        {
            "ts": [pd.Timestamp("2026-10-28T10:00:01Z")] * 3,
            "severity": [2, 3, 1],
            "code": ["JOINT_LIMIT_WARN", "ESTOP", "INSTRUMENT_SWAP"],
        }
    )
    m = session_metrics(pose, events)
    assert m["n_samples"] == 200
    assert m["duration_s"] == pytest.approx(199 / 50.0, rel=1e-6)
    assert 0.45 <= m["idle_fraction"] <= 0.55
    assert m["path_length_m"] == pytest.approx(0.02 * 99 / 50.0, rel=1e-3)
    assert m["warn_events"] == 1 and m["error_events"] == 1 and m["instrument_swaps"] == 1


def test_session_metrics_empty():
    m = session_metrics(pd.DataFrame(columns=["ts", "x", "y", "z", "jaw_angle"]))
    assert m["n_samples"] == 0 and m["path_length_m"] == 0


def test_kpi_windows_one_minute():
    pose = _line(n=50 * 150, speed=0.01)  # 150 s at 50 Hz -> 3 windows (60, 60, 30 s)
    hb = pd.DataFrame({"ts": pose["ts"].iloc[::50]})  # 1 Hz
    k = kpi_windows(pose, None, hb, window="1min")
    assert len(k) == 3
    assert list(k["window_start"].dt.second) == [0, 0, 0] and list(k["window_start"].dt.minute) == [
        0,
        1,
        2,
    ]
    assert k["n_samples"].iloc[0] == 3000 and k["n_samples"].iloc[2] == 1500
    assert k["mean_speed_mps"].iloc[0] == pytest.approx(0.01, rel=1e-6)
    assert k["path_length_m"].iloc[0] == pytest.approx(0.6, rel=1e-2)
    assert k["idle_fraction"].iloc[0] == 0.0
    assert k["heartbeat_gap_s"].iloc[0] == pytest.approx(1.0)


def test_split_sessions_prefers_events_then_heartbeat_gaps():
    t = pd.Timestamp("2026-10-28T10:00:00Z")
    span = (t, t + pd.Timedelta(minutes=10))
    events = pd.DataFrame(
        {
            "ts": [
                t + pd.Timedelta(minutes=1),
                t + pd.Timedelta(minutes=3),
                t + pd.Timedelta(minutes=5),
                t + pd.Timedelta(minutes=8),
            ],
            "code": ["SESSION_START", "SESSION_END", "SESSION_START", "SESSION_END"],
            "severity": [1, 1, 1, 1],
        }
    )
    s = split_sessions(events, None, span)
    assert s == [
        (t + pd.Timedelta(minutes=1), t + pd.Timedelta(minutes=3)),
        (t + pd.Timedelta(minutes=5), t + pd.Timedelta(minutes=8)),
    ]
    # heartbeat gap of 2 minutes between 10:04 and 10:06 splits in two
    hb_ts = list(pd.date_range(t, t + pd.Timedelta(minutes=4), freq="1s", tz="UTC")) + list(
        pd.date_range(t + pd.Timedelta(minutes=6), span[1], freq="1s", tz="UTC")
    )
    s2 = split_sessions(None, pd.DataFrame({"ts": hb_ts}), span, gap_s=60)
    assert len(s2) == 2 and s2[0][0] == t and s2[1][1] == span[1]
    assert split_sessions(None, None, span) == [span]


def test_session_id_format():
    assert session_id("dv-001", pd.Timestamp("2026-10-28T10:00:00Z")) == "dv-001-20261028T100000"
