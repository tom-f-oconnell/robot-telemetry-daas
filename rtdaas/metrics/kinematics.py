"""Economy-of-motion metrics for tool-tip telemetry (spec §3.2 ``telemetry.sessions`` /
``telemetry.kpi_1m`` and §4.5).

All functions take pandas frames with a UTC ``ts`` column (``datetime64[ns, UTC]``) and
metres for ``x, y, z``; speeds are in m/s. They are pure functions so the Spark jobs
(``applyInPandas``) and the local pipeline compute identical numbers.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

IDLE_SPEED_MPS = 0.005
JAW_OPEN_RAD = 0.1
SESSION_START = "SESSION_START"
SESSION_END = "SESSION_END"
INSTRUMENT_SWAP = "INSTRUMENT_SWAP"
SEVERITY_WARN = 2
SEVERITY_ERROR = 3

KPI_METRIC_COLUMNS: tuple[str, ...] = (
    "n_samples",
    "mean_speed_mps",
    "max_speed_mps",
    "path_length_m",
    "idle_fraction",
    "jaw_cycles",
    "warn_events",
    "error_events",
    "heartbeat_gap_s",
)
SESSION_METRIC_KEYS: tuple[str, ...] = (
    "duration_s",
    "n_samples",
    "path_length_m",
    "mean_speed_mps",
    "idle_fraction",
    "jaw_cycles",
    "warn_events",
    "error_events",
    "instrument_swaps",
)


def to_utc(ts: pd.Series | np.ndarray | list) -> pd.Series:
    """Coerce timestamps (datetime64, tz-aware, or ISO-8601 strings) to a UTC datetime Series."""
    s = pd.Series(ts)
    if pd.api.types.is_datetime64_any_dtype(s.dtype):
        return pd.to_datetime(s, utc=True)
    return pd.to_datetime(s, utc=True, format="ISO8601")


def _to_ns(ts: pd.Series | np.ndarray | list) -> np.ndarray:
    """UTC timestamps -> int64 nanoseconds since epoch (tz-safe under pandas 2 and 3)."""
    t = to_utc(ts)
    return (
        t.dt.tz_convert("UTC")
        .dt.tz_localize(None)
        .to_numpy()
        .astype("datetime64[ns]")
        .astype(np.int64)
    )


def _ts_seconds(ts: pd.Series) -> np.ndarray:
    """UTC timestamps -> float seconds relative to the first sample.

    Differences are taken on int64 nanoseconds before converting to float: float64 seconds
    since the epoch only resolve ~0.2 us at 1.7e9 s, which would put ~1e-5 relative error on
    50 Hz finite differences.
    """
    ns = _to_ns(ts)
    if len(ns) == 0:
        return ns.astype(np.float64)
    return (ns - ns[0]) / 1e9


def _stamp_ns(t: pd.Timestamp) -> int:
    t = pd.Timestamp(t)
    t = t.tz_convert("UTC") if t.tzinfo is not None else t.tz_localize("UTC")
    return int(t.value)


def _ensure_sorted(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy sorted by ``ts`` with ``ts`` coerced to UTC datetimes."""
    out = df.reset_index(drop=True).copy()
    out["ts"] = to_utc(out["ts"])
    if len(out) > 1 and not out["ts"].is_monotonic_increasing:
        out = out.sort_values("ts", kind="stable").reset_index(drop=True)
    return out


def speeds(df: pd.DataFrame) -> np.ndarray:
    """Per-sample 3-D speed (m/s) from finite differences of x, y, z over ts.

    ``speeds[0] == 0``; samples with a non-positive time step get speed 0 (duplicates).
    """
    n = len(df)
    out = np.zeros(n, dtype=np.float64)
    if n < 2:
        return out
    df = _ensure_sorted(df)
    xyz = df[["x", "y", "z"]].to_numpy(dtype=np.float64)
    t = _ts_seconds(df["ts"])
    step = np.linalg.norm(np.diff(xyz, axis=0), axis=1)
    dt = np.diff(t)
    with np.errstate(divide="ignore", invalid="ignore"):
        v = np.where(dt > 0, step / dt, 0.0)
    out[1:] = np.nan_to_num(v, nan=0.0, posinf=0.0, neginf=0.0)
    return out


def path_length(df: pd.DataFrame) -> float:
    """Total 3-D path length in metres (sum of step distances)."""
    if len(df) < 2:
        return 0.0
    df = _ensure_sorted(df)
    xyz = df[["x", "y", "z"]].to_numpy(dtype=np.float64)
    return float(np.linalg.norm(np.diff(xyz, axis=0), axis=1).sum())


def jaw_cycles(jaw_angle: pd.Series | np.ndarray, threshold: float = JAW_OPEN_RAD) -> int:
    """Number of upward crossings of ``threshold`` (closed -> open transitions)."""
    a = np.asarray(jaw_angle, dtype=np.float64)
    a = a[np.isfinite(a)]
    if a.size < 2:
        return 0
    above = a >= threshold
    return int(np.count_nonzero(~above[:-1] & above[1:]))


def _count_events(
    events: pd.DataFrame | None, start: pd.Timestamp | None = None, end: pd.Timestamp | None = None
) -> tuple[int, int, int]:
    """(warn_events, error_events, instrument_swaps) inside [start, end] (inclusive)."""
    if events is None or len(events) == 0:
        return 0, 0, 0
    ev = events
    if start is not None or end is not None:
        ts_ns = _to_ns(ev["ts"])
        mask = np.ones(len(ev), dtype=bool)
        if start is not None:
            mask &= ts_ns >= _stamp_ns(start)
        if end is not None:
            mask &= ts_ns <= _stamp_ns(end)
        ev = ev.loc[mask]
    sev = (
        pd.to_numeric(ev["severity"], errors="coerce").fillna(0).to_numpy()
        if "severity" in ev
        else np.zeros(len(ev))
    )
    warn = int(np.count_nonzero(sev == SEVERITY_WARN))
    err = int(np.count_nonzero(sev >= SEVERITY_ERROR))
    swaps = int((ev["code"] == INSTRUMENT_SWAP).sum()) if "code" in ev else 0
    return warn, err, swaps


def session_metrics(pose: pd.DataFrame, events: pd.DataFrame | None = None) -> dict:
    """Economy-of-motion summary for one robot x arm x session.

    ``pose`` columns: ``ts``, ``x``, ``y``, ``z``, ``jaw_angle`` (jaw may be NaN for arms
    without a jaw). ``events`` (optional) columns: ``ts``, ``severity``, ``code``; events are
    counted over the pose time span (or all events if the pose frame is empty).
    """
    n = len(pose)
    result: dict = {k: 0 for k in SESSION_METRIC_KEYS}
    result.update(
        {"duration_s": 0.0, "path_length_m": 0.0, "mean_speed_mps": 0.0, "idle_fraction": 0.0}
    )
    start = end = None
    if n:
        pose = _ensure_sorted(pose)
        t = _ts_seconds(pose["ts"])
        start, end = pd.Timestamp(pose["ts"].iloc[0]), pd.Timestamp(pose["ts"].iloc[-1])
        v = speeds(pose)
        result["duration_s"] = float(t[-1] - t[0])
        result["n_samples"] = int(n)
        result["path_length_m"] = path_length(pose)
        result["mean_speed_mps"] = (
            float(result["path_length_m"] / result["duration_s"])
            if result["duration_s"] > 0
            else 0.0
        )
        result["idle_fraction"] = float(np.mean(v[1:] < IDLE_SPEED_MPS)) if n > 1 else 1.0
        result["jaw_cycles"] = jaw_cycles(pose["jaw_angle"]) if "jaw_angle" in pose else 0
    warn, err, swaps = _count_events(events, start, end)
    result["warn_events"], result["error_events"], result["instrument_swaps"] = warn, err, swaps
    return result


def _window_bounds(ts: pd.Series, window: str) -> tuple[pd.Series, pd.Timedelta]:
    delta = pd.Timedelta(window)
    starts = to_utc(ts).dt.floor(delta)
    return starts, delta


def kpi_windows(
    pose: pd.DataFrame,
    events: pd.DataFrame | None,
    heartbeat: pd.DataFrame | None,
    window: str = "1min",
) -> pd.DataFrame:
    """Tumbling-window KPIs for one robot x arm.

    Returns one row per window with ``window_start``, ``window_end`` and the
    ``telemetry.kpi_1m`` metric columns (no robot_id/arm/computed_*). Windows are taken
    from the pose samples; speed for the first sample of a window uses the previous sample
    so step distances are not lost at window edges.
    """
    cols = ["window_start", "window_end", *KPI_METRIC_COLUMNS]
    if pose is None or len(pose) == 0:
        return pd.DataFrame(columns=cols)
    pose = _ensure_sorted(pose)
    v = speeds(pose)
    xyz = pose[["x", "y", "z"]].to_numpy(dtype=np.float64)
    step = np.zeros(len(pose))
    step[1:] = np.linalg.norm(np.diff(xyz, axis=0), axis=1)
    starts, delta = _window_bounds(pose["ts"], window)
    jaw = (
        pose["jaw_angle"].to_numpy(dtype=np.float64)
        if "jaw_angle" in pose
        else np.full(len(pose), np.nan)
    )

    hb_ns = None
    if heartbeat is not None and len(heartbeat):
        hb_ns = np.sort(_to_ns(heartbeat["ts"]))

    rows: list[dict] = []
    for ws, idx in pd.Series(np.arange(len(pose))).groupby(starts.to_numpy(), sort=True):
        ii = idx.to_numpy()
        ws = pd.Timestamp(ws)
        we = ws + delta
        sp = v[ii]
        # idle fraction excludes the very first sample of the whole series (no speed defined)
        sp_defined = sp if ii[0] != 0 else sp[1:]
        warn, err, _ = _count_events(events, ws, we - pd.Timedelta(nanoseconds=1))
        gap = np.nan
        if hb_ns is not None:
            in_win = hb_ns[(hb_ns >= _stamp_ns(ws)) & (hb_ns < _stamp_ns(we))]
            if in_win.size >= 2:
                gap = float(np.max(np.diff(in_win)) / 1e9)
        rows.append(
            {
                "window_start": ws,
                "window_end": we,
                "n_samples": int(ii.size),
                "mean_speed_mps": float(sp_defined.mean()) if sp_defined.size else 0.0,
                "max_speed_mps": float(sp_defined.max()) if sp_defined.size else 0.0,
                "path_length_m": float(step[ii].sum()),
                "idle_fraction": float(np.mean(sp_defined < IDLE_SPEED_MPS))
                if sp_defined.size
                else 1.0,
                "jaw_cycles": jaw_cycles(jaw[ii]),
                "warn_events": warn,
                "error_events": err,
                "heartbeat_gap_s": gap,
            }
        )
    out = pd.DataFrame(rows, columns=cols)
    out["window_start"] = pd.to_datetime(out["window_start"], utc=True)
    out["window_end"] = pd.to_datetime(out["window_end"], utc=True)
    return out


def split_sessions(
    events: pd.DataFrame | None,
    heartbeat: pd.DataFrame | None,
    span: tuple[pd.Timestamp, pd.Timestamp],
    gap_s: float = 60.0,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Split a robot's time span into sessions.

    Priority: ``SESSION_START``/``SESSION_END`` events; else heartbeat gaps larger than
    ``gap_s``; else the whole span. Returns inclusive ``(start, end)`` pairs, sorted.
    """
    span_start, span_end = (pd.Timestamp(span[0]), pd.Timestamp(span[1]))
    if span_start.tzinfo is None:
        span_start = span_start.tz_localize("UTC")
    if span_end.tzinfo is None:
        span_end = span_end.tz_localize("UTC")

    if events is not None and len(events) and "code" in events:
        ev = events.copy()
        ev["ts"] = to_utc(ev["ts"])
        ev = ev.sort_values("ts", kind="stable")
        starts = ev.loc[ev["code"] == SESSION_START, "ts"].tolist()
        ends = ev.loc[ev["code"] == SESSION_END, "ts"].tolist()
        if starts or ends:
            # Data before the first SESSION_START (robot idle, not in a session) is ignored.
            if not starts:
                starts = [span_start]
            sessions: list[tuple[pd.Timestamp, pd.Timestamp]] = []
            for i, s in enumerate(starts):
                next_start = starts[i + 1] if i + 1 < len(starts) else None
                candidate_ends = [
                    e for e in ends if e >= s and (next_start is None or e < next_start)
                ]
                if candidate_ends:
                    e = candidate_ends[0]
                elif next_start is not None:
                    e = next_start - pd.Timedelta(nanoseconds=1)
                else:
                    e = span_end
                sessions.append((s, e))
            return [(s, e) for s, e in sessions if e >= s]

    if heartbeat is not None and len(heartbeat):
        hb = np.sort(_to_ns(heartbeat["ts"]))
        if hb.size >= 2:
            gaps = np.diff(hb) / 1e9
            breaks = np.nonzero(gaps > gap_s)[0]
            if breaks.size:
                sessions = []
                s = span_start
                for b in breaks:
                    sessions.append((s, pd.Timestamp(int(hb[b]), unit="ns", tz="UTC")))
                    s = pd.Timestamp(int(hb[b + 1]), unit="ns", tz="UTC")
                sessions.append((s, span_end))
                return sessions
    return [(span_start, span_end)]


def session_id(robot_id: str, start_ts: pd.Timestamp) -> str:
    """``dv-001-20261029T134500`` (UTC)."""
    t = pd.Timestamp(start_ts)
    t = t.tz_convert("UTC") if t.tzinfo is not None else t.tz_localize("UTC")
    return f"{robot_id}-{t.strftime('%Y%m%dT%H%M%S')}"


def assign_sessions(
    ts: pd.Series, sessions: Iterable[tuple[pd.Timestamp, pd.Timestamp]]
) -> np.ndarray:
    """Map each timestamp to the index of its session (-1 if outside every session)."""
    t = _to_ns(ts)
    out = np.full(len(t), -1, dtype=np.int64)
    for i, (s, e) in enumerate(sessions):
        mask = (t >= _stamp_ns(s)) & (t <= _stamp_ns(e))
        out[mask] = i
    return out
