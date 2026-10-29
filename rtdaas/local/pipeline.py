"""Serial, pure-Python pipeline that mirrors the Spark jobs into SQLite (spec §4.7).

Bags -> ``telemetry.*_raw`` -> sessions / 1-minute KPIs; JIGSAWS -> ``procedures.*``;
quality suites -> ``dq.check_results``; catalog.yml -> ``catalog``. Every computation
calls the same ``rtdaas.metrics`` / ``rtdaas.jigsaws`` / ``rtdaas.quality`` functions the
Spark jobs use, so the SQLite mirror is a faithful small-scale stand-in for the lake.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from collections.abc import Iterable

import pandas as pd
import yaml

from rtdaas import __version__
from rtdaas.config import DEFAULT_CHECKS_DIR, DEFAULT_MSGS_DIR
from rtdaas.jigsaws.layout import (
    ARM_FIELDS,
    ARM_ORDER,
    TrialMeta,
    discover_trials,
    label_frames,
    parse_kinematics,
    parse_transcription,
)
from rtdaas.local.bags import RAW_TABLES, bag_frames, find_bags
from rtdaas.local.sqlite_store import SqliteStore, format_ts
from rtdaas.metrics.gestures import gesture_metrics
from rtdaas.metrics.kinematics import (
    KPI_METRIC_COLUMNS,
    SESSION_METRIC_KEYS,
    kpi_windows,
    session_id,
    session_metrics,
    split_sessions,
)
from rtdaas.quality.adapters import SqliteAdapter
from rtdaas.quality.engine import has_blocking_failures, load_suite, new_run_id, run_suite, to_frame

log = logging.getLogger(__name__)

CATALOG_YML = Path(__file__).resolve().parent.parent / "serving" / "catalog.yml"
SESSION_COLUMNS = [
    "session_id", "robot_id", "arm", "start_ts", "end_ts", *SESSION_METRIC_KEYS,
    "bag_file", "pipeline_version", "processed_at",
]
KPI_COLUMNS = ["robot_id", "arm", "window_start", "window_end", *KPI_METRIC_COLUMNS, "computed_by", "computed_at"]
TRIAL_COLUMNS = [
    "trial_id", "task", "subject", "repetition", "skill_level", "grs_total", "duration_s", "n_frames",
    "n_kinematic_samples", "sample_rate_hz", "video_path", "kinematics_path", "transcription_path",
    "n_gestures", "processed_at", "pipeline_version",
]
KINEMATIC_FLAT_COLUMNS = [f"{arm}_{f}" for arm in ARM_ORDER for f in ARM_FIELDS]
GESTURE_COLUMNS = [
    "trial_id", "task", "subject", "skill_level", "gesture", "start_frame", "end_frame", "duration_s",
    "path_length_psml", "path_length_psmr", "mean_speed_psml", "mean_speed_psmr", "gripper_cycles",
    "motion_energy_mean", "n_frames",
]
FRAME_COLUMNS = ["trial_id", "task", "frame_idx", "t_s", "mean_intensity", "motion_energy", "sharpness", "gesture"]


def _utcnow() -> pd.Timestamp:
    return pd.Timestamp(datetime.now(UTC))


def default_suites() -> list[Path]:
    return sorted(Path(DEFAULT_CHECKS_DIR).glob("*.yml"))


# --------------------------------------------------------------------------- telemetry

def _events_for_arm(events: pd.DataFrame, arm: str) -> pd.DataFrame:
    if events is None or len(events) == 0:
        return events
    arm_col = events["arm"].fillna("")
    return events.loc[(arm_col == arm) | (arm_col == "")]


def build_sessions_and_kpis(
    frames: dict[str, pd.DataFrame],
    pipeline_version: str = __version__,
    processed_at: pd.Timestamp | None = None,
    gap_s: float = 60.0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """``telemetry.sessions`` and ``telemetry.kpi_1m`` (computed_by=batch) from raw frames.

    Sessions are found per (robot_id, bag_file) with :func:`split_sessions`, so a bag
    boundary always splits sessions; metrics are per session x arm.
    """
    processed_at = processed_at or _utcnow()
    pose = frames.get("tool_pose", pd.DataFrame())
    events = frames.get("events", pd.DataFrame())
    heartbeat = frames.get("heartbeat", pd.DataFrame())
    session_rows: list[dict] = []
    kpi_rows: list[pd.DataFrame] = []
    if len(pose) == 0:
        return pd.DataFrame(columns=SESSION_COLUMNS), pd.DataFrame(columns=KPI_COLUMNS)

    for (robot, bag_file), pose_rb in pose.groupby(["robot_id", "bag_file"], sort=True, dropna=False):
        ev_rb = events.loc[(events["robot_id"] == robot) & (events["bag_file"] == bag_file)] if len(events) else events
        hb_rb = heartbeat.loc[(heartbeat["robot_id"] == robot) & (heartbeat["bag_file"] == bag_file)] if len(heartbeat) else heartbeat
        span = (pose_rb["ts"].min(), pose_rb["ts"].max())
        for start, end in split_sessions(ev_rb if len(ev_rb) else None, hb_rb if len(hb_rb) else None, span, gap_s=gap_s):
            in_session = pose_rb.loc[(pose_rb["ts"] >= start) & (pose_rb["ts"] <= end)]
            if len(in_session) == 0:
                continue
            sid = session_id(robot, start)
            for arm, pose_arm in in_session.groupby("arm", sort=True):
                ev_arm = _events_for_arm(ev_rb, arm) if len(ev_rb) else None
                ev_arm = ev_arm.loc[(ev_arm["ts"] >= start) & (ev_arm["ts"] <= end)] if ev_arm is not None and len(ev_arm) else None
                metrics = session_metrics(pose_arm, ev_arm)
                session_rows.append(
                    {
                        "session_id": sid, "robot_id": robot, "arm": arm,
                        "start_ts": pd.Timestamp(start), "end_ts": pd.Timestamp(end),
                        **{k: metrics[k] for k in SESSION_METRIC_KEYS},
                        "bag_file": bag_file, "pipeline_version": pipeline_version, "processed_at": processed_at,
                    }
                )
                kpi = kpi_windows(pose_arm, ev_arm, hb_rb if len(hb_rb) else None)
                if len(kpi):
                    kpi.insert(0, "arm", arm)
                    kpi.insert(0, "robot_id", robot)
                    kpi["computed_by"] = "batch"
                    kpi["computed_at"] = processed_at
                    kpi_rows.append(kpi[KPI_COLUMNS])

    sessions = pd.DataFrame(session_rows, columns=SESSION_COLUMNS)
    kpis = pd.concat(kpi_rows, ignore_index=True) if kpi_rows else pd.DataFrame(columns=KPI_COLUMNS)
    if len(kpis):
        kpis = kpis.drop_duplicates(subset=["robot_id", "arm", "window_start"], keep="last").reset_index(drop=True)
    for c in ("n_samples", "jaw_cycles", "warn_events", "error_events", "instrument_swaps"):
        sessions[c] = sessions[c].astype("int64") if len(sessions) else sessions[c]
    return sessions, kpis


def run_telemetry(store: SqliteStore, bags_dir: Path, msgs_dir: Path = DEFAULT_MSGS_DIR, pipeline_version: str = __version__) -> dict[str, int]:
    paths = find_bags(bags_dir)
    log.info("found %d bags under %s", len(paths), bags_dir)
    frames = bag_frames(paths, msgs_dir)
    counts: dict[str, int] = {}
    for short, table in RAW_TABLES.items():
        counts[table] = store.write(table, frames[short], mode="replace")
    sessions, kpis = build_sessions_and_kpis(frames, pipeline_version)
    counts["telemetry.sessions"] = store.write("telemetry.sessions", sessions, mode="replace")
    counts["telemetry.kpi_1m"] = store.write("telemetry.kpi_1m", kpis, mode="replace")
    return counts


# --------------------------------------------------------------------------- procedures

def process_trial(trial: TrialMeta, pipeline_version: str = __version__, processed_at: pd.Timestamp | None = None, sample_rate_hz: float = 30.0, with_video: bool = True) -> dict[str, pd.DataFrame]:
    """All ``procedures.*`` rows for one trial (same function the Spark job maps per trial)."""
    processed_at = processed_at or _utcnow()
    kin = parse_kinematics(trial.kinematics_path, sample_rate_hz)
    transcript = parse_transcription(trial.transcription_path) if trial.transcription_path else pd.DataFrame(columns=["start_frame", "end_frame", "gesture"])
    kin = label_frames(kin, transcript)

    frames: pd.DataFrame | None = None
    if with_video and trial.video_path is not None:
        try:
            from rtdaas.jigsaws.video import frame_features

            frames = frame_features(trial.video_path)
        except Exception as exc:  # noqa: BLE001 - a bad video must not drop the trial's kinematics
            log.warning("%s: video features unavailable (%s)", trial.trial_id, exc)
            frames = None

    gestures = gesture_metrics(kin, transcript, frames, sample_rate_hz) if len(transcript) else pd.DataFrame(columns=GESTURE_COLUMNS[4:])
    gestures.insert(0, "skill_level", trial.skill_level)
    gestures.insert(0, "subject", trial.subject)
    gestures.insert(0, "task", trial.task)
    gestures.insert(0, "trial_id", trial.trial_id)

    kin_out = kin.copy()
    kin_out.insert(0, "task", trial.task)
    kin_out.insert(0, "trial_id", trial.trial_id)
    kin_out = kin_out[["trial_id", "task", "frame_idx", "t_s", *KINEMATIC_FLAT_COLUMNS, "gesture"]]

    if frames is not None and len(frames):
        ff = frames.copy()
        ff.insert(0, "task", trial.task)
        ff.insert(0, "trial_id", trial.trial_id)
        label = kin.set_index("frame_idx")["gesture"]
        ff["gesture"] = ff["frame_idx"].map(label).astype(object).where(ff["frame_idx"].isin(label.index), None)
        ff = ff[FRAME_COLUMNS]
    else:
        ff = pd.DataFrame(columns=FRAME_COLUMNS)

    trial_row = {
        "trial_id": trial.trial_id, "task": trial.task, "subject": trial.subject, "repetition": trial.repetition,
        "skill_level": trial.skill_level, "grs_total": trial.grs_total,
        "duration_s": float(kin["t_s"].iloc[-1]) if len(kin) else 0.0,
        "n_frames": int(len(ff)) if len(ff) else int(len(kin)),
        "n_kinematic_samples": int(len(kin)), "sample_rate_hz": float(sample_rate_hz),
        "video_path": str(trial.video_path) if trial.video_path else None,
        "kinematics_path": str(trial.kinematics_path),
        "transcription_path": str(trial.transcription_path) if trial.transcription_path else None,
        "n_gestures": int(len(transcript)), "processed_at": processed_at, "pipeline_version": pipeline_version,
    }
    return {
        "trials": pd.DataFrame([trial_row], columns=TRIAL_COLUMNS),
        "kinematics": kin_out,
        "gestures": gestures[GESTURE_COLUMNS],
        "frame_features": ff,
    }


def run_procedures(store: SqliteStore, jigsaws_dir: Path, pipeline_version: str = __version__, tasks: Iterable[str] | None = None, with_video: bool = True) -> dict[str, int]:
    trials = discover_trials(Path(jigsaws_dir), tasks)
    processed_at = _utcnow()
    parts: dict[str, list[pd.DataFrame]] = {"trials": [], "kinematics": [], "gestures": [], "frame_features": []}
    for trial in trials:
        try:
            out = process_trial(trial, pipeline_version, processed_at, with_video=with_video)
        except Exception as exc:  # noqa: BLE001 - keep going, report in counts
            log.exception("trial %s failed: %s", trial.trial_id, exc)
            continue
        for k, df in out.items():
            parts[k].append(df)
    counts: dict[str, int] = {}
    for k, cols in (("trials", TRIAL_COLUMNS), ("kinematics", ["trial_id", "task", "frame_idx", "t_s", *KINEMATIC_FLAT_COLUMNS, "gesture"]), ("gestures", GESTURE_COLUMNS), ("frame_features", FRAME_COLUMNS)):
        df = pd.concat(parts[k], ignore_index=True) if parts[k] else pd.DataFrame(columns=cols)
        counts[f"procedures.{k}"] = store.write(f"procedures.{k}", df, mode="replace")
    return counts


# --------------------------------------------------------------------------- quality + catalog

def run_quality(store: SqliteStore, suites: Iterable[Path], partition_value: str | None = None, run_id: str | None = None) -> tuple[str, pd.DataFrame, bool]:
    run_id = run_id or new_run_id()
    run_ts = datetime.now(UTC)
    adapter = SqliteAdapter(store.conn)
    results = []
    for path in suites:
        results.extend(run_suite(load_suite(path), adapter, partition_value, run_id, now=run_ts))
    frame = to_frame(results, run_id, run_ts)
    store.write("dq.check_results", frame, mode="append")
    return run_id, frame, has_blocking_failures(results)


def load_catalog_yml(path: Path = CATALOG_YML) -> list[dict]:
    with open(path) as fh:
        doc = yaml.safe_load(fh) or {}
    owner_default = doc.get("owner_default")
    entries = []
    for t in doc.get("tables") or []:
        e = dict(t)
        e.setdefault("owner", owner_default)
        entries.append(e)
    return entries


def write_catalog(store: SqliteStore, now: pd.Timestamp | None = None, catalog_path: Path = CATALOG_YML) -> int:
    """Materialize ``catalog`` rows with live row counts and freshness (spec §6)."""
    now = now or _utcnow()
    entries = []
    for e in load_catalog_yml(catalog_path):
        name = e["name"]
        row_count = store.row_count(name)
        freshness_col = e.get("freshness_column")
        last = store.max_value(name, freshness_col) if freshness_col else None
        sla = e.get("sla_hours")
        if last is not None:
            last_ts = pd.to_datetime(last, utc=True)
            age_h = (now - last_ts).total_seconds() / 3600.0
            fresh = bool(age_h <= float(sla)) if sla is not None else True
            last_updated = format_ts(last_ts)
        else:
            last_updated = format_ts(now) if row_count else None
            fresh = bool(row_count > 0)
        entries.append(
            {
                "name": name, "owner": e.get("owner"), "description": e.get("description"), "sla_hours": sla,
                "row_count": row_count, "last_updated": last_updated, "freshness_ok": fresh,
                "contract": e.get("contract"), "columns": e.get("columns", []),
            }
        )
    return store.write_catalog(entries)


# --------------------------------------------------------------------------- entry point

def run(
    bags_dir: Path | str | None,
    jigsaws_dir: Path | str | None,
    sqlite_path: Path | str,
    msgs_dir: Path | str = DEFAULT_MSGS_DIR,
    suites: list[Path] | None = None,
    pipeline_version: str = __version__,
    partition_value: str | None = None,
    with_video: bool = True,
) -> dict:
    """Run the whole local pipeline; returns row counts per table plus quality run info."""
    store = SqliteStore(sqlite_path)
    counts: dict = {}
    try:
        if bags_dir is not None:
            counts.update(run_telemetry(store, Path(bags_dir), Path(msgs_dir), pipeline_version))
        if jigsaws_dir is not None:
            counts.update(run_procedures(store, Path(jigsaws_dir), pipeline_version, with_video=with_video))
        suite_paths = list(suites) if suites is not None else default_suites()
        if suite_paths:
            run_id, dq, blocking = run_quality(store, suite_paths, partition_value)
            counts["dq.check_results"] = int(len(dq))
            counts["quality"] = {
                "run_id": run_id,
                "checks": int(len(dq)),
                "failed": int((dq["status"] == "fail").sum()),
                "errors": int((dq["status"] == "error").sum()),
                "blocking_failures": bool(blocking),
            }
        counts["catalog"] = write_catalog(store)
    finally:
        store.close()
    log.info("local pipeline done: %s", counts)
    return counts
