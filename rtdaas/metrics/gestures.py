"""Per-gesture metrics for JIGSAWS trials (spec §3.2 ``procedures.gestures``)."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

GRIPPER_OPEN = 0.5
GESTURE_METRIC_COLUMNS: tuple[str, ...] = (
    "gesture", "start_frame", "end_frame", "duration_s",
    "path_length_psml", "path_length_psmr", "mean_speed_psml", "mean_speed_psmr",
    "gripper_cycles", "motion_energy_mean", "n_frames",
)


def _path_and_speed(xyz: np.ndarray, dt: float) -> tuple[float, float]:
    if len(xyz) < 2:
        return 0.0, 0.0
    steps = np.linalg.norm(np.diff(xyz, axis=0), axis=1)
    total = float(steps.sum())
    duration = dt * (len(xyz) - 1)
    return total, (total / duration if duration > 0 else 0.0)


def _upward_crossings(x: np.ndarray, threshold: float) -> int:
    x = x[np.isfinite(x)]
    if x.size < 2:
        return 0
    above = x >= threshold
    return int(np.count_nonzero(~above[:-1] & above[1:]))


def gesture_metrics(
    kin: pd.DataFrame,
    transcript: pd.DataFrame,
    frames: pd.DataFrame | None,
    sample_rate_hz: float = 30.0,
) -> pd.DataFrame:
    """One row per transcript segment with motion metrics for the PSM arms.

    ``kin`` is the output of :func:`rtdaas.jigsaws.parse_kinematics` (0-based ``frame_idx``);
    ``transcript`` has 1-based inclusive ``start_frame``/``end_frame`` and ``gesture``;
    ``frames`` (optional) is :func:`rtdaas.jigsaws.video.frame_features` output, used for
    ``motion_energy_mean`` (NaN when absent).
    """
    dt = 1.0 / float(sample_rate_hz)
    frame_1based = kin["frame_idx"].to_numpy() + 1
    psml = kin[["psml_x", "psml_y", "psml_z"]].to_numpy(dtype=np.float64)
    psmr = kin[["psmr_x", "psmr_y", "psmr_z"]].to_numpy(dtype=np.float64)
    grip_l = kin["psml_gripper"].to_numpy(dtype=np.float64)
    grip_r = kin["psmr_gripper"].to_numpy(dtype=np.float64)

    motion = None
    if frames is not None and len(frames) and "motion_energy" in frames:
        motion = pd.Series(frames["motion_energy"].to_numpy(dtype=np.float64), index=frames["frame_idx"].to_numpy())

    rows: list[dict] = []
    for start, end, gesture in transcript[["start_frame", "end_frame", "gesture"]].itertuples(index=False):
        start, end = int(start), int(end)
        mask = (frame_1based >= start) & (frame_1based <= end)
        n = int(mask.sum())
        pl_l, sp_l = _path_and_speed(psml[mask], dt)
        pl_r, sp_r = _path_and_speed(psmr[mask], dt)
        me = np.nan
        if motion is not None:
            seg_idx = kin["frame_idx"].to_numpy()[mask]
            vals = motion.reindex(seg_idx).dropna()
            me = float(vals.mean()) if len(vals) else np.nan
        rows.append(
            {
                "gesture": gesture,
                "start_frame": start,
                "end_frame": end,
                "duration_s": float(max(n - 1, 0) * dt) if n else float((end - start + 1) * dt),
                "path_length_psml": pl_l,
                "path_length_psmr": pl_r,
                "mean_speed_psml": sp_l,
                "mean_speed_psmr": sp_r,
                "gripper_cycles": _upward_crossings(grip_l[mask], GRIPPER_OPEN) + _upward_crossings(grip_r[mask], GRIPPER_OPEN),
                "motion_energy_mean": me,
                "n_frames": n,
            }
        )
    out = pd.DataFrame(rows, columns=list(GESTURE_METRIC_COLUMNS))
    for c in ("start_frame", "end_frame", "gripper_cycles", "n_frames"):
        out[c] = out[c].astype("int64")
    return out
