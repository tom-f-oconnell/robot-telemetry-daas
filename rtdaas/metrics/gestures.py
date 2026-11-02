"""Per-gesture metrics for JIGSAWS trials (spec §3.2 ``procedures.gestures``)."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

MIN_GRIPPER_SWING = 0.2  # rad: an arm whose gripper barely moves never opens
GESTURE_METRIC_COLUMNS: tuple[str, ...] = (
    "gesture",
    "start_frame",
    "end_frame",
    "duration_s",
    "path_length_psml",
    "path_length_psmr",
    "mean_speed_psml",
    "mean_speed_psmr",
    "gripper_cycles",
    "motion_energy_mean",
    "n_frames",
)


def _path_and_speed(xyz: np.ndarray, dt: float) -> tuple[float, float]:
    if len(xyz) < 2:
        return 0.0, 0.0
    steps = np.linalg.norm(np.diff(xyz, axis=0), axis=1)
    total = float(steps.sum())
    duration = dt * (len(xyz) - 1)
    return total, (total / duration if duration > 0 else 0.0)


def _jaw_openings(gripper: np.ndarray) -> np.ndarray:
    """Whether the jaw opens at each sample, for one arm over a whole trial.

    What closed and open read differs by arm and trial (in JIGSAWS, closed is near -0.9 rad and open
    anywhere from 0 to 0.5), so both come from this arm's own signal. An opening is a rise from the
    lower 40 % of that range to the upper 40 %: with a single threshold, noise near it would count
    over and over.
    """
    opens = np.zeros(gripper.shape, dtype=bool)
    valid = gripper[np.isfinite(gripper)]
    if valid.size < 2:
        return opens
    closed, wide_open = np.percentile(valid, [5, 95])
    swing = wide_open - closed
    if swing < MIN_GRIPPER_SWING:
        return opens
    low, high = closed + 0.4 * swing, closed + 0.6 * swing
    is_open = None
    for i, value in enumerate(gripper):
        if value >= high:
            opens[i] = is_open is False
            is_open = True
        elif value <= low:
            is_open = False
    return opens


def gesture_metrics(
    kin: pd.DataFrame,
    transcript: pd.DataFrame,
    frames: pd.DataFrame | None,
    sample_rate_hz: float = 30.0,
) -> pd.DataFrame:
    """One row per transcript segment with motion metrics for the PSM arms.

    ``kin`` is the output of :func:`rtdaas.jigsaws.parse_kinematics` (0-based ``frame_idx``), for
    the whole trial: gripper levels are judged across it;
    ``transcript`` has 1-based inclusive ``start_frame``/``end_frame`` and ``gesture``;
    ``frames`` (optional) is :func:`rtdaas.jigsaws.video.frame_features` output, used for
    ``motion_energy_mean`` (NaN when absent).
    """
    dt = 1.0 / float(sample_rate_hz)
    frame_1based = kin["frame_idx"].to_numpy() + 1
    psml = kin[["psml_x", "psml_y", "psml_z"]].to_numpy(dtype=np.float64)
    psmr = kin[["psmr_x", "psmr_y", "psmr_z"]].to_numpy(dtype=np.float64)
    openings = _jaw_openings(kin["psml_gripper"].to_numpy(dtype=np.float64)).astype(int)
    openings += _jaw_openings(kin["psmr_gripper"].to_numpy(dtype=np.float64))

    motion = None
    if frames is not None and len(frames) and "motion_energy" in frames:
        motion = pd.Series(
            frames["motion_energy"].to_numpy(dtype=np.float64), index=frames["frame_idx"].to_numpy()
        )

    rows: list[dict] = []
    for start, end, gesture in transcript[["start_frame", "end_frame", "gesture"]].itertuples(
        index=False
    ):
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
                "gripper_cycles": int(openings[mask].sum()),
                "motion_energy_mean": me,
                "n_frames": n,
            }
        )
    out = pd.DataFrame(rows, columns=list(GESTURE_METRIC_COLUMNS))
    for c in ("start_frame", "end_frame", "gripper_cycles", "n_frames"):
        out[c] = out[c].astype("int64")
    return out
