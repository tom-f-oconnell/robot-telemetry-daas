"""Synthetic JIGSAWS-layout dataset generator.

The real JIGSAWS dataset requires a data-use agreement, so tests, CI and the demo
use this generator. It writes exactly the layout :mod:`rtdaas.jigsaws.layout` reads:
kinematics (76 columns at 30 Hz), transcriptions (1-based frames, G1..G15), a meta
file, and a small video whose two moving discs follow the PSM tool-tip positions so
that video ``motion_energy`` correlates with kinematic speed.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from pathlib import Path

import numpy as np

from rtdaas.jigsaws.layout import ARM_FIELDS, ARM_ORDER, TASKS, TrialMeta, discover_trials

log = logging.getLogger(__name__)

SUBJECTS: tuple[str, ...] = ("B", "C", "D", "E", "F", "G", "H", "I")
SKILL_LETTERS: tuple[str, ...] = ("N", "I", "E")
GESTURES: tuple[str, ...] = tuple(f"G{i}" for i in range(1, 16))
WORKSPACE_M = 0.05  # PSM tool tips move inside a ~5 cm cube
VIDEO_SIZE = (320, 240)


def _psm_trajectory(rng: np.random.Generator, t: np.ndarray, skill: str) -> np.ndarray:
    """Smooth 3-D position (n, 3) in metres: sum of a few sinusoids inside the workspace.

    Experts move a little faster and smoother than novices; that is only flavour for
    demos, not a claim about the real data.
    """
    speed_scale = {"E": 1.3, "I": 1.0, "N": 0.8}[skill]
    jitter = {"E": 0.0005, "I": 0.001, "N": 0.002}[skill]
    pos = np.zeros((len(t), 3), dtype=np.float64)
    for axis in range(3):
        n_terms = int(rng.integers(2, 4))
        for _ in range(n_terms):
            freq = rng.uniform(0.1, 0.6) * speed_scale
            amp = rng.uniform(0.005, 0.02)
            phase = rng.uniform(0, 2 * np.pi)
            pos[:, axis] += amp * np.sin(2 * np.pi * freq * t + phase)
        pos[:, axis] += rng.uniform(-0.01, 0.01)
    # low-frequency wander + small measurement jitter, clipped to the workspace
    pos += rng.normal(0.0, jitter, size=pos.shape).cumsum(axis=0) * 0.05
    return np.clip(pos, -WORKSPACE_M, WORKSPACE_M)


def _rotation_matrices(rng: np.random.Generator, t: np.ndarray) -> np.ndarray:
    """(n, 9) row-major rotation matrices: small rotations about a wandering axis."""
    angle = 0.15 * np.sin(2 * np.pi * rng.uniform(0.05, 0.2) * t + rng.uniform(0, 2 * np.pi))
    axis = rng.normal(size=3)
    axis /= np.linalg.norm(axis)
    kx, ky, kz = axis
    K = np.array([[0, -kz, ky], [kz, 0, -kx], [-ky, kx, 0]])
    eye = np.eye(3)
    out = np.empty((len(t), 9), dtype=np.float64)
    for i, a in enumerate(angle):
        R = eye + np.sin(a) * K + (1 - np.cos(a)) * (K @ K)  # Rodrigues
        out[i] = R.reshape(-1)
    return out


def _finite_diff(x: np.ndarray, fps: float) -> np.ndarray:
    v = np.gradient(x, 1.0 / fps, axis=0)
    return v


def _arm_block(
    rng: np.random.Generator,
    t: np.ndarray,
    fps: float,
    skill: str,
    scale: float,
    gripper_phase: float,
) -> np.ndarray:
    pos = _psm_trajectory(rng, t, skill) * scale
    rot = _rotation_matrices(rng, t)
    lin_v = _finite_diff(pos, fps)
    ang_v = _finite_diff(
        np.arcsin(np.clip(rot[:, [7, 2, 3]], -1, 1)), fps
    )  # crude from off-diagonals
    gripper_freq = rng.uniform(0.2, 0.5)
    gripper = 0.5 + 0.5 * np.sin(2 * np.pi * gripper_freq * t + gripper_phase)
    block = np.concatenate([pos, rot, lin_v, ang_v, gripper[:, None]], axis=1)
    assert block.shape[1] == len(ARM_FIELDS)
    return block


def synth_kinematics(rng: np.random.Generator, n_frames: int, fps: float, skill: str) -> np.ndarray:
    """Return an (n_frames, 76) array in JIGSAWS column order (MTML, MTMR, PSML, PSMR)."""
    t = np.arange(n_frames, dtype=np.float64) / fps
    psml = _arm_block(rng, t, fps, skill, scale=1.0, gripper_phase=0.0)
    psmr = _arm_block(rng, t, fps, skill, scale=1.0, gripper_phase=np.pi / 2)
    # Masters mirror the slaves at the dVRK's ~3x motion scale (plus a little hand tremor).
    tremor = rng.normal(0.0, 0.0005, size=(n_frames, 3))
    mtml = psml.copy()
    mtmr = psmr.copy()
    for m in (mtml, mtmr):
        m[:, 0:3] *= 3.0
        m[:, 0:3] += tremor
        m[:, 12:15] *= 3.0
    blocks = {"mtml": mtml, "mtmr": mtmr, "psml": psml, "psmr": psmr}
    return np.concatenate([blocks[a] for a in ARM_ORDER], axis=1)


def synth_transcript(rng: np.random.Generator, n_frames: int) -> list[tuple[int, int, str]]:
    """3–6 contiguous gesture segments (1-based inclusive frames) covering ~90% of frames."""
    n_segments = int(rng.integers(3, 7))
    lead_in = int(rng.integers(0, max(1, n_frames // 20) + 1))
    tail = int(rng.integers(0, max(1, n_frames // 20) + 1))
    usable = max(n_segments, n_frames - lead_in - tail)
    cuts = (
        np.sort(rng.choice(np.arange(1, usable), size=n_segments - 1, replace=False))
        if usable > n_segments
        else np.array([], dtype=int)
    )
    bounds = np.concatenate([[0], cuts, [usable]])
    rows: list[tuple[int, int, str]] = []
    prev_gesture = None
    for i in range(len(bounds) - 1):
        start = lead_in + int(bounds[i]) + 1  # 1-based
        end = lead_in + int(bounds[i + 1])
        if end < start:
            continue
        gesture = str(rng.choice(GESTURES))
        if gesture == prev_gesture:
            gesture = GESTURES[(GESTURES.index(gesture) + 1) % len(GESTURES)]
        prev_gesture = gesture
        rows.append((start, end, gesture))
    return rows


def _render_frames(kin: np.ndarray):
    """Yield BGR frames: two instrument discs whose screen position tracks the PSM tips."""
    import cv2

    psml_i = ARM_ORDER.index("psml") * len(ARM_FIELDS)
    psmr_i = ARM_ORDER.index("psmr") * len(ARM_FIELDS)
    w, h = VIDEO_SIZE
    shift = 4  # fixed-point sub-pixel coordinates so small motions still change pixels
    gain = 1.6  # magnify the workspace so typical motion is a few pixels per frame

    def to_px(xy: np.ndarray) -> tuple[int, int]:
        px = np.clip((gain * xy[0] / WORKSPACE_M + 1) / 2 * (w - 40) + 20, 0, w - 1)
        py = np.clip((gain * xy[1] / WORKSPACE_M + 1) / 2 * (h - 40) + 20, 0, h - 1)
        return int(round(px * (1 << shift))), int(round(py * (1 << shift)))

    def to_int(pt: tuple[int, int]) -> tuple[int, int]:
        return pt[0] >> shift, pt[1] >> shift

    for i in range(kin.shape[0]):
        frame = np.full((h, w, 3), (40, 30, 30), dtype=np.uint8)  # dark tissue-ish background
        cv2.rectangle(frame, (10, 10), (w - 10, h - 10), (70, 50, 50), 2)
        lp = to_px(kin[i, psml_i : psml_i + 2])
        rp = to_px(kin[i, psmr_i : psmr_i + 2])
        l_grip = float(kin[i, psml_i + 18])
        r_grip = float(kin[i, psmr_i + 18])
        r_px = 14 << shift
        # instrument shafts from the frame edge to the tip, then the tips themselves
        cv2.line(frame, to_int(lp), (0, h // 2), (120, 120, 140), 3, cv2.LINE_AA)
        cv2.line(frame, to_int(rp), (w - 1, h // 2), (140, 120, 120), 3, cv2.LINE_AA)
        cv2.circle(frame, lp, r_px, (200, 200, 220), -1, cv2.LINE_AA, shift)
        cv2.circle(frame, rp, r_px, (220, 200, 200), -1, cv2.LINE_AA, shift)
        # jaw opening shown as a small inner dot whose brightness follows the gripper
        cv2.circle(frame, lp, 4 << shift, (int(60 + 150 * l_grip),) * 3, -1, cv2.LINE_AA, shift)
        cv2.circle(frame, rp, 4 << shift, (int(60 + 150 * r_grip),) * 3, -1, cv2.LINE_AA, shift)
        yield frame


def _write_video_h264(path: Path, kin: np.ndarray, fps: float) -> Path | None:
    """Encode with libx264 through imageio-ffmpeg so browsers can play the file (yuv420p)."""
    try:
        import imageio_ffmpeg
    except ImportError:
        return None
    w, h = VIDEO_SIZE
    out_path = path.with_suffix(".mp4")
    try:
        writer = imageio_ffmpeg.write_frames(
            str(out_path),
            (w, h),
            fps=fps,
            codec="libx264",
            pix_fmt_in="rgb24",
            pix_fmt_out="yuv420p",
            output_params=["-movflags", "+faststart", "-crf", "23"],
            macro_block_size=1,
        )
        writer.send(None)
        for frame in _render_frames(kin):
            writer.send(np.ascontiguousarray(frame[:, :, ::-1]))  # BGR -> RGB
        writer.close()
    except Exception as exc:  # noqa: BLE001 - fall back to OpenCV codecs
        log.warning("imageio-ffmpeg H.264 encode failed (%s); falling back to OpenCV", exc)
        if out_path.exists():
            out_path.unlink()
        return None
    return out_path if out_path.exists() and out_path.stat().st_size > 0 else None


def _write_video(path: Path, kin: np.ndarray, fps: float) -> Path | None:
    """Render the PSM tool tips as two discs; returns the path actually written.

    Prefers H.264 (playable in browsers and by the UI's trial page); falls back to OpenCV's
    ``mp4v``/``MJPG`` encoders, which OpenCV can read but browsers cannot.
    """
    try:
        import cv2
    except ImportError:  # pragma: no cover - opencv is an optional dependency
        log.warning("opencv not available; skipping video for %s", path)
        return None

    written = _write_video_h264(path, kin, fps)
    if written is not None:
        return written

    w, h = VIDEO_SIZE
    attempts = [("avc1", ".mp4"), ("mp4v", ".mp4"), ("MJPG", ".avi")]
    for codec, ext in attempts:
        out_path = path.with_suffix(ext)
        writer = cv2.VideoWriter(str(out_path), cv2.VideoWriter_fourcc(*codec), fps, (w, h))
        if not writer.isOpened():
            writer.release()
            if out_path.exists():
                out_path.unlink()
            continue
        try:
            for frame in _render_frames(kin):
                writer.write(frame)
        finally:
            writer.release()
        if out_path.exists() and out_path.stat().st_size > 0:
            return out_path
    log.warning("could not write a video for %s with any codec", path)
    return None


def generate_dataset(
    out_dir: Path,
    n_trials_per_task: int = 2,
    tasks: Sequence[str] | Iterable[str] = TASKS,
    seed: int = 0,
    fps: float = 30.0,
    duration_s: float = 6.0,
    video: bool = True,
) -> list[TrialMeta]:
    """Write a synthetic JIGSAWS-layout dataset under ``out_dir`` and return its trials."""
    out_dir = Path(out_dir)
    tasks = tuple(tasks)
    n_frames = max(2, int(round(duration_s * fps)))
    for task_i, task in enumerate(tasks):
        task_dir = out_dir / task
        kin_dir = task_dir / "kinematics" / "AllGestures"
        tr_dir = task_dir / "transcriptions"
        vid_dir = task_dir / "video"
        for d in (kin_dir, tr_dir, vid_dir):
            d.mkdir(parents=True, exist_ok=True)
        meta_lines: list[str] = []
        for k in range(n_trials_per_task):
            subject = SUBJECTS[k % len(SUBJECTS)]
            rep = k // len(SUBJECTS) + 1
            trial_id = f"{task}_{subject}{rep:03d}"
            skill = SKILL_LETTERS[(SUBJECTS.index(subject)) % len(SKILL_LETTERS)]
            rng = np.random.default_rng([seed, task_i, k])
            kin = synth_kinematics(rng, n_frames, fps, skill)
            np.savetxt(kin_dir / f"{trial_id}.txt", kin, fmt="%.6f", delimiter="     ")
            transcript = synth_transcript(rng, n_frames)
            (tr_dir / f"{trial_id}.txt").write_text(
                "".join(f"{s} {e} {g} \n" for s, e, g in transcript)
            )
            grs_sub = rng.integers(1, 6, size=6)
            base = {"N": 0, "I": 5, "E": 10}[skill]
            grs_total = int(min(30, base + grs_sub.sum() // 2 + 6))
            meta_lines.append(
                f"{trial_id}\t{skill}\t{grs_total}\t" + "\t".join(str(int(x)) for x in grs_sub)
            )
            if video:
                _write_video(vid_dir / f"{trial_id}_capture1.mp4", kin, fps)
        (task_dir / f"meta_file_{task}.txt").write_text("\n".join(meta_lines) + "\n")
    trials = discover_trials(out_dir, tasks=tasks)
    log.info("generated %d synthetic trials under %s", len(trials), out_dir)
    return trials
