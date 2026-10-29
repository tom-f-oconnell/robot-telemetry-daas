"""Per-frame video features computed with OpenCV.

These are deliberately cheap, interpretable features: enough to QC a recording and to
correlate video motion with kinematic speed, not a computer-vision model.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

FRAME_FEATURE_COLUMNS: tuple[str, ...] = (
    "frame_idx",
    "t_s",
    "mean_intensity",
    "motion_energy",
    "sharpness",
)
DEFAULT_FPS = 30.0


class VideoReadError(RuntimeError):
    """Raised when a video file cannot be opened or yields no frames."""


def _open_capture(video_path: Path):
    import cv2  # local import: optional dependency

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        cap.release()
        raise VideoReadError(f"OpenCV could not open video: {video_path}")
    return cv2, cap


def video_info(video_path: Path) -> dict:
    """Return ``{"fps", "n_frames", "width", "height"}`` without decoding frames."""
    cv2, cap = _open_capture(Path(video_path))
    try:
        fps = float(cap.get(cv2.CAP_PROP_FPS)) or DEFAULT_FPS
        return {
            "fps": fps,
            "n_frames": int(cap.get(cv2.CAP_PROP_FRAME_COUNT)),
            "width": int(cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
            "height": int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
        }
    finally:
        cap.release()


def frame_features(video_path: Path, max_frames: int | None = None) -> pd.DataFrame:
    """Compute per-frame features for a video.

    Columns: ``frame_idx`` (0-based), ``t_s`` (= frame_idx / fps),
    ``mean_intensity`` (mean grey level / 255), ``motion_energy`` (mean |grey[t] - grey[t-1]| / 255,
    0 for the first frame) and ``sharpness`` (variance of the Laplacian of the grey frame).
    Raises :class:`VideoReadError` if the file cannot be opened or contains no frames.
    """
    video_path = Path(video_path)
    if not video_path.exists():
        raise VideoReadError(f"video file not found: {video_path}")
    cv2, cap = _open_capture(video_path)
    fps = float(cap.get(cv2.CAP_PROP_FPS))
    if not np.isfinite(fps) or fps <= 0:
        log.warning("%s: no fps in container metadata, assuming %.1f", video_path, DEFAULT_FPS)
        fps = DEFAULT_FPS

    idx: list[int] = []
    mean_i: list[float] = []
    motion: list[float] = []
    sharp: list[float] = []
    prev: np.ndarray | None = None
    i = 0
    try:
        while True:
            if max_frames is not None and i >= max_frames:
                break
            ok, frame = cap.read()
            if not ok or frame is None:
                break
            grey = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            g = grey.astype(np.float32)
            idx.append(i)
            mean_i.append(float(g.mean()) / 255.0)
            motion.append(0.0 if prev is None else float(np.abs(g - prev).mean()) / 255.0)
            sharp.append(float(cv2.Laplacian(grey, cv2.CV_64F).var()))
            prev = g
            i += 1
    finally:
        cap.release()

    if not idx:
        raise VideoReadError(f"video contains no decodable frames: {video_path}")

    frame_idx = np.asarray(idx, dtype=np.int64)
    return pd.DataFrame(
        {
            "frame_idx": frame_idx,
            "t_s": frame_idx.astype(np.float64) / fps,
            "mean_intensity": np.asarray(mean_i, dtype=np.float64),
            "motion_energy": np.asarray(motion, dtype=np.float64),
            "sharpness": np.asarray(sharp, dtype=np.float64),
        }
    )
