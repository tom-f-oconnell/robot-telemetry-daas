"""Browser-ready copy of a downloaded JIGSAWS dataset (``rtdaas-jigsaws import``).

The download has the layout the pipelines read, but not quite the contents they should get:

* its videos are MPEG-4 Part 2 AVIs, which browsers cannot play;
* each trial has a video per eye, and the pipelines take one video per trial;
* some kinematics files have no video, transcript or skill score (they are the difference between
  40 files per task and the dataset's 103 trials).

The copy has the same layout, holding the complete trials and one H.264 MP4 each (the left eye).
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from rtdaas.jigsaws.layout import TrialMeta, discover_trials

log = logging.getLogger(__name__)

DEFAULT_JOBS = 4


def transcode_h264(src: Path, dst: Path) -> None:
    """Write video ``src`` to ``dst`` as an H.264 MP4 that browsers can play and seek in.

    ``passthrough`` keeps frame N as frame N, which the kinematics cursor and the frame features
    rely on. ``-g 30`` puts a keyframe every second, so scrubbing is quick, and ``faststart`` puts
    the index first, so playback starts before the download ends. The file appears only when
    complete, so an interrupted run is redone rather than mistaken for finished.
    """
    import imageio_ffmpeg  # local import: optional dependency

    tmp = dst.with_name(f"{dst.stem}.part{dst.suffix}")
    # fmt: off
    cmd = [
        imageio_ffmpeg.get_ffmpeg_exe(), "-nostdin", "-v", "error", "-y", "-i", str(src),
        "-map", "0:v:0", "-an", "-c:v", "libx264", "-preset", "medium", "-crf", "23",
        "-pix_fmt", "yuv420p", "-fps_mode", "passthrough", "-g", "30", "-threads", "2",
        "-movflags", "+faststart", str(tmp),
    ]
    # fmt: on
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        os.replace(tmp, dst)
    except subprocess.CalledProcessError as exc:
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"ffmpeg failed on {src}: {exc.stderr.strip()}") from exc


def _is_complete(trial: TrialMeta) -> bool:
    return bool(trial.video_path and trial.transcription_path and trial.skill_level != "unknown")


def _copy(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)


def import_dataset(
    root: Path, out: Path, tasks: Iterable[str] | None = None, jobs: int = DEFAULT_JOBS
) -> dict:
    """Copy the complete trials under ``root`` to ``out``, transcoding their videos.

    Safe to re-run: a video already transcoded from an unchanged source is left alone. Returns
    ``{"trials", "transcoded", "skipped"}``, with ``skipped`` the ids of the incomplete trials.
    Raises :class:`RuntimeError` if any video failed, after trying the rest.
    """
    root, out = Path(root), Path(out)
    trials = discover_trials(root, tasks)
    complete = [t for t in trials if _is_complete(t)]
    skipped = sorted(t.trial_id for t in trials if not _is_complete(t))
    if not complete:
        raise RuntimeError(f"no complete JIGSAWS trials under {root}")

    to_transcode: list[tuple[Path, Path]] = []
    for t in complete:
        task_out = out / t.task
        _copy(t.kinematics_path, task_out / "kinematics" / "AllGestures" / t.kinematics_path.name)
        _copy(t.transcription_path, task_out / "transcriptions" / t.transcription_path.name)
        video = task_out / "video" / t.video_path.with_suffix(".mp4").name
        video.parent.mkdir(parents=True, exist_ok=True)
        if not video.exists() or video.stat().st_mtime < t.video_path.stat().st_mtime:
            to_transcode.append((t.video_path, video))
    for task in {t.task for t in complete}:
        meta = f"meta_file_{task}.txt"
        _copy(root / task / meta, out / task / meta)

    log.info("transcoding %d of %d videos", len(to_transcode), len(complete))
    with ThreadPoolExecutor(max_workers=jobs) as pool:
        futures = {pool.submit(transcode_h264, *pair): pair[0] for pair in to_transcode}
    failed = {src: f.exception() for f, src in futures.items() if f.exception()}
    if failed:
        details = "; ".join(str(exc) for exc in failed.values())
        raise RuntimeError(f"{len(failed)} of {len(to_transcode)} videos failed: {details}")
    return {"trials": len(complete), "transcoded": len(to_transcode), "skipped": skipped}
