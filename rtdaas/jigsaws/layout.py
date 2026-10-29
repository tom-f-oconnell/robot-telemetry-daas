"""JIGSAWS on-disk layout and file parsers.

Layout (one directory per task)::

    <root>/<Task>/kinematics/AllGestures/<Task>_<S><RRR>.txt   76 whitespace-separated floats per line, 30 Hz
    <root>/<Task>/transcriptions/<Task>_<S><RRR>.txt            "<start_frame> <end_frame> <Gxx>" per line
    <root>/<Task>/video/<Task>_<S><RRR>_capture1.avi|.mp4       endoscope video, 30 fps
    <root>/<Task>/meta_file_<Task>.txt                           "<trial_id> <N|I|E> <GRS total> <6 sub-scores>"

Frame indices in transcription files are **1-based** (as in the released dataset);
``parse_kinematics`` produces a 0-based ``frame_idx`` and ``label_frames`` reconciles
the two (a transcript row ``[start, end]`` covers ``frame_idx + 1 in [start, end]``).

Kinematic column order (76 = 4 arms x 19): MTML, MTMR, PSML, PSMR; each arm is
position (3), rotation matrix (9, row-major), linear velocity (3), angular velocity (3),
gripper angle (1). See docs/architecture/spec.md §3.2 (``procedures.kinematics``).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

TASKS: tuple[str, ...] = ("Suturing", "Knot_Tying", "Needle_Passing")
ARM_ORDER: tuple[str, ...] = ("mtml", "mtmr", "psml", "psmr")
ARM_FIELDS: tuple[str, ...] = (
    "x", "y", "z",
    "r11", "r12", "r13", "r21", "r22", "r23", "r31", "r32", "r33",
    "vx", "vy", "vz",
    "wx", "wy", "wz",
    "gripper",
)
N_KINEMATIC_COLUMNS = len(ARM_ORDER) * len(ARM_FIELDS)  # 76
KINEMATIC_COLUMNS: tuple[str, ...] = tuple(f"{arm}_{field}" for arm in ARM_ORDER for field in ARM_FIELDS)

SKILL_LEVELS: dict[str, str] = {"N": "novice", "I": "intermediate", "E": "expert"}
VIDEO_EXTENSIONS: tuple[str, ...] = (".avi", ".mp4", ".mkv", ".mov")

_TRIAL_RE = re.compile(r"^(?P<task>[A-Za-z_]+?)_(?P<subject>[A-Z])(?P<rep>\d{3})$")


@dataclass
class TrialMeta:
    """Everything known about one JIGSAWS trial before parsing its data files."""

    trial_id: str
    task: str
    subject: str
    repetition: int
    skill_level: str
    grs_total: int | None
    kinematics_path: Path
    transcription_path: Path | None
    video_path: Path | None


def parse_trial_id(trial_id: str) -> tuple[str, str, int]:
    """``"Suturing_B001"`` -> ``("Suturing", "B", 1)``."""
    m = _TRIAL_RE.match(trial_id)
    if not m:
        raise ValueError(f"not a JIGSAWS trial id: {trial_id!r}")
    return m.group("task"), m.group("subject"), int(m.group("rep"))


def parse_meta_file(path: Path) -> dict[str, tuple[str, int]]:
    """Parse ``meta_file_<Task>.txt`` -> ``{trial_id: (skill_level, grs_total)}``.

    Lines are whitespace/tab separated: trial id, skill letter (N/I/E), GRS total,
    then six sub-scores (ignored). Blank lines and malformed lines are skipped with a warning.
    """
    out: dict[str, tuple[str, int]] = {}
    for lineno, line in enumerate(Path(path).read_text().splitlines(), start=1):
        parts = line.split()
        if not parts:
            continue
        if len(parts) < 3:
            log.warning("%s:%d: malformed meta line %r", path, lineno, line)
            continue
        trial_id, skill_letter, grs = parts[0], parts[1].upper(), parts[2]
        skill = SKILL_LEVELS.get(skill_letter)
        if skill is None:
            log.warning("%s:%d: unknown skill letter %r", path, lineno, skill_letter)
            skill = "unknown"
        try:
            grs_total: int | None = int(float(grs))
        except ValueError:
            grs_total = None
        out[trial_id] = (skill, grs_total)
    return out


def _find_video(video_dir: Path, trial_id: str) -> Path | None:
    if not video_dir.is_dir():
        return None
    for suffix in ("_capture1", "_capture2", ""):
        for ext in VIDEO_EXTENSIONS:
            candidate = video_dir / f"{trial_id}{suffix}{ext}"
            if candidate.exists():
                return candidate
    return None


def discover_trials(root: Path, tasks: Iterable[str] | None = None) -> list[TrialMeta]:
    """Walk ``root`` and return one :class:`TrialMeta` per kinematics file found.

    ``tasks`` restricts discovery to the given task directories (default: every
    subdirectory of ``root`` that has ``kinematics/AllGestures``).
    """
    root = Path(root)
    if not root.is_dir():
        raise FileNotFoundError(f"JIGSAWS root does not exist: {root}")
    task_dirs: list[Path]
    if tasks is None:
        task_dirs = sorted(p for p in root.iterdir() if (p / "kinematics" / "AllGestures").is_dir())
    else:
        task_dirs = [root / t for t in tasks]

    trials: list[TrialMeta] = []
    for task_dir in task_dirs:
        task = task_dir.name
        kin_dir = task_dir / "kinematics" / "AllGestures"
        if not kin_dir.is_dir():
            log.warning("no kinematics directory for task %s under %s", task, root)
            continue
        meta: dict[str, tuple[str, int]] = {}
        meta_path = task_dir / f"meta_file_{task}.txt"
        if meta_path.exists():
            meta = parse_meta_file(meta_path)
        for kin_path in sorted(kin_dir.glob("*.txt")):
            trial_id = kin_path.stem
            try:
                _, subject, rep = parse_trial_id(trial_id)
            except ValueError:
                log.warning("skipping unrecognised kinematics file %s", kin_path)
                continue
            skill, grs = meta.get(trial_id, ("unknown", None))
            transcription = task_dir / "transcriptions" / f"{trial_id}.txt"
            trials.append(
                TrialMeta(
                    trial_id=trial_id,
                    task=task,
                    subject=subject,
                    repetition=rep,
                    skill_level=skill,
                    grs_total=grs,
                    kinematics_path=kin_path,
                    transcription_path=transcription if transcription.exists() else None,
                    video_path=_find_video(task_dir / "video", trial_id),
                )
            )
    log.info("discovered %d trials under %s", len(trials), root)
    return trials


def parse_kinematics(path: Path, sample_rate_hz: float = 30.0) -> pd.DataFrame:
    """Parse a 76-column kinematics file.

    Returns a frame with ``frame_idx`` (0-based), ``t_s`` (= frame_idx / sample_rate_hz)
    and the 76 ``<arm>_<field>`` columns in :data:`KINEMATIC_COLUMNS` order.
    """
    data = np.loadtxt(path, dtype=np.float64, ndmin=2)
    if data.size == 0:
        raise ValueError(f"empty kinematics file: {path}")
    if data.shape[1] != N_KINEMATIC_COLUMNS:
        raise ValueError(
            f"{path}: expected {N_KINEMATIC_COLUMNS} kinematic columns, found {data.shape[1]}"
        )
    df = pd.DataFrame(data, columns=list(KINEMATIC_COLUMNS))
    n = len(df)
    df.insert(0, "t_s", np.arange(n, dtype=np.float64) / float(sample_rate_hz))
    df.insert(0, "frame_idx", np.arange(n, dtype=np.int64))
    return df


def parse_transcription(path: Path) -> pd.DataFrame:
    """Parse ``<start_frame> <end_frame> <Gxx>`` lines (1-based inclusive frame numbers)."""
    rows: list[tuple[int, int, str]] = []
    for lineno, line in enumerate(Path(path).read_text().splitlines(), start=1):
        parts = line.split()
        if not parts:
            continue
        if len(parts) < 3:
            log.warning("%s:%d: malformed transcription line %r", path, lineno, line)
            continue
        rows.append((int(parts[0]), int(parts[1]), parts[2]))
    df = pd.DataFrame(rows, columns=["start_frame", "end_frame", "gesture"])
    df["start_frame"] = df["start_frame"].astype("int64")
    df["end_frame"] = df["end_frame"].astype("int64")
    df["gesture"] = df["gesture"].astype(object)
    return df.sort_values("start_frame", kind="stable").reset_index(drop=True)


def label_frames(kin: pd.DataFrame, transcript: pd.DataFrame | None) -> pd.DataFrame:
    """Add a ``gesture`` column to ``kin`` (None outside any transcribed segment).

    Transcript frames are 1-based; a segment ``[start, end]`` covers kinematic rows with
    ``frame_idx + 1`` in that inclusive range. Later segments win on overlap.
    """
    out = kin.copy()
    labels = np.full(len(out), None, dtype=object)
    if transcript is not None and len(transcript):
        frame_1based = out["frame_idx"].to_numpy() + 1
        for start, end, gesture in transcript[["start_frame", "end_frame", "gesture"]].itertuples(index=False):
            mask = (frame_1based >= int(start)) & (frame_1based <= int(end))
            labels[mask] = gesture
    out["gesture"] = labels
    return out
