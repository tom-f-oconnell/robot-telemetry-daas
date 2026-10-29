"""Batch job: JIGSAWS (or synthetic) dataset -> ``procedures.*`` tables + ES gestures (spec §5.3).

The dataset may live on a local/NFS path or in object storage (``s3a://``). Both are read
through Spark's ``binaryFile`` source, so executors never need direct S3 credentials or
a shared filesystem: each trial's kinematics text, transcription text and video bytes are
joined by ``trial_id`` and processed inside ``mapPartitions`` with the pure-Python
parsers (:mod:`rtdaas.jigsaws`) and metrics (:mod:`rtdaas.metrics.gestures`). Files are
spilled to a per-task temp dir because the parsers (and OpenCV) take paths.

Trial metadata (subject, repetition, skill level, GRS) comes from
:func:`rtdaas.jigsaws.layout.discover_trials` when the root is a local directory, and is
derived from file names + ``meta_file_<Task>.txt`` otherwise.

Run::

    spark-submit rtdaas/spark/batch_jigsaws.py --root s3a://rtdaas-raw/jigsaws [--task Suturing]
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import posixpath
import sys
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path

import pandas as pd

from rtdaas.config import Settings, configure_logging, get_settings
from rtdaas.spark import ddl
from rtdaas.spark.common import (
    ES_INDEX_GESTURES,
    build_spark,
    ensure_tables,
    es_bulk,
    es_ensure_index,
    merge_iceberg,
    rows_as_dicts,
    write_iceberg,
)

log = logging.getLogger(__name__)

SAMPLE_RATE_HZ = 30.0
VIDEO_GLOB = "*/video/*.{avi,mp4,mkv,mov}"
KINEMATICS_GLOB = "*/kinematics/AllGestures/*.txt"
TRANSCRIPTION_GLOB = "*/transcriptions/*.txt"
META_GLOB = "*/meta_file_*.txt"


def _is_local(root: str) -> bool:
    return "://" not in root or root.startswith("file:")


def _strip_scheme(p: str) -> str:
    return p[len("file:") :] if p.startswith("file:") else p


# ---------------------------------------------------------------------------
# Discovery (driver)
# ---------------------------------------------------------------------------


def _trial_id_from_path(path: str) -> str:
    stem = posixpath.splitext(posixpath.basename(path))[0]
    for suffix in ("_capture1", "_capture2"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    return stem


def _task_from_path(path: str) -> str:
    # .../<Task>/kinematics/AllGestures/<trial>.txt | .../<Task>/transcriptions/<trial>.txt | .../<Task>/video/<file>
    parts = path.rstrip("/").split("/")
    for i, p in enumerate(parts):
        if p in ("kinematics", "transcriptions", "video") and i > 0:
            return parts[i - 1]
    return parts[-2] if len(parts) >= 2 else ""


def _binary_files(spark, root: str, glob: str, with_content: bool = True):
    df = spark.read.format("binaryFile").load(f"{root.rstrip('/')}/{glob}")
    return df.select("path", "content") if with_content else df.select("path")


def trial_metadata(spark, root: str, task: str | None) -> dict[str, dict]:
    """``trial_id -> {task, subject, repetition, skill_level, grs_total}`` for every kinematics file."""
    from rtdaas.jigsaws.layout import parse_meta_file, parse_trial_id

    if _is_local(root):
        from rtdaas.jigsaws.layout import discover_trials

        trials = discover_trials(Path(_strip_scheme(root)), tasks=[task] if task else None)
        return {
            t.trial_id: {
                "task": t.task,
                "subject": t.subject,
                "repetition": t.repetition,
                "skill_level": t.skill_level,
                "grs_total": t.grs_total,
            }
            for t in trials
        }

    # Object storage: derive from file names + meta files.
    meta: dict[str, tuple[str, int | None]] = {}
    for r in _binary_files(spark, root, META_GLOB).collect():
        with tempfile.NamedTemporaryFile("wb", suffix=".txt", delete=False) as fh:
            fh.write(bytes(r["content"]))
            tmp = fh.name
        try:
            meta.update(parse_meta_file(Path(tmp)))
        finally:
            os.unlink(tmp)
    out: dict[str, dict] = {}
    for r in _binary_files(spark, root, KINEMATICS_GLOB, with_content=False).collect():
        trial_id = _trial_id_from_path(r["path"])
        t_task = _task_from_path(r["path"])
        if task and t_task != task:
            continue
        try:
            _, subject, rep = parse_trial_id(trial_id)
        except ValueError:
            subject, rep = trial_id[-4:-3], 0
        skill, grs = meta.get(trial_id, ("unknown", None))
        out[trial_id] = {
            "task": t_task,
            "subject": subject,
            "repetition": rep,
            "skill_level": skill,
            "grs_total": grs,
        }
    return out


def gather_inputs(spark, root: str, task: str | None):
    """RDD of ``(trial_id, {"kin": bytes, "transcript": bytes|None, "video": bytes|None, paths...})``."""

    def keyed(df, kind: str):
        return df.rdd.map(
            lambda r: (_trial_id_from_path(r["path"]), (kind, r["path"], bytes(r["content"])))
        )

    kin = keyed(_binary_files(spark, root, KINEMATICS_GLOB), "kin")
    tr = keyed(_binary_files(spark, root, TRANSCRIPTION_GLOB), "transcript")
    try:
        vid = keyed(_binary_files(spark, root, VIDEO_GLOB), "video")
    except Exception as exc:  # no video directory at all
        log.warning("no videos found under %s (%s)", root, exc)
        vid = spark.sparkContext.emptyRDD()

    def fold(acc, item):
        kind, path, content = item
        acc = dict(acc)
        acc[kind] = content
        acc[f"{kind}_path"] = path
        return acc

    def merge(a, b):
        out = dict(a)
        out.update(b)
        return out

    all_files = kin.union(tr).union(vid)
    grouped = all_files.aggregateByKey({}, fold, merge)
    if task:
        grouped = grouped.filter(lambda kv: _task_from_path(kv[1].get("kin_path", "")) == task)
    return grouped.filter(lambda kv: "kin" in kv[1])


# ---------------------------------------------------------------------------
# Per-trial processing (executors)
# ---------------------------------------------------------------------------


def _arm_struct(row: pd.Series, arm: str) -> dict:
    return {f: float(row[f"{arm}_{f}"]) for f in ddl.ARM_STRUCT_FIELDS}


def process_trial(
    trial_id: str, files: dict, meta: dict, pipeline_version: str, processed_at: dt.datetime
):
    """Parse + compute everything for one trial. Returns ``{"trials": [...], "kinematics": [...], "gestures": [...], "frame_features": [...]}``."""
    from rtdaas.jigsaws.layout import label_frames, parse_kinematics, parse_transcription
    from rtdaas.metrics.gestures import gesture_metrics

    task = meta.get("task") or _task_from_path(files.get("kin_path", ""))
    with tempfile.TemporaryDirectory(prefix="rtdaas-jigsaws-") as tmp:
        tmp_path = Path(tmp)
        kin_file = tmp_path / f"{trial_id}.txt"
        kin_file.write_bytes(files["kin"])
        kin = parse_kinematics(kin_file, SAMPLE_RATE_HZ)

        transcript = None
        if files.get("transcript") is not None:
            tr_file = tmp_path / f"{trial_id}_transcript.txt"
            tr_file.write_bytes(files["transcript"])
            transcript = parse_transcription(tr_file)
        kin = label_frames(kin, transcript)

        frames = None
        if files.get("video") is not None:
            from rtdaas.jigsaws.video import frame_features

            ext = posixpath.splitext(files.get("video_path", "x.avi"))[1] or ".avi"
            vid_file = tmp_path / f"{trial_id}{ext}"
            vid_file.write_bytes(files["video"])
            try:
                frames = frame_features(vid_file)
            except Exception as exc:
                log.warning("%s: video features failed (%s)", trial_id, exc)
                frames = None

    n_kin = int(len(kin))
    gestures_pdf = (
        gesture_metrics(kin, transcript, frames, SAMPLE_RATE_HZ)
        if transcript is not None and len(transcript)
        else pd.DataFrame()
    )

    trial_row = {
        "trial_id": trial_id,
        "task": task,
        "subject": meta.get("subject"),
        "repetition": meta.get("repetition"),
        "skill_level": meta.get("skill_level"),
        "grs_total": meta.get("grs_total"),
        "duration_s": float(max(n_kin - 1, 0) / SAMPLE_RATE_HZ),
        "n_frames": int(len(frames)) if frames is not None else n_kin,
        "n_kinematic_samples": n_kin,
        "sample_rate_hz": SAMPLE_RATE_HZ,
        "video_path": files.get("video_path"),
        "kinematics_path": files.get("kin_path"),
        "transcription_path": files.get("transcript_path"),
        "n_gestures": int(len(gestures_pdf)),
        "processed_at": processed_at,
        "pipeline_version": pipeline_version,
    }

    kin_rows = []
    for _, r in kin.iterrows():
        kin_rows.append(
            {
                "trial_id": trial_id,
                "task": task,
                "frame_idx": int(r["frame_idx"]),
                "t_s": float(r["t_s"]),
                "mtml": _arm_struct(r, "mtml"),
                "mtmr": _arm_struct(r, "mtmr"),
                "psml": _arm_struct(r, "psml"),
                "psmr": _arm_struct(r, "psmr"),
                "gesture": r["gesture"] if isinstance(r["gesture"], str) else None,
            }
        )

    gesture_rows = []
    for _, g in gestures_pdf.iterrows():
        row = {
            c: (None if pd.isna(g.get(c)) else g.get(c))
            for c in ddl.GESTURES.column_names
            if c in g.index
        }
        row.update(
            {
                "trial_id": trial_id,
                "task": task,
                "subject": meta.get("subject"),
                "skill_level": meta.get("skill_level"),
            }
        )
        for c in ("start_frame", "end_frame", "gripper_cycles", "n_frames"):
            if row.get(c) is not None:
                row[c] = int(row[c])
        gesture_rows.append(row)

    frame_rows = []
    if frames is not None:
        labels = kin.set_index("frame_idx")["gesture"] if "gesture" in kin else None
        for _, f in frames.iterrows():
            idx = int(f["frame_idx"])
            gesture = labels.get(idx) if labels is not None else None
            frame_rows.append(
                {
                    "trial_id": trial_id,
                    "task": task,
                    "frame_idx": idx,
                    "t_s": float(f["t_s"]),
                    "mean_intensity": float(f["mean_intensity"]),
                    "motion_energy": float(f["motion_energy"]),
                    "sharpness": float(f["sharpness"]),
                    "gesture": gesture if isinstance(gesture, str) else None,
                }
            )
    return {
        "trials": [trial_row],
        "kinematics": kin_rows,
        "gestures": gesture_rows,
        "frame_features": frame_rows,
    }


TABLE_FOR = {
    "trials": ddl.TRIALS,
    "kinematics": ddl.KINEMATICS,
    "gestures": ddl.GESTURES,
    "frame_features": ddl.FRAME_FEATURES,
}


def process_partition(
    items: Iterable, meta_b, pipeline_version: str, processed_at: dt.datetime
) -> Iterator[tuple[str, tuple]]:
    meta_all = meta_b.value
    for trial_id, files in items:
        try:
            result = process_trial(
                trial_id, files, meta_all.get(trial_id, {}), pipeline_version, processed_at
            )
        except Exception as exc:
            log.error("trial %s failed: %s", trial_id, exc)
            continue
        for kind, rows in result.items():
            tdef = TABLE_FOR[kind]
            for row in rows:
                yield kind, ddl.to_tuple(tdef, row)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--root",
        default=None,
        help="dataset root (local path or s3a://...); default s3a://<raw bucket>/jigsaws",
    )
    p.add_argument("--task", default=None, help="restrict to one task, e.g. Suturing")
    p.add_argument("--skip-es", action="store_true")
    return p.parse_args(argv)


def run(spark, settings: Settings, root: str, task: str | None, skip_es: bool = False) -> dict:
    ensure_tables(spark)
    meta = trial_metadata(spark, root, task)
    log.info("discovered %d trials under %s", len(meta), root)
    if not meta:
        return {k: 0 for k in TABLE_FOR}

    meta_b = spark.sparkContext.broadcast(meta)
    processed_at = dt.datetime.now(tz=dt.UTC)
    inputs = gather_inputs(spark, root, task).repartition(max(len(meta), 1))
    decoded = inputs.mapPartitions(
        lambda it: process_partition(it, meta_b, settings.pipeline_version, processed_at)
    ).persist()

    counts = {}
    frames = {}
    for kind, tdef in TABLE_FOR.items():
        rows = decoded.filter(lambda kv, k=kind: kv[0] == k).map(lambda kv: kv[1])
        frames[kind] = spark.createDataFrame(rows, schema=ddl.struct_type(tdef.name)).persist()
        counts[kind] = frames[kind].count()
    log.info("rows: %s", counts)

    merge_iceberg(spark, frames["trials"], ddl.TRIALS.name, ["trial_id"])
    write_iceberg(frames["kinematics"], ddl.KINEMATICS.name, mode="overwrite_partitions")
    merge_iceberg(spark, frames["gestures"], ddl.GESTURES.name, ["trial_id", "start_frame"])
    write_iceberg(frames["frame_features"], ddl.FRAME_FEATURES.name, mode="overwrite_partitions")

    if not skip_es and counts["gestures"]:
        es_ensure_index(ES_INDEX_GESTURES, settings=settings)
        es_bulk(
            rows_as_dicts(frames["gestures"]),
            ES_INDEX_GESTURES,
            id_fn=lambda r: f"{r['trial_id']}|{r['start_frame']}",
            settings=settings,
        )
    return counts


def main(argv=None) -> int:
    args = parse_args(argv)
    configure_logging()
    settings = get_settings()
    spark = build_spark("rtdaas-batch-jigsaws", settings)
    result = run(spark, settings, args.root or settings.raw_jigsaws_uri, args.task, args.skip_es)
    log.info("batch_jigsaws done: %s", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
