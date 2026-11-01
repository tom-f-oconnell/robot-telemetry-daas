from __future__ import annotations

import os
import shutil
import subprocess

import numpy as np
import pytest

from rtdaas.jigsaws.layout import (
    ARM_FIELDS,
    ARM_ORDER,
    discover_trials,
    label_frames,
    parse_kinematics,
    parse_meta_file,
    parse_transcription,
    parse_trial_id,
)
from rtdaas.jigsaws.synth import generate_dataset
from rtdaas.jigsaws.video import VideoReadError, frame_features, video_info
from rtdaas.metrics.gestures import gesture_metrics


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    root = tmp_path_factory.mktemp("jigsaws")
    trials = generate_dataset(root, n_trials_per_task=1, seed=5, duration_s=2.0)
    return root, trials


def test_layout_matches_real_jigsaws(dataset):
    root, trials = dataset
    assert len(trials) == 3
    for t in trials:
        assert (root / t.task / "kinematics" / "AllGestures" / f"{t.trial_id}.txt").is_file()
        assert (root / t.task / "transcriptions" / f"{t.trial_id}.txt").is_file()
        assert (root / t.task / f"meta_file_{t.task}.txt").is_file()
        assert t.video_path is not None and t.video_path.name.startswith(f"{t.trial_id}_capture1")
    assert parse_trial_id("Suturing_B001") == ("Suturing", "B", 1)
    assert parse_trial_id("Knot_Tying_I004") == ("Knot_Tying", "I", 4)


def test_discover_and_meta(dataset):
    root, _ = dataset
    found = discover_trials(root)
    assert {t.task for t in found} == {"Suturing", "Knot_Tying", "Needle_Passing"}
    assert all(t.skill_level in {"novice", "intermediate", "expert"} for t in found)
    meta = parse_meta_file(root / "Suturing" / "meta_file_Suturing.txt")
    skill, grs = meta["Suturing_B001"]
    assert skill in {"novice", "intermediate", "expert"} and isinstance(grs, int)
    assert [t.task for t in discover_trials(root, tasks=["Suturing"])] == ["Suturing"]


def test_parse_kinematics_shape_and_columns(dataset):
    root, trials = dataset
    t = next(t for t in trials if t.task == "Suturing")
    kin = parse_kinematics(t.kinematics_path)
    expected = ["frame_idx", "t_s"] + [f"{a}_{f}" for a in ARM_ORDER for f in ARM_FIELDS]
    assert list(kin.columns) == expected and len(expected) == 78
    assert len(kin) == 60 and kin["frame_idx"].iloc[0] == 0
    assert kin["t_s"].iloc[-1] == pytest.approx(59 / 30)
    # rotation matrices should be close to orthonormal (synthetic data perturbs identity)
    r = (
        kin[[f"psml_r{i}{j}" for i in (1, 2, 3) for j in (1, 2, 3)]]
        .iloc[0]
        .to_numpy()
        .reshape(3, 3)
    )
    assert np.allclose(r @ r.T, np.eye(3), atol=0.2)


def test_transcription_and_labels(dataset):
    root, trials = dataset
    t = trials[0]
    tr = parse_transcription(t.transcription_path)
    assert list(tr.columns) == ["start_frame", "end_frame", "gesture"]
    assert (tr["start_frame"] >= 1).all() and (tr["end_frame"] >= tr["start_frame"]).all()
    kin = label_frames(parse_kinematics(t.kinematics_path), tr)
    first = tr.iloc[0]
    assert (
        kin.loc[kin["frame_idx"] == first["start_frame"] - 1, "gesture"].iloc[0] == first["gesture"]
    )
    assert kin["gesture"].notna().sum() == sum(
        int(e - s + 1) for s, e in zip(tr["start_frame"], tr["end_frame"], strict=True)
    )


def test_frame_features_track_motion(dataset):
    root, trials = dataset
    t = trials[0]
    info = video_info(t.video_path)
    assert info["fps"] == pytest.approx(30, abs=1) and info["n_frames"] == 60
    ff = frame_features(t.video_path)
    assert list(ff.columns) == ["frame_idx", "t_s", "mean_intensity", "motion_energy", "sharpness"]
    assert (
        len(ff) == 60
        and ff["motion_energy"].iloc[0] == 0
        and (ff["motion_energy"].iloc[1:] > 0).all()
    )
    assert 0 < ff["mean_intensity"].mean() < 1
    # the rendered discs follow the synthetic PSM tips, so video motion correlates with kinematic speed
    kin = parse_kinematics(t.kinematics_path)
    speed = np.hypot(kin["psml_vx"], kin["psml_vy"]) + np.hypot(kin["psmr_vx"], kin["psmr_vy"])
    corr = np.corrcoef(speed.iloc[1:], ff["motion_energy"].iloc[1:])[0, 1]
    assert corr > 0.3, corr


def test_end_to_end_gesture_metrics(dataset):
    root, trials = dataset
    t = trials[0]
    kin = parse_kinematics(t.kinematics_path)
    tr = parse_transcription(t.transcription_path)
    g = gesture_metrics(kin, tr, frame_features(t.video_path))
    assert len(g) == len(tr) and (g["n_frames"] > 0).all() and g["motion_energy_mean"].notna().all()


def test_frame_features_missing_file(tmp_path):
    with pytest.raises(VideoReadError):
        frame_features(tmp_path / "nope.mp4")


def test_discover_skips_empty_kinematics(dataset, tmp_path):
    root = tmp_path / "root"
    shutil.copytree(dataset[0], root)
    (root / "Suturing" / "kinematics" / "AllGestures" / "Suturing_Z009.txt").write_text("")
    assert not any(t.trial_id == "Suturing_Z009" for t in discover_trials(root))


@pytest.fixture
def download(dataset, tmp_path):
    """Shaped like the real download: two eyes of AVI per trial, and kinematics with nothing else."""
    imageio_ffmpeg = pytest.importorskip("imageio_ffmpeg")
    root = tmp_path / "download"
    shutil.copytree(dataset[0], root)
    for mp4 in root.glob("*/video/*_capture1.mp4"):
        avi = mp4.with_suffix(".avi")
        ffmpeg = [imageio_ffmpeg.get_ffmpeg_exe(), "-v", "error", "-i", str(mp4), "-c:v", "mpeg4"]
        subprocess.run([*ffmpeg, str(avi)], check=True)
        shutil.copy(avi, str(avi).replace("_capture1", "_capture2"))
        mp4.unlink()
    kin = root / "Suturing" / "kinematics" / "AllGestures"
    shutil.copy(next(kin.glob("*.txt")), kin / "Suturing_Z001.txt")  # no video, transcript or meta
    (kin / "Suturing_Z002.txt").write_text("")
    (root / "Suturing" / "readme.txt").write_text("not part of the dataset")
    return root


def test_import_keeps_complete_trials_with_one_browser_ready_video(download, tmp_path):
    from rtdaas.jigsaws.importer import import_dataset

    out = tmp_path / "stage"
    result = import_dataset(download, out, jobs=2)

    assert result["skipped"] == ["Suturing_Z001"]
    trials = discover_trials(out)
    assert (
        len(trials)
        == result["trials"]
        == result["transcoded"]
        == len(discover_trials(download)) - 1
    )
    for t in trials:
        assert t.video_path.name == f"{t.trial_id}_capture1.mp4"
        assert t.transcription_path is not None and t.skill_level != "unknown"
        data = t.video_path.read_bytes()
        assert b"avc1" in data[:4096], "not H.264, or the index is not at the start"
        source = next(s for s in discover_trials(download) if s.trial_id == t.trial_id)
        assert source.video_path.name.endswith("_capture1.avi")
        assert video_info(t.video_path)["n_frames"] == video_info(source.video_path)["n_frames"]
    assert not list(out.rglob("*capture2*")) and not list(out.rglob("readme.txt"))
    assert not list(out.rglob("*.part.mp4"))


def test_import_again_transcodes_only_changed_videos(download, tmp_path):
    from rtdaas.jigsaws.importer import import_dataset

    out = tmp_path / "stage"
    n = import_dataset(download, out, jobs=2)["trials"]
    assert import_dataset(download, out, jobs=2)["transcoded"] == 0

    source = next(download.glob("*/video/*_capture1.avi"))
    os.utime(source, (source.stat().st_atime, source.stat().st_mtime + 3600))
    assert import_dataset(download, out, jobs=2) == {
        "trials": n,
        "transcoded": 1,
        "skipped": ["Suturing_Z001"],
    }


def test_import_reports_a_bad_video_and_finishes_the_rest(download, tmp_path):
    from rtdaas.jigsaws.importer import import_dataset

    next(download.glob("*/video/*_capture1.avi")).write_bytes(b"not a video")
    out = tmp_path / "stage"
    with pytest.raises(RuntimeError, match="1 of .* videos failed"):
        import_dataset(download, out, jobs=2)
    assert len(list(out.rglob("*.mp4"))) == len(discover_trials(download)) - 2
    assert not list(out.rglob("*.part.mp4"))
