from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from rtdaas.jigsaws.layout import ARM_FIELDS, ARM_ORDER
from rtdaas.metrics.gestures import gesture_metrics


def _kin(n=90, hz=30.0):
    cols = {"frame_idx": np.arange(n), "t_s": np.arange(n) / hz}
    for arm in ARM_ORDER:
        for f in ARM_FIELDS:
            cols[f"{arm}_{f}"] = np.zeros(n)
    df = pd.DataFrame(cols)
    df["psml_x"] = np.arange(n) * 0.001  # 0.03 m/s along x
    df["psmr_gripper"] = (np.arange(n) % 30 >= 15).astype(
        float
    )  # opens mid-way through each second
    return df


def test_gesture_metrics_per_segment():
    kin = _kin()
    transcript = pd.DataFrame(
        {"start_frame": [1, 31, 61], "end_frame": [30, 60, 90], "gesture": ["G1", "G2", "G3"]}
    )
    frames = pd.DataFrame(
        {
            "frame_idx": np.arange(90),
            "t_s": np.arange(90) / 30,
            "motion_energy": np.full(90, 0.2),
            "sharpness": 1.0,
            "mean_intensity": 0.5,
        }
    )
    g = gesture_metrics(kin, transcript, frames)
    assert list(g["gesture"]) == ["G1", "G2", "G3"]
    assert (g["n_frames"] == 30).all()
    assert g["duration_s"].iloc[0] == pytest.approx(1.0, abs=0.05)
    assert g["path_length_psml"].iloc[0] == pytest.approx(0.029, abs=0.002)
    assert g["mean_speed_psml"].iloc[0] == pytest.approx(0.03, rel=0.05)
    assert g["path_length_psmr"].iloc[0] == 0
    assert list(g["gripper_cycles"]) == [1, 1, 1]
    assert np.allclose(g["motion_energy_mean"], 0.2)


def test_gesture_metrics_without_frames_gives_nan_energy():
    kin = _kin(30)
    g = gesture_metrics(
        kin, pd.DataFrame({"start_frame": [1], "end_frame": [30], "gesture": ["G5"]}), None
    )
    assert len(g) == 1 and np.isnan(g["motion_energy_mean"].iloc[0])


def _opening_counts(gripper, frames=None):
    """gripper_cycles for one segment per ``frames`` (default all), with only the left arm moving."""
    n = len(gripper)
    kin = pd.DataFrame(
        {
            "frame_idx": np.arange(n),
            "psml_x": 0.0, "psml_y": 0.0, "psml_z": 0.0,
            "psmr_x": 0.0, "psmr_y": 0.0, "psmr_z": 0.0,
            "psml_gripper": gripper,
            "psmr_gripper": -0.9,
        }
    )  # fmt: skip
    spans = frames or [(1, n)]
    transcript = pd.DataFrame(
        {"start_frame": [a for a, _ in spans], "end_frame": [b for _, b in spans], "gesture": ["G1"] * len(spans)}
    )  # fmt: skip
    return list(gesture_metrics(kin, transcript, None)["gripper_cycles"])


def _steps(*levels, each=20):
    return np.concatenate([np.full(each, float(level)) for level in levels])


def test_openings_use_the_arms_own_levels():
    # Real JIGSAWS levels never reach the synthetic data's 0.5 on many arms.
    assert _opening_counts(_steps(-0.9, -0.1, -0.9, -0.1, -0.9)) == [2]
    assert _opening_counts(_steps(0, 1, 0, 1, 0, 1, 0)) == [3]


def test_noise_around_the_midpoint_is_not_more_openings():
    rng = np.random.default_rng(0)
    gripper = _steps(-0.9, -0.1, -0.9) + rng.normal(0, 0.01, 60)
    assert _opening_counts(gripper) == [1]
    gripper[20:40] = -0.5 + rng.normal(0, 0.05, 20)  # a wobble halfway is not an opening
    gripper = np.concatenate([gripper, _steps(0.0, -0.9)])
    assert _opening_counts(gripper) == [1]


def test_an_opening_counts_in_the_segment_where_it_happens():
    gripper = _steps(-0.9, -0.1, -0.9, -0.9, -0.1, -0.9)
    assert _opening_counts(gripper, [(1, 30), (31, 70), (71, 120)]) == [1, 0, 1]


def test_a_gripper_that_barely_moves_never_opens():
    assert _opening_counts(np.linspace(-0.9, -0.85, 60)) == [0]
    assert _opening_counts(_steps(-0.9, np.nan, -0.1, -0.9)) == [1]
