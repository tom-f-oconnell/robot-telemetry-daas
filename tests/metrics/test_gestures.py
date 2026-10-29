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
    df["psmr_gripper"] = (np.arange(n) % 30 >= 15).astype(float)  # opens mid-way through each second
    return df


def test_gesture_metrics_per_segment():
    kin = _kin()
    transcript = pd.DataFrame({"start_frame": [1, 31, 61], "end_frame": [30, 60, 90], "gesture": ["G1", "G2", "G3"]})
    frames = pd.DataFrame({"frame_idx": np.arange(90), "t_s": np.arange(90) / 30, "motion_energy": np.full(90, 0.2), "sharpness": 1.0, "mean_intensity": 0.5})
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
    g = gesture_metrics(kin, pd.DataFrame({"start_frame": [1], "end_frame": [30], "gesture": ["G5"]}), None)
    assert len(g) == 1 and np.isnan(g["motion_energy_mean"].iloc[0])
