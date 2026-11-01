from __future__ import annotations

import numpy as np
import pytest

from rtdaas.metrics import da_vinci as dv


def _rot(roll, pitch, yaw):
    cr, sr, cp, sp, cy, sy = (
        np.cos(roll),
        np.sin(roll),
        np.cos(pitch),
        np.sin(pitch),
        np.cos(yaw),
        np.sin(yaw),
    )
    rx = np.array([[1, 0, 0], [0, cr, -sr], [0, sr, cr]])
    ry = np.array([[cp, 0, sp], [0, 1, 0], [-sp, 0, cp]])
    rz = np.array([[cy, -sy, 0], [sy, cy, 0], [0, 0, 1]])
    return rz @ ry @ rx


def _link(xyz, rpy, angle=0.0, slide=0.0):
    out = np.eye(4)
    out[:3, :3], out[:3, 3] = _rot(*rpy), xyz
    motion = np.eye(4)
    motion[:3, :3], motion[2, 3] = _rot(0, 0, angle), slide
    return out @ motion


def _urdf_tip(q: dict[str, float]) -> np.ndarray:
    """dvrk_model's URDF, joint by joint, to check the closed forms against (and robot-sim.js has)."""
    half = np.pi / 2
    return (
        _link((0, 0, 0), (0, -half, half), q["yaw"])
        @ _link((0, 0, 0), (-half, -half, 0), q["pitch"])
        @ _link((0, dv.L_RCC, 0), (half, 0, 0), slide=q["insertion"])
        @ _link((0, 0, dv.L_TOOL), (0, 0, 0), q["roll"])
        @ _link((0, 0, 0), (-half, -half, 0), q["wrist_pitch"])
        @ _link((dv.L_PITCH_TO_YAW, 0, 0), (-half, -half, 0), q["wrist_yaw"])
        @ _link((0, dv.L_YAW_TO_TIP, 0), (0, half, half))
    )


@pytest.fixture
def poses():
    rng = np.random.default_rng(1)
    n = 200
    q = {
        "yaw": rng.uniform(-1.2, 1.2, n),
        "pitch": rng.uniform(-0.8, 0.8, n),
        "insertion": rng.uniform(0.05, 0.24, n),
        "roll": rng.uniform(-3, 3, n),
        "wrist_pitch": rng.uniform(-1.3, 1.3, n),
        "wrist_yaw": rng.uniform(-1.3, 1.3, n),
    }
    tips = np.array([_urdf_tip({k: v[i] for k, v in q.items()}) for i in range(n)])
    return q, tips[:, :3, 3], tips[:, :3, :3]


def test_forward_matches_the_urdf(poses):
    q, positions, rotations = poses
    p, r = dv.psm_forward(q)
    assert p == pytest.approx(positions, abs=1e-12)
    assert r == pytest.approx(rotations, abs=1e-12)


def test_inverse_recovers_joints_and_pose(poses):
    q, positions, rotations = poses
    solved = dv.psm_inverse(positions, rotations)
    p, r = dv.psm_forward(solved)
    assert p == pytest.approx(positions, abs=1e-9)
    assert r == pytest.approx(rotations, abs=1e-9)
    for joint in ("yaw", "pitch", "insertion", "wrist_pitch", "wrist_yaw"):
        assert solved[joint] == pytest.approx(q[joint], abs=1e-9)


def test_inverse_without_rotations_holds_the_tool_straight():
    tip = np.array([[0.02, -0.03, -0.12]])
    q = dv.psm_inverse(tip)
    assert dv.psm_forward(q)[0] == pytest.approx(tip, abs=1e-9)
    assert q["wrist_pitch"] == q["wrist_yaw"] == pytest.approx(0.0)


def test_inverse_limits_the_wrist_to_what_it_can_reach():
    # Jaws pointing back at the pivot: the wrist cannot do that.
    q = dv.psm_inverse(np.array([[0.0, 0.0, -0.12]]), np.eye(3)[None])
    assert abs(q["wrist_pitch"][0]) <= dv.WRIST_LIMIT and abs(q["wrist_yaw"][0]) <= dv.WRIST_LIMIT


def test_roll_is_continuous_across_the_wrap():
    q = {j: np.zeros(40) for j in dv.JOINTS}
    q["insertion"] += 0.15
    q["roll"] = np.linspace(2.8, 3.6, 40)  # past pi, where atan2 would jump
    p, r = dv.psm_forward(q)
    assert np.abs(np.diff(dv.psm_inverse(p, r)["roll"])).max() < 0.1


@pytest.mark.parametrize(
    "arms,system",
    [
        (["PSM1", "ECM"], dv.SYSTEM),
        (["psm2"], dv.SYSTEM),
        (["ECM"], None),
        (["UR5"], None),
        ([], None),
    ],
)
def test_system_is_recognised_from_its_arms(arms, system):
    assert dv.system_for_arms(arms) == system


def _trial(n=300, offset=(0.03, 0.0, -0.04)):
    """Two tools converging from either side, down being +z in the data, meeting at frame 100."""
    t = np.arange(n) / 30.0
    samples = []
    for i in range(n):
        wobble = 0.01 * np.sin(t[i])
        row = {"frame_idx": i, "t_s": float(t[i])}
        for arm, side, shift in (("psml", -1.0, np.zeros(3)), ("psmr", 1.0, np.array(offset))):
            apart = 0.0 if i == 100 else 0.02 * side
            p = np.array([apart, wobble, 0.0]) + shift
            approach = np.array([-side * 0.5, 0.0, 0.8])
            z = approach / np.linalg.norm(approach)
            x = np.array([0.8, 0.0, side * 0.5])
            x /= np.linalg.norm(x)
            rot = np.column_stack([x, np.cross(z, x), z])
            row |= {f"{arm}_{a}": p[k] for k, a in enumerate("xyz")}
            row |= {f"{arm}_r{i_ + 1}{j + 1}": rot[i_, j] for i_ in range(3) for j in range(3)}
            row[f"{arm}_gripper"] = float(np.sin(t[i]))
        samples.append(row)
    return samples


def test_trial_scene_is_posed_by_the_recording():
    scene = dv.trial_scene(_trial(), [{"gesture": "G4", "start_frame": 90, "end_frame": 110}])
    assert scene["system"] == dv.SYSTEM and scene["duration_s"] == pytest.approx(299 / 30)
    left, right = scene["arms"]
    assert (left["name"], right["name"]) == ("PSML", "PSMR")
    for arm in (left, right):
        q = {j: np.array(arm["q"][j]) for j in dv.JOINTS}
        base = np.array(arm["base"]).reshape(4, 4)
        tip, _ = dv.psm_forward(q)
        assert tip @ base[:3, :3].T + base[:3, 3] == pytest.approx(np.array(arm["tip"]), abs=1e-4)
        assert np.linalg.det(base[:3, :3]) == pytest.approx(1.0)
        assert 0.0 <= min(arm["q"]["jaw"]) and max(arm["q"]["jaw"]) <= dv.MAX_JAW
        assert np.abs(q["yaw"]).max() < np.radians(91) and 0 < q["insertion"].min()
    tips = np.array(left["tip"]), np.array(right["tip"])
    # Left is on the left, and the arm offset is gone: the graspers meet in the transfer.
    assert tips[0][:, 0].mean() < tips[1][:, 0].mean()
    assert np.linalg.norm(tips[0][100] - tips[1][100]) < 1e-3
    # Pivots are above the work.
    assert np.array(left["base"]).reshape(4, 4)[2, 3] > tips[0][:, 2].max()


def test_arm_offset_falls_back_when_a_trial_has_no_transfer():
    left, right = np.zeros((10, 3)), np.ones((10, 3))
    frames = np.arange(10)
    assert dv.arm_offset(left, right, frames, []) == pytest.approx(dv.DEFAULT_ARM_OFFSET)
    other = [{"gesture": "G1", "start_frame": 1, "end_frame": 5}]
    assert dv.arm_offset(left, right, frames, other) == pytest.approx(dv.DEFAULT_ARM_OFFSET)


def test_trial_scene_without_samples_has_no_system():
    assert dv.trial_scene([], []) == {"system": None}


def _points(n=20, z=-0.12, quaternion=(1.0, 0.0, 0.0, 0.0)):
    start = np.datetime64("2026-10-28T10:00:00")
    return [
        {
            "ts": f"{start + np.timedelta64(i * 20, 'ms')}+00:00",
            "x": 0.01 * np.sin(i / 3),
            "y": 0.01 * np.cos(i / 3),
            "z": z,
            "qx": quaternion[0],
            "qy": quaternion[1],
            "qz": quaternion[2],
            "qw": quaternion[3],
            "jaw_angle": 0.3,
            "speed": 0.01,
        }
        for i in range(n)
    ]


def test_session_scene_uses_each_arm_frame_as_it_is():
    # A half turn about x: jaws along -z, pointing away from a pivot above them, as on a dVRK.
    scene = dv.session_scene({"PSM1": _points(), "PSM2": _points(), "ECM": _points()})
    assert scene["system"] == dv.SYSTEM
    arms = {a["name"]: a for a in scene["arms"]}
    assert arms["ECM"]["kind"] == "ecm" and arms["PSM1"]["kind"] == "psm"
    assert "held straight" not in scene["note"]
    assert arms["PSM1"]["t_s"][1] == pytest.approx(0.02)
    base = np.array(arms["PSM1"]["base"]).reshape(4, 4)
    q = {j: np.array(arms["PSM1"]["q"][j]) for j in dv.JOINTS}
    expected = dv.psm_forward(q)[0] @ base[:3, :3].T + base[:3, 3]
    assert np.array(arms["PSM1"]["tip"]) == pytest.approx(expected, abs=1e-4)
    # The first point, in the arm's own frame, comes back out of the joints.
    assert dv.psm_forward(q)[0][0] == pytest.approx([0.0, 0.01, -0.12], abs=2e-3)


def test_session_scene_holds_tools_straight_when_rotations_do_not_fit():
    # The ROS-free simulator's rotations stay near identity: jaws pointing back up at the pivot.
    scene = dv.session_scene({"PSM1": _points(quaternion=(0.0, 0.0, 0.0, 1.0))})
    assert "held straight" in scene["note"]
    assert scene["arms"][0]["q"]["wrist_pitch"][0] == 0.0


def test_session_scene_needs_a_da_vinci_arm():
    assert dv.session_scene({"UR5": _points()}) == {"system": None}
    assert dv.session_scene({"PSM1": []}) == {"system": None}
