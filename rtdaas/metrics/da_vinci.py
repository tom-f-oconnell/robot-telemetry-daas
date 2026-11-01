"""Replay of recorded tool poses on a da Vinci arm, for the UI's simulation (pure numpy).

Neither data source records every joint of a patient-side manipulator (PSM), so this works them out
with inverse kinematics on dVRK's Classic PSM (jhu-dvrk/dvrk_model, ``PSM_base.urdf.xacro`` and the
Large Needle Driver's instrument files). The UI's ``static/robot-sim.js`` holds the same joints.

The two sources differ in what they say about where the arms are:

* Telemetry is in each arm's own frame, the pivot (remote centre of motion, RCM) at the origin. It
  is used as it is, and only the arms' places around the table are chosen here.
* JIGSAWS records tool tips in a frame of its own, and no pivots. :func:`trial_scene` estimates a
  scene from the trial: "down" and "right" from how the two tools approach the work, and each pivot
  some way back along its tool. The tips are then where the data says; the rest of each arm is a
  plausible fit, not a measurement.
"""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

from rtdaas.metrics.kinematics import to_utc

SYSTEM = "da_vinci"

L_RCC = 0.4318  # m, RCM to the instrument mount at zero insertion
L_TOOL = 0.4162  # mount to wrist pitch axis
L_PITCH_TO_YAW = 0.0091
L_YAW_TO_TIP = 0.0102
WRIST_LIMIT = 1.39626  # rad, wrist pitch and yaw
JOINTS = ("yaw", "pitch", "insertion", "roll", "wrist_pitch", "wrist_yaw")

# Mean right-minus-left tip offset at the graspers' closest approach during needle transfers (G4),
# over JIGSAWS' 39 suturing trials. It is close to constant per surgeon, and the graspers should
# meet there, so the two arms' data are not quite in one frame. Used when a trial has no G4.
DEFAULT_ARM_OFFSET = np.array([0.032, -0.003, -0.038])
TRANSFER_GESTURE = "G4"
PIVOT_DISTANCE = 0.14  # m, an estimate: tip to pivot, for trials that do not record the pivot
MAX_JAW = 1.0  # rad, the display's fully open
YAW_AXIS_RISE = np.radians(25)  # how far each arm's yaw axis rises toward its mount
YAW_AXIS_FLARE = 0.6  # how far apart the arms' mounts splay
MEAN_PITCH = np.radians(25)

# Tool axes are columns; the URDF's fixed rotations between joints, all of which turn about z.
_TO_YAW = np.array([[0.0, -1.0, 0.0], [0.0, 0.0, -1.0], [1.0, 0.0, 0.0]])  # rpy(0, -pi/2, pi/2)
_CYCLE = np.array([[0.0, 1.0, 0.0], [0.0, 0.0, 1.0], [1.0, 0.0, 0.0]])  # rpy(-pi/2, -pi/2, 0)
_TO_ADAPTOR = np.array([[1.0, 0.0, 0.0], [0.0, 0.0, -1.0], [0.0, 1.0, 0.0]])  # rpy(pi/2, 0, 0)
_TO_TIP = np.array([[0.0, 0.0, 1.0], [1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]).T @ np.diag([-1, -1, 1.0])


def system_for_arms(arms: Iterable[str]) -> str | None:
    """The robot system that uses these arm names, or None if none we can simulate does."""
    names = [str(a).upper() for a in arms]
    return SYSTEM if any(a.startswith("PSM") for a in names) else None


def _rz(angle: np.ndarray) -> np.ndarray:
    c, s = np.cos(angle), np.sin(angle)
    out = np.zeros((*np.shape(angle), 3, 3))
    out[..., 0, 0], out[..., 0, 1], out[..., 1, 0], out[..., 1, 1] = c, -s, s, c
    out[..., 2, 2] = 1.0
    return out


def _unit(v: np.ndarray) -> np.ndarray:
    return v / np.linalg.norm(v, axis=-1, keepdims=True)


def _adaptor_rotation(yaw: np.ndarray, pitch: np.ndarray) -> np.ndarray:
    return _TO_YAW @ _rz(yaw) @ _CYCLE @ _rz(pitch) @ _TO_ADAPTOR


def psm_forward(q: dict[str, np.ndarray]) -> tuple[np.ndarray, np.ndarray]:
    """Tool tip position and rotation (tool axes as columns) in the RCM frame, for joints ``q``."""
    adaptor = _adaptor_rotation(q["yaw"], q["pitch"])
    # The instrument mount starts L_RCC behind the RCM and travels with insertion toward the tip.
    shaft = adaptor[..., :, 2]
    pitch_point = shaft * (q["insertion"] - L_RCC + L_TOOL)[..., None]
    wrist = adaptor @ _rz(q["roll"]) @ _CYCLE @ _rz(q["wrist_pitch"])
    yaw_point = pitch_point + wrist[..., :, 0] * L_PITCH_TO_YAW
    tip_frame = wrist @ _CYCLE @ _rz(q["wrist_yaw"])
    position = yaw_point + tip_frame[..., :, 1] * L_YAW_TO_TIP
    return position, tip_frame @ _TO_TIP


def psm_inverse(
    positions: np.ndarray, rotations: np.ndarray | None = None
) -> dict[str, np.ndarray]:
    """Joints that put the tool tip at ``positions`` (n, 3) with ``rotations`` (n, 3, 3).

    Both are in the RCM frame, tool axes as columns: z runs along the jaws, and the wrist's yaw axis
    is x. Wrist angles are limited to what the instrument can reach, so a pose out of its reach
    gets the nearest it can. Without ``rotations`` the tool is held straight in line with its shaft.
    """
    p = np.asarray(positions, dtype=float)
    if rotations is None:
        shaft = _unit(p)
        pitch_point = p - shaft * (L_PITCH_TO_YAW + L_YAW_TO_TIP)
    else:
        rot = np.asarray(rotations, dtype=float)
        x, z = rot[..., :, 0], rot[..., :, 2]
        yaw_point = p - z * L_YAW_TO_TIP
        # The wrist link is square to the yaw axis and in line with the RCM, seen along that axis.
        to_rcm = -yaw_point
        link = _unit(to_rcm - x * (to_rcm * x).sum(-1, keepdims=True))
        pitch_point = yaw_point + link * L_PITCH_TO_YAW

    px, py, pz = pitch_point[..., 0], pitch_point[..., 1], pitch_point[..., 2]
    q = {
        "yaw": np.arctan2(px, -pz),
        "pitch": -np.arctan2(py, np.hypot(px, pz)),
        "insertion": np.linalg.norm(pitch_point, axis=-1) + L_RCC - L_TOOL,
    }
    if rotations is None:
        for joint in ("roll", "wrist_pitch", "wrist_yaw"):
            q[joint] = np.zeros_like(px)
        return q

    adaptor = _adaptor_rotation(q["yaw"], q["pitch"])
    wrist = np.swapaxes(adaptor, -1, -2) @ rot @ _TO_TIP.T
    wrist_pitch = np.arctan2(-wrist[..., 2, 2], np.hypot(wrist[..., 0, 2], wrist[..., 1, 2]))
    roll = np.arctan2(wrist[..., 1, 2], wrist[..., 0, 2])
    upto_yaw = _rz(roll) @ _CYCLE @ _rz(wrist_pitch) @ _CYCLE
    remainder = np.swapaxes(upto_yaw, -1, -2) @ wrist
    q["roll"] = np.unwrap(roll)
    q["wrist_pitch"] = np.clip(wrist_pitch, -WRIST_LIMIT, WRIST_LIMIT)
    q["wrist_yaw"] = np.clip(
        np.arctan2(remainder[..., 1, 0], remainder[..., 0, 0]), -WRIST_LIMIT, WRIST_LIMIT
    )
    return q


def wrist_reachable(positions: np.ndarray, rotations: np.ndarray) -> bool:
    """Whether recorded tool rotations look like a tool on this arm at all.

    Most samples must have the jaws pointing away from the pivot, within the wrist's reach. The
    ROS-free simulator's rotations stay close to identity, for example, which does not.
    """
    shaft = _unit(np.asarray(positions, dtype=float))
    approach = np.asarray(rotations, dtype=float)[..., :, 2]
    return bool(np.median((shaft * approach).sum(-1)) > np.cos(WRIST_LIMIT))


def arm_offset(
    left: np.ndarray, right: np.ndarray, frames: np.ndarray, gestures: list[dict]
) -> np.ndarray:
    """Right-minus-left tip offset to remove so that the graspers meet during needle transfers."""
    gap = right - left
    distance = np.linalg.norm(gap, axis=1)
    closest = []
    for g in gestures:
        if g.get("gesture") != TRANSFER_GESTURE:
            continue
        # transcript frames are 1-based and inclusive
        inside = np.flatnonzero((frames >= g["start_frame"] - 1) & (frames <= g["end_frame"] - 1))
        if len(inside):
            closest.append(gap[inside[np.argmin(distance[inside])]])
    return np.median(closest, axis=0) if closest else DEFAULT_ARM_OFFSET


def _matrix(rotation: np.ndarray, translation: np.ndarray) -> np.ndarray:
    out = np.eye(4)
    out[:3, :3], out[:3, 3] = rotation, translation
    return out


def _base_rotation(approach: np.ndarray, flare: float) -> np.ndarray:
    """An arm's base frame in the scene (x right, y away from the viewer, z up).

    Its yaw axis, -y, rises away from the viewer toward the arm's mount, which ``flare`` splays
    sideways. A shaft is square to that axis at zero pitch, and square to a shaft leaning outward, a
    mount could only be outward by also being lower. So the arm works around ``MEAN_PITCH``, as a da
    Vinci arm does, and its other joints stay mid-range around ``approach``.
    """
    mount = np.array([flare, np.cos(YAW_AXIS_RISE), np.sin(YAW_AXIS_RISE)])
    square = _unit(mount - approach * (mount @ approach))
    y = -(square * np.cos(MEAN_PITCH) - approach * np.sin(MEAN_PITCH))
    z = _unit(-approach + y * (approach @ y))
    return np.column_stack([np.cross(y, z), y, z])


def _round(values: np.ndarray) -> list:
    return np.round(values, 5).tolist()


def _psm_arm(
    name: str, base: np.ndarray, t_s: np.ndarray, q: dict[str, np.ndarray], jaw: np.ndarray
) -> dict:
    tip, _ = psm_forward(q)
    world = tip @ base[:3, :3].T + base[:3, 3]
    return {
        "name": name,
        "kind": "psm",
        "base": _round(base.ravel()),
        "t_s": _round(t_s),
        "q": {joint: _round(q[joint]) for joint in JOINTS} | {"jaw": _round(jaw)},
        "tip": _round(world),
    }


def _views(tips: np.ndarray, pivots: np.ndarray) -> dict:
    """Camera placements: the whole arms, and the instruments at work."""
    center = tips.mean(axis=0)
    spread = max(float(np.ptp(tips, axis=0).max()), 0.06)
    around_arms = (center + pivots.mean(axis=0)) / 2 + np.array([0.0, 0.25, 0.1])
    return {
        "arms": {
            "eye": _round(around_arms + np.array([0.15, -1.45, 0.45])),
            "target": _round(around_arms),
        },
        "tools": {
            "eye": _round(center + np.array([0.0, -1.1, 0.8]) * max(spread, 0.08)),
            "target": _round(center),
        },
    }


def trial_scene(samples: list[dict], gestures: list[dict]) -> dict:
    """The simulation of one JIGSAWS trial, from its ``kinematics`` samples and gesture segments."""
    if not samples:
        return {"system": None}
    df = pd.DataFrame(samples)
    frames = df["frame_idx"].to_numpy()
    t_s = df["t_s"].to_numpy(dtype=float)

    def positions(arm: str) -> np.ndarray:
        return df[[f"{arm}_{a}" for a in "xyz"]].to_numpy(dtype=float)

    def rotations(arm: str) -> np.ndarray:
        names = [f"{arm}_r{i}{j}" for i in (1, 2, 3) for j in (1, 2, 3)]
        return df[names].to_numpy(dtype=float).reshape(-1, 3, 3)

    left, right = positions("psml"), positions("psmr")
    offset = arm_offset(left, right, frames, gestures)
    left, right = left + offset / 2, right - offset / 2
    left_rot, right_rot = rotations("psml"), rotations("psmr")

    # The tools converge on the work from either side: their mean approach gives down, their
    # difference gives right.
    left_approach = _unit(left_rot[:, :, 2].mean(axis=0))
    right_approach = _unit(right_rot[:, :, 2].mean(axis=0))
    down = _unit(left_approach + right_approach)
    across = left_approach - right_approach
    across = _unit(across - down * (across @ down))
    up = -down
    to_scene = np.vstack([across, np.cross(up, across), up])
    center = np.vstack([left, right]).mean(axis=0)

    arms, tips, pivots = [], [], []
    for name, arm, p, rot, flare in (
        ("PSML", "psml", left, left_rot, -YAW_AXIS_FLARE),
        ("PSMR", "psmr", right, right_rot, YAW_AXIS_FLARE),
    ):
        scene_p = (p - center) @ to_scene.T
        scene_rot = to_scene @ rot
        approach = to_scene @ _unit(rot[:, :, 2].mean(axis=0))
        pivot = scene_p.mean(axis=0) - approach * PIVOT_DISTANCE
        base_rot = _base_rotation(approach, flare)
        q = psm_inverse((scene_p - pivot) @ base_rot, base_rot.T @ scene_rot)
        # Gripper angles in the data have no agreed zero: open and closed are this arm's own extremes.
        g = df[f"{arm}_gripper"].to_numpy(dtype=float)
        low, high = np.percentile(g, [1, 99])
        jaw = np.clip((g - low) / max(high - low, 1e-6), 0, 1) * MAX_JAW
        arms.append(_psm_arm(name, _matrix(base_rot, pivot), t_s, q, jaw))
        tips.append(np.asarray(arms[-1]["tip"]))
        pivots.append(pivot)
    return {
        "system": SYSTEM,
        "source": "jigsaws",
        "duration_s": float(t_s[-1]),
        "arms": arms,
        "views": _views(np.vstack(tips), np.vstack(pivots)),
        "note": (
            "Tool tips follow the recording. JIGSAWS records no joints or pivots, so the rest of "
            "each arm is a fitted estimate, as is which way each jaw faces. The cart is schematic."
        ),
    }


# Where telemetry's arms stand in the scene: pivot x, y, height, and mount splay.
_TELEMETRY_PLACES = {
    "PSM1": (0.09, 0.0, 0.14, YAW_AXIS_FLARE),
    "PSM2": (-0.09, 0.0, 0.14, -YAW_AXIS_FLARE),
    "PSM3": (0.0, 0.12, 0.14, 0.0),
}
_ECM_PLACE = np.array([0.0, -0.1, 0.22])


def session_scene(arm_points: dict[str, list[dict]]) -> dict:
    """The simulation of one telemetry session, from each arm's ``trajectory`` points."""
    if system_for_arms(arm_points) is None:
        return {"system": None}
    frames = {arm: pd.DataFrame(points) for arm, points in arm_points.items() if points}
    if not frames:
        return {"system": None}
    start = min(to_utc(df["ts"]).min() for df in frames.values())

    arms, tips, pivots = [], [], []
    tool_held_straight = False
    for name, df in sorted(frames.items()):
        t_s = ((to_utc(df["ts"]) - start).dt.total_seconds()).to_numpy(dtype=float)
        p = df[["x", "y", "z"]].to_numpy(dtype=float)
        if name == "ECM":
            world = p + _ECM_PLACE
            arms.append(
                {
                    "name": name,
                    "kind": "ecm",
                    "pivot": _round(_ECM_PLACE),
                    "t_s": _round(t_s),
                    "tip": _round(world),
                }
            )
            continue
        if name not in _TELEMETRY_PLACES:  # masters are not part of the patient side
            continue
        x, y, height, flare = _TELEMETRY_PLACES[name]
        pivot = np.array([x, y, height])
        # Telemetry's frames are the arms' own: a tool at mid-insertion hangs straight along -z.
        base_rot = _base_rotation(_unit(-pivot), flare)
        rotations = _quaternion_rotations(df)
        if rotations is not None and not wrist_reachable(p, rotations):
            rotations = None
        tool_held_straight |= rotations is None
        q = psm_inverse(p, rotations)
        jaw = np.clip(df["jaw_angle"].fillna(0).to_numpy(dtype=float), 0, MAX_JAW)
        arms.append(_psm_arm(name, _matrix(base_rot, pivot), t_s, q, jaw))
        tips.append(np.asarray(arms[-1]["tip"]))
        pivots.append(pivot)
    if not tips:
        return {"system": None}
    return {
        "system": SYSTEM,
        "source": "telemetry",
        "duration_s": float(max(arm["t_s"][-1] for arm in arms)),
        "arms": arms,
        "views": _views(np.vstack(tips), np.vstack(pivots)),
        "note": (
            "Tool tips follow the recording, in each arm's own frame; where the arms stand around "
            "the table is chosen for display, and the cart is schematic."
            + (
                " These tool rotations do not fit the wrist, so the tools are held straight."
                if tool_held_straight
                else ""
            )
        ),
    }


def _quaternion_rotations(df: pd.DataFrame) -> np.ndarray | None:
    if not {"qx", "qy", "qz", "qw"} <= set(df.columns):
        return None
    q = df[["qx", "qy", "qz", "qw"]].to_numpy(dtype=float)
    if np.isnan(q).any():
        return None
    x, y, z, w = q.T
    return np.stack(
        [
            np.stack([1 - 2 * (y * y + z * z), 2 * (x * y - z * w), 2 * (x * z + y * w)], axis=-1),
            np.stack([2 * (x * y + z * w), 1 - 2 * (x * x + z * z), 2 * (y * z - x * w)], axis=-1),
            np.stack([2 * (x * z - y * w), 2 * (y * z + x * w), 1 - 2 * (x * x + y * y)], axis=-1),
        ],
        axis=-2,
    )
