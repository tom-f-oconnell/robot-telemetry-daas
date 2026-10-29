"""ROS-less fleet simulator producing dVRK-like telemetry (spec §4.3).

:class:`RobotSim` emits, per tick, message dicts shaped exactly like the ROS 2
messages in ``contracts/msgs`` for one robot:

* ``sensor_msgs/JointState`` per arm (6 joints)
* ``rtdaas_msgs/ToolPose`` per arm (smooth pseudo-surgical motion with idle periods)
* ``rtdaas_msgs/Heartbeat`` at 1 Hz with ``idle -> homing -> ready -> teleop``
* ``rtdaas_msgs/SystemEvent``: ``SESSION_START`` on the first tick, ``SESSION_END``
  from :meth:`RobotSim.end`, plus seeded ``INSTRUMENT_SWAP``, ``CLUTCH_ENGAGED`` /
  ``CLUTCH_RELEASED``, ``JOINT_LIMIT_WARN`` and (rare) ``ESTOP`` events.

Motion model: each PSM integrates a velocity field made of low-frequency sinusoids
plus a weak spring toward the centre of a 10 cm workspace cube, gated by an
activity signal that is idle roughly 20 % of the time. Integrating velocity keeps
position continuous across idle/active transitions. The ECM moves slowly; MTM arms
mirror their PSM (MTMR <-> PSM1, MTML <-> PSM2) scaled by 3. Everything is
deterministic for a given seed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np

KAFKA_JOINT_STATES = "rtdaas.telemetry.joint_states"
KAFKA_TOOL_POSE = "rtdaas.telemetry.tool_pose"
KAFKA_EVENTS = "rtdaas.telemetry.events"
KAFKA_HEARTBEAT = "rtdaas.telemetry.heartbeat"

TOPIC_TYPES: dict[str, str] = {
    KAFKA_JOINT_STATES: "sensor_msgs/JointState",
    KAFKA_TOOL_POSE: "rtdaas_msgs/ToolPose",
    KAFKA_EVENTS: "rtdaas_msgs/SystemEvent",
    KAFKA_HEARTBEAT: "rtdaas_msgs/Heartbeat",
}

JOINT_NAMES = (
    "outer_yaw",
    "outer_pitch",
    "outer_insertion",
    "outer_roll",
    "outer_wrist_pitch",
    "outer_wrist_yaw",
)

SEVERITY_INFO, SEVERITY_WARN, SEVERITY_ERROR = 1, 2, 3
WORKSPACE_HALF_M = 0.05  # 10 cm cube
JAW_MAX_RAD = 0.6
NS = 1_000_000_000


def ros_topic(robot_id: str, arm: str, suffix: str) -> str:
    return f"/{robot_id}/{arm}/{suffix}"


def robot_topic(robot_id: str, suffix: str) -> str:
    return f"/{robot_id}/rtdaas/{suffix}"


def stamp(t_ns: int) -> dict[str, int]:
    return {"sec": int(t_ns // NS), "nanosec": int(t_ns % NS)}


def header(t_ns: int, frame_id: str = "") -> dict[str, Any]:
    return {"stamp": stamp(t_ns), "frame_id": frame_id}


@dataclass
class SimConfig:
    robot_id: str
    arms: tuple[str, ...] = ("PSM1", "PSM2", "ECM")
    rate_hz: float = 50.0
    seed: int = 0
    software_version: str = "sim-1.0"
    idle_fraction: float = 0.2


@dataclass
class _ArmState:
    name: str
    center: np.ndarray
    pos: np.ndarray
    prev_pos: np.ndarray
    freqs: np.ndarray  # (3, k)
    phases: np.ndarray
    amps: np.ndarray
    jaw_freq: float
    jaw_phase: float
    rot_freqs: np.ndarray
    speed_scale: float
    prev_joints: np.ndarray = field(default_factory=lambda: np.zeros(6))
    jaw: float = 0.0
    speed: float = 0.0


class RobotSim:
    def __init__(self, cfg: SimConfig) -> None:
        self.cfg = cfg
        self.rng = np.random.default_rng(cfg.seed)
        self.dt = 1.0 / cfg.rate_hz
        self._t0_ns: int | None = None
        self._last_tick_ns: int | None = None
        self._last_heartbeat_sec: int | None = None
        self._ended = False
        self._tick_count = 0
        self.arms: dict[str, _ArmState] = {arm: self._init_arm(arm) for arm in cfg.arms}
        # activity gate: list of (end_time_s, active) segments, generated lazily
        self._gate_until_s = 0.0
        self._gate_active = True
        # event schedule (seconds since start)
        self._next_swap_s = self._exp(45.0) + 5.0
        self._next_clutch_s = self._exp(20.0) + 2.0
        self._clutch_release_s: float | None = None
        self._next_limit_warn_s = self._exp(30.0) + 3.0
        self._next_estop_s = self._exp(900.0) + 30.0
        self._estop_active_until_s: float | None = None
        self._instrument: dict[str, str] = {a: "LARGE_NEEDLE_DRIVER" for a in cfg.arms if a.startswith("PSM")}

    # -- setup -------------------------------------------------------------------------------
    def _exp(self, mean_s: float) -> float:
        return float(self.rng.exponential(mean_s))

    def _init_arm(self, arm: str) -> _ArmState:
        k = 4
        if arm.startswith("PSM"):
            scale, speed_scale = 1.0, 1.0
        elif arm == "ECM":
            scale, speed_scale = 0.4, 0.15
        else:  # MTM
            scale, speed_scale = 3.0, 1.0
        center = np.array([0.0, 0.0, -0.12]) + self.rng.normal(0, 0.01, 3) * scale
        freqs = self.rng.uniform(0.05, 0.4, size=(3, k))
        phases = self.rng.uniform(0, 2 * math.pi, size=(3, k))
        amps = self.rng.uniform(0.004, 0.02, size=(3, k)) * scale  # m/s velocity amplitudes
        return _ArmState(
            name=arm,
            center=center,
            pos=center.copy(),
            prev_pos=center.copy(),
            freqs=freqs,
            phases=phases,
            amps=amps,
            jaw_freq=float(self.rng.uniform(0.3, 0.8)),
            jaw_phase=float(self.rng.uniform(0, 2 * math.pi)),
            rot_freqs=self.rng.uniform(0.02, 0.1, size=3),
            speed_scale=speed_scale,
        )

    # -- helpers -----------------------------------------------------------------------------
    def _elapsed_s(self, t_ns: int) -> float:
        assert self._t0_ns is not None
        return (t_ns - self._t0_ns) / NS

    def _gate(self, t_s: float) -> bool:
        """Alternate active and idle segments; mean durations give ~idle_fraction idle time."""
        f = min(max(self.cfg.idle_fraction, 0.01), 0.9)
        active_mean = 9.0
        idle_mean = active_mean * f / (1 - f)
        while t_s >= self._gate_until_s:
            if self._gate_active:
                self._gate_active = False
                self._gate_until_s += float(self.rng.uniform(0.4 * idle_mean, 1.6 * idle_mean))
            else:
                self._gate_active = True
                self._gate_until_s += float(self.rng.uniform(0.4 * active_mean, 1.6 * active_mean))
        return self._gate_active

    def _quaternion(self, arm: _ArmState, t_s: float) -> dict[str, float]:
        roll, pitch, yaw = (0.3 * math.sin(2 * math.pi * f * t_s + i) for i, f in enumerate(arm.rot_freqs))
        cr, sr = math.cos(roll / 2), math.sin(roll / 2)
        cp, sp = math.cos(pitch / 2), math.sin(pitch / 2)
        cy, sy = math.cos(yaw / 2), math.sin(yaw / 2)
        q = np.array(
            [
                sr * cp * cy - cr * sp * sy,
                cr * sp * cy + sr * cp * sy,
                cr * cp * sy - sr * sp * cy,
                cr * cp * cy + sr * sp * sy,
            ]
        )
        q /= np.linalg.norm(q)
        return {"x": float(q[0]), "y": float(q[1]), "z": float(q[2]), "w": float(q[3])}

    def _step_arm(self, arm: _ArmState, t_s: float, active: bool, estop: bool) -> None:
        arm.prev_pos = arm.pos.copy()
        if active and not estop:
            vel = (arm.amps * np.sin(2 * math.pi * arm.freqs * t_s + arm.phases)).sum(axis=1) * arm.speed_scale
            vel += -0.6 * (arm.pos - arm.center)  # spring toward centre keeps motion inside the cube
            vel += self.rng.normal(0, 0.0005, 3)
            arm.pos = arm.pos + vel * self.dt
            arm.pos = np.clip(arm.pos, arm.center - WORKSPACE_HALF_M, arm.center + WORKSPACE_HALF_M)
            if arm.name.startswith("PSM"):
                arm.jaw = JAW_MAX_RAD * 0.5 * (1 + math.sin(2 * math.pi * arm.jaw_freq * t_s + arm.jaw_phase))
        else:
            arm.pos = arm.pos + self.rng.normal(0, 0.00002, 3)
        arm.speed = float(np.linalg.norm(arm.pos - arm.prev_pos) / self.dt)

    def _joint_positions(self, arm: _ArmState) -> np.ndarray:
        """Plausible joint values derived from the tip position (not real inverse kinematics)."""
        x, y, z = (float(v) for v in arm.pos)
        depth = max(-z, 1e-3)  # insertion depth below the remote centre of motion
        return np.array(
            [
                math.atan2(x, depth),
                math.atan2(-y, depth),
                math.sqrt(x * x + y * y + depth * depth),
                0.4 * math.sin(30.0 * x),
                0.5 * math.sin(20.0 * y),
                0.5 * math.cos(20.0 * x),
            ]
        )

    def _mirror(self, arm: str) -> _ArmState | None:
        if arm == "MTMR" and "PSM1" in self.arms:
            return self.arms["PSM1"]
        if arm == "MTML" and "PSM2" in self.arms:
            return self.arms["PSM2"]
        return None

    def _event(self, t_ns: int, arm: str, severity: int, code: str, message: str) -> tuple[str, str, dict[str, Any]]:
        return (
            KAFKA_EVENTS,
            robot_topic(self.cfg.robot_id, "events"),
            {"header": header(t_ns), "arm": arm, "severity": severity, "code": code, "message": message},
        )

    def _state(self, t_s: float) -> str:
        if self._estop_active_until_s is not None and t_s < self._estop_active_until_s:
            return "fault"
        if t_s < 2:
            return "idle"
        if t_s < 5:
            return "homing"
        if t_s < 8:
            return "ready"
        return "teleop"

    # -- public API --------------------------------------------------------------------------
    def begin(self, t_ns: int) -> list[tuple[str, str, dict[str, Any]]]:
        """Mark the session start; returns the SESSION_START event. Called implicitly by tick()."""
        if self._t0_ns is not None:
            return []
        self._t0_ns = t_ns
        return [self._event(t_ns, "", SEVERITY_INFO, "SESSION_START", f"session started on {self.cfg.robot_id}")]

    def end(self, t_ns: int) -> list[tuple[str, str, dict[str, Any]]]:
        if self._ended or self._t0_ns is None:
            return []
        self._ended = True
        return [self._event(t_ns, "", SEVERITY_INFO, "SESSION_END", f"session ended after {self._elapsed_s(t_ns):.1f} s")]

    def tick(self, t_ns: int) -> list[tuple[str, str, dict[str, Any]]]:
        out: list[tuple[str, str, dict[str, Any]]] = []
        out.extend(self.begin(t_ns))
        t_s = self._elapsed_s(t_ns)
        self._tick_count += 1
        rid = self.cfg.robot_id

        # -- events ------------------------------------------------------------------------
        estop = self._estop_active_until_s is not None and t_s < self._estop_active_until_s
        if t_s >= self._next_estop_s:
            self._estop_active_until_s = t_s + float(self.rng.uniform(3.0, 8.0))
            self._next_estop_s = t_s + self._exp(900.0) + 60.0
            out.append(self._event(t_ns, "", SEVERITY_ERROR, "ESTOP", "emergency stop engaged"))
            estop = True
        if t_s >= self._next_swap_s and self._instrument:
            arm = str(self.rng.choice(sorted(self._instrument)))
            new = str(self.rng.choice(["LARGE_NEEDLE_DRIVER", "MARYLAND_BIPOLAR", "CADIERE_FORCEPS", "MONOPOLAR_SCISSORS"]))
            out.append(self._event(t_ns, arm, SEVERITY_INFO, "INSTRUMENT_SWAP", f"{self._instrument[arm]} -> {new}"))
            self._instrument[arm] = new
            self._next_swap_s = t_s + self._exp(45.0) + 5.0
        if self._clutch_release_s is None and t_s >= self._next_clutch_s:
            out.append(self._event(t_ns, "", SEVERITY_INFO, "CLUTCH_ENGAGED", "master clutch engaged"))
            self._clutch_release_s = t_s + float(self.rng.uniform(1.0, 3.0))
        elif self._clutch_release_s is not None and t_s >= self._clutch_release_s:
            out.append(self._event(t_ns, "", SEVERITY_INFO, "CLUTCH_RELEASED", "master clutch released"))
            self._clutch_release_s = None
            self._next_clutch_s = t_s + self._exp(20.0) + 2.0
        if t_s >= self._next_limit_warn_s:
            arm = str(self.rng.choice(list(self.cfg.arms)))
            joint = str(self.rng.choice(JOINT_NAMES))
            out.append(self._event(t_ns, arm, SEVERITY_WARN, "JOINT_LIMIT_WARN", f"{joint} within 5% of its limit"))
            self._next_limit_warn_s = t_s + self._exp(30.0) + 3.0

        # -- motion ------------------------------------------------------------------------
        active = self._gate(t_s)
        # PSMs / ECM first, MTMs mirror afterwards
        for name in self.cfg.arms:
            if name.startswith("MTM"):
                continue
            self._step_arm(self.arms[name], t_s, active, estop)
        for name in self.cfg.arms:
            if not name.startswith("MTM"):
                continue
            arm = self.arms[name]
            src = self._mirror(name)
            arm.prev_pos = arm.pos.copy()
            if src is not None:
                arm.pos = arm.center + 3.0 * (src.pos - src.center)
                arm.jaw = src.jaw
            else:
                self._step_arm(arm, t_s, active, estop)
                continue
            arm.speed = float(np.linalg.norm(arm.pos - arm.prev_pos) / self.dt)

        for name in self.cfg.arms:
            arm = self.arms[name]
            frame = f"{rid}/{name}"
            joints = self._joint_positions(arm)
            vel = (joints - arm.prev_joints) / self.dt if self._tick_count > 1 else np.zeros(6)
            arm.prev_joints = joints
            out.append(
                (
                    KAFKA_JOINT_STATES,
                    ros_topic(rid, name, "measured_js"),
                    {
                        "header": header(t_ns, frame),
                        "name": list(JOINT_NAMES),
                        "position": [float(v) for v in joints],
                        "velocity": [float(v) for v in vel],
                        "effort": [float(v) for v in self.rng.normal(0, 0.05, 6)],
                    },
                )
            )
            out.append(
                (
                    KAFKA_TOOL_POSE,
                    ros_topic(rid, name, "rtdaas/tool_pose"),
                    {
                        "header": header(t_ns, frame),
                        "arm": name,
                        "position": {"x": float(arm.pos[0]), "y": float(arm.pos[1]), "z": float(arm.pos[2])},
                        "orientation": self._quaternion(arm, t_s),
                        "jaw_angle": float(arm.jaw) if name.startswith(("PSM", "MTM")) else float("nan"),
                        "speed": arm.speed,
                    },
                )
            )

        # -- heartbeat (1 Hz) --------------------------------------------------------------
        sec = int(t_s)
        if self._last_heartbeat_sec is None or sec != self._last_heartbeat_sec:
            self._last_heartbeat_sec = sec
            out.append(
                (
                    KAFKA_HEARTBEAT,
                    robot_topic(rid, "heartbeat"),
                    {
                        "header": header(t_ns),
                        "robot_id": rid,
                        "uptime_s": float(t_s + 3600.0 * (1 + self.cfg.seed % 7)),
                        "state": self._state(t_s),
                        "active_arms": list(self.cfg.arms),
                        "software_version": self.cfg.software_version,
                    },
                )
            )
        self._last_tick_ns = t_ns
        return out
