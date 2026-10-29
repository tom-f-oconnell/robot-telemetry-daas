"""Pure-Python helpers shared by the adapter nodes.

Deliberately free of any ROS import so the math is unit-testable with plain pytest
(see test/test_kinematics.py). The nodes in tool_pose_adapter.py and fleet_monitor.py
only convert ROS messages to/from the plain values used here.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

NAN = float("nan")

# Arms that have a jaw (instrument gripper). ECM (camera) and MTMs (masters) do not;
# their ToolPose.jaw_angle is NaN per the message contract.
ARMS_WITH_JAW = frozenset({"PSM1", "PSM2", "PSM3"})

# Default per-joint limits (radians, or metres for the insertion joint) used by the
# fleet monitor to raise JOINT_LIMIT_WARN. Values are conservative dVRK PSM limits.
DEFAULT_JOINT_LIMITS: dict[str, tuple[float, float]] = {
    "outer_yaw": (-1.5, 1.5),
    "outer_pitch": (-0.9, 0.9),
    "outer_insertion": (0.0, 0.24),
    "outer_roll": (-4.5, 4.5),
    "outer_wrist_pitch": (-1.4, 1.4),
    "outer_wrist_yaw": (-1.4, 1.4),
}

# Coarse operating states published in Heartbeat.state (spec §2, rtdaas_msgs/Heartbeat).
STATE_IDLE = "idle"
STATE_HOMING = "homing"
STATE_READY = "ready"
STATE_TELEOP = "teleop"
STATE_FAULT = "fault"

SEVERITY_INFO = 1
SEVERITY_WARN = 2
SEVERITY_ERROR = 3


@dataclass
class SpeedEstimator:
    """Finite-difference Cartesian speed with a one-pole low-pass filter.

    ``update(t_s, x, y, z)`` returns the filtered speed in m/s. The first sample
    returns 0.0; samples with non-increasing timestamps are ignored (returns the
    last speed) so a replayed/duplicated pose cannot produce an infinite speed.
    """

    alpha: float = 0.3  # filter weight for the newest raw estimate (1.0 = no filtering)
    max_dt_s: float = 1.0  # gaps longer than this restart the estimate
    _last_t: float | None = None
    _last_p: tuple[float, float, float] | None = None
    _speed: float = 0.0

    def update(self, t_s: float, x: float, y: float, z: float) -> float:
        p = (x, y, z)
        if self._last_t is None or self._last_p is None:
            self._last_t, self._last_p, self._speed = t_s, p, 0.0
            return self._speed
        dt = t_s - self._last_t
        if dt <= 0.0:
            return self._speed
        if dt > self.max_dt_s:
            self._last_t, self._last_p, self._speed = t_s, p, 0.0
            return self._speed
        dist = math.dist(p, self._last_p)
        raw = dist / dt
        self._speed = self.alpha * raw + (1.0 - self.alpha) * self._speed
        self._last_t, self._last_p = t_s, p
        return self._speed

    @property
    def speed(self) -> float:
        return self._speed


def jaw_angle_from_joint_state(names: list[str], positions: list[float]) -> float:
    """Jaw opening from a dVRK ``<arm>/jaw/measured_js`` JointState.

    The dVRK publishes a single-joint JointState for the jaw; position[0] is the
    opening angle in radians. If a joint named ``jaw`` is present it is preferred,
    otherwise the first position is used. Returns NaN when nothing usable is present.
    """
    if not positions:
        return NAN
    if names:
        for i, n in enumerate(names):
            if n.lower().endswith("jaw") and i < len(positions):
                return float(positions[i])
    return float(positions[0])


def arm_has_jaw(arm: str) -> bool:
    return arm.upper() in ARMS_WITH_JAW


def joint_limit_violations(
    names: list[str],
    positions: list[float],
    limits: dict[str, tuple[float, float]] | None = None,
    margin_fraction: float = 0.05,
) -> list[tuple[str, float, tuple[float, float]]]:
    """Joints whose position is outside ``limits`` shrunk by ``margin_fraction``.

    Returns a list of (joint_name, position, (lo, hi)) for every violating joint. Joints
    without a configured limit are ignored; a JointState with fewer positions than names
    is handled defensively.
    """
    limits = DEFAULT_JOINT_LIMITS if limits is None else limits
    out: list[tuple[str, float, tuple[float, float]]] = []
    for name, pos in zip(names, positions):
        lim = limits.get(name)
        if lim is None:
            continue
        lo, hi = lim
        span = hi - lo
        lo_m, hi_m = lo + margin_fraction * span, hi - margin_fraction * span
        if pos < lo_m or pos > hi_m:
            out.append((name, float(pos), (lo, hi)))
    return out


def operating_state_to_heartbeat_state(
    state: str | None, is_homed: bool | None, is_busy: bool | None, teleop_active: bool
) -> str:
    """Map a CRTK ``operating_state`` (plus console teleop flag) to Heartbeat.state.

    CRTK states: DISABLED, ENABLED, PAUSED, FAULT. Within ENABLED the dVRK reports
    ``is_homed``; while homing it is ENABLED + not homed (+ busy).
    """
    if state is None:
        return STATE_IDLE
    s = state.upper()
    if s == "FAULT":
        return STATE_FAULT
    if s in ("DISABLED", "PAUSED"):
        return STATE_IDLE
    if s == "ENABLED":
        if is_homed is False or (is_homed is None and is_busy):
            return STATE_HOMING
        if teleop_active:
            return STATE_TELEOP
        return STATE_READY
    return STATE_IDLE


@dataclass
class StateTracker:
    """Tracks per-arm heartbeat states and tells the monitor which events to emit.

    ``update(arm, new_state)`` returns a list of (severity, code, message) tuples for
    transitions that happened: INFO ``STATE_<NEW>`` for every change, ERROR ``ESTOP``
    when an arm enters ``fault``, INFO ``FAULT_CLEARED`` when it leaves it.
    """

    states: dict[str, str] = field(default_factory=dict)

    def update(self, arm: str, new_state: str) -> list[tuple[int, str, str]]:
        old = self.states.get(arm)
        self.states[arm] = new_state
        if old == new_state:
            return []
        events: list[tuple[int, str, str]] = []
        if old is not None:
            events.append((SEVERITY_INFO, f"STATE_{new_state.upper()}", f"{arm}: {old} -> {new_state}"))
        else:
            events.append((SEVERITY_INFO, f"STATE_{new_state.upper()}", f"{arm}: initial state {new_state}"))
        if new_state == STATE_FAULT:
            events.append((SEVERITY_ERROR, "ESTOP", f"{arm} entered FAULT"))
        elif old == STATE_FAULT:
            events.append((SEVERITY_INFO, "FAULT_CLEARED", f"{arm} left FAULT -> {new_state}"))
        return events

    def robot_state(self) -> str:
        """Aggregate state for the Heartbeat: fault > homing > teleop > ready > idle."""
        values = set(self.states.values())
        for s in (STATE_FAULT, STATE_HOMING, STATE_TELEOP, STATE_READY):
            if s in values:
                return s
        return STATE_IDLE

    def active_arms(self) -> list[str]:
        return sorted(a for a, s in self.states.items() if s in (STATE_READY, STATE_TELEOP))
