"""fleet_monitor: liveness and discrete events for one robot.

Publishes

- ``rtdaas_msgs/Heartbeat`` on ``/<robot_id>/rtdaas/heartbeat`` at 1 Hz, with the aggregate
  operating state (fault > homing > teleop > ready > idle; offline when every arm is silent)
  and the list of active arms;
- ``rtdaas_msgs/SystemEvent`` on ``/<robot_id>/rtdaas/events`` for
  ``SESSION_START`` (first heartbeat), ``SESSION_END`` (shutdown), every state transition
  (INFO ``STATE_<NEW>``), ``ESTOP`` (ERROR, an arm entered FAULT), ``FAULT_CLEARED`` and
  ``JOINT_LIMIT_WARN`` (WARN, with a per-joint cool-down).

Liveness: an arm that has reported and then sends nothing for ``input_timeout_s`` goes to
``offline`` (WARN ``STATE_OFFLINE``) and leaves ``active_arms``; the robot state is ``offline``
once every reporting arm is. When input resumes the arm gets back the state it had. Without
this the heartbeat would keep saying ``ready`` after the dVRK died, since states only change
when a message arrives.

Inputs per arm (under ``source_namespace``):

- ``<arm>/operating_state`` — ``crtk_msgs/OperatingState`` when the ``crtk_msgs`` package is
  importable; otherwise a presence-only subscription (any message = arm "ready") using the
  CRTK message type name resolved at runtime via ``rosidl_runtime_py``.
- ``<arm>/measured_js`` — ``sensor_msgs/JointState`` for joint-limit checks.
- ``/<source_namespace>/console/teleop/enabled`` — ``std_msgs/Bool`` (dVRK console; optional).

Parameters: ``robot_id`` (required), ``arms`` (default PSM1, PSM2, ECM), ``source_namespace``
(default ""), ``software_version`` (default "dvrk-sim"), ``joint_limit_margin`` (default 0.05),
``joint_limits`` (flattened ``<joint>.min`` / ``<joint>.max`` overrides of the PSM defaults),
``joint_limit_cooldown_s`` (default 5.0), ``heartbeat_hz`` (default 1.0), ``input_timeout_s``
(default 0.5; checked on every heartbeat).
"""

from __future__ import annotations

import time

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
from sensor_msgs.msg import JointState
from std_msgs.msg import Bool

from rtdaas_msgs.msg import Heartbeat, SystemEvent

from rtdaas_adapters.kinematics import (
    DEFAULT_JOINT_LIMITS,
    INPUT_TIMEOUT_S,
    SEVERITY_INFO,
    SEVERITY_WARN,
    STATE_READY,
    StateTracker,
    is_stale,
    joint_limit_violations,
    operating_state_to_heartbeat_state,
)

try:  # crtk_msgs ships with the dVRK ROS 2 stack (jhu-dvrk/crtk_msgs); optional here.
    from crtk_msgs.msg import OperatingState as _OperatingState  # type: ignore
except ImportError:  # pragma: no cover - depends on the environment
    _OperatingState = None


def _topic(namespace: str, *parts: str) -> str:
    ns = namespace.strip("/")
    segments = ([ns] if ns else []) + [p.strip("/") for p in parts]
    return "/" + "/".join(segments)


class FleetMonitor(Node):
    def __init__(self) -> None:
        super().__init__("fleet_monitor")
        self.declare_parameter("robot_id", "")
        self.declare_parameter("arms", ["PSM1", "PSM2", "ECM"])
        self.declare_parameter("source_namespace", "")
        self.declare_parameter("software_version", "dvrk-sim")
        self.declare_parameter("joint_limit_margin", 0.05)
        self.declare_parameter("joint_limit_cooldown_s", 5.0)
        self.declare_parameter("heartbeat_hz", 1.0)
        self.declare_parameter("input_timeout_s", INPUT_TIMEOUT_S)
        for joint, (lo, hi) in DEFAULT_JOINT_LIMITS.items():
            self.declare_parameter(f"joint_limits.{joint}.min", lo)
            self.declare_parameter(f"joint_limits.{joint}.max", hi)

        self.robot_id: str = self.get_parameter("robot_id").value
        if not self.robot_id:
            raise ValueError("parameter 'robot_id' is required")
        self.arms: list[str] = list(self.get_parameter("arms").value)
        source_ns: str = self.get_parameter("source_namespace").value
        self.software_version: str = self.get_parameter("software_version").value
        self.margin: float = float(self.get_parameter("joint_limit_margin").value)
        self.cooldown_s: float = float(self.get_parameter("joint_limit_cooldown_s").value)
        hz: float = float(self.get_parameter("heartbeat_hz").value)
        self.input_timeout_s: float = float(self.get_parameter("input_timeout_s").value)
        self.limits: dict[str, tuple[float, float]] = {
            joint: (
                float(self.get_parameter(f"joint_limits.{joint}.min").value),
                float(self.get_parameter(f"joint_limits.{joint}.max").value),
            )
            for joint in DEFAULT_JOINT_LIMITS
        }

        reliable_latched = QoSProfile(
            depth=100, reliability=ReliabilityPolicy.RELIABLE,
            history=HistoryPolicy.KEEP_LAST, durability=DurabilityPolicy.VOLATILE)
        sensor_qos = QoSProfile(
            depth=10, reliability=ReliabilityPolicy.BEST_EFFORT, history=HistoryPolicy.KEEP_LAST)

        self._hb_pub = self.create_publisher(
            Heartbeat, _topic(self.robot_id, "rtdaas", "heartbeat"), reliable_latched)
        self._ev_pub = self.create_publisher(
            SystemEvent, _topic(self.robot_id, "rtdaas", "events"), reliable_latched)

        self._tracker = StateTracker()
        self._teleop_active = False
        self._last_limit_warn: dict[tuple[str, str], float] = {}
        self._last_rx: dict[str, float] = {}  # arm -> time.monotonic() of its last message
        self._start_mono = time.monotonic()
        self._session_started = False
        self._seq = 0

        for arm in self.arms:
            if _OperatingState is not None:
                self.create_subscription(
                    _OperatingState, _topic(source_ns, arm, "operating_state"),
                    lambda msg, a=arm: self._on_operating_state(a, msg), reliable_latched)
            else:
                # Presence-only fallback: treat any measured_js as "ready".
                self.get_logger().warning(
                    "crtk_msgs not available; inferring arm state from measured_js presence")
            self.create_subscription(
                JointState, _topic(source_ns, arm, "measured_js"),
                lambda msg, a=arm: self._on_joint_state(a, msg), sensor_qos)
        self.create_subscription(
            Bool, _topic(source_ns, "console", "teleop", "enabled"), self._on_teleop, reliable_latched)

        self._hb_timer = self.create_timer(1.0 / max(hz, 0.1), self._publish_heartbeat)
        self.get_logger().info(f"fleet_monitor for {self.robot_id} watching arms {self.arms}")

    # --- helpers --------------------------------------------------------------------------
    def _emit(self, arm: str, severity: int, code: str, message: str) -> None:
        ev = SystemEvent()
        ev.header.stamp = self.get_clock().now().to_msg()
        ev.header.frame_id = self.robot_id
        ev.arm = arm
        ev.severity = int(severity)
        ev.code = code
        ev.message = message
        self._ev_pub.publish(ev)
        # One call site per severity: rclpy raises "Logger severity cannot be changed between
        # calls" when a single logging line is used with different severities.
        text = f"event {code} [{arm or 'robot'}]: {message}"
        if severity >= 3:
            self.get_logger().error(text)
        elif severity == 2:
            self.get_logger().warning(text)
        else:
            self.get_logger().info(text)

    def _apply_state(self, arm: str, new_state: str) -> None:
        self._emit_all(arm, self._tracker.update(arm, new_state))

    def _emit_all(self, arm: str, events: list[tuple[int, str, str]]) -> None:
        for severity, code, message in events:
            self._emit(arm, severity, code, message)

    # --- callbacks ------------------------------------------------------------------------
    def _on_operating_state(self, arm: str, msg) -> None:
        self._last_rx[arm] = time.monotonic()
        state = getattr(msg, "state", None)
        is_homed = getattr(msg, "is_homed", None)
        is_busy = getattr(msg, "is_busy", None)
        self._apply_state(
            arm, operating_state_to_heartbeat_state(state, is_homed, is_busy, self._teleop_active))

    def _on_teleop(self, msg: Bool) -> None:
        self._teleop_active = bool(msg.data)
        # Re-evaluate arms that are ready/teleop so the aggregate state flips.
        for arm, st in list(self._tracker.states.items()):
            if st in ("ready", "teleop"):
                self._apply_state(arm, "teleop" if self._teleop_active else "ready")

    def _on_joint_state(self, arm: str, msg: JointState) -> None:
        now = time.monotonic()
        self._last_rx[arm] = now
        # presence-only monitoring: the first measured_js makes the arm "ready"
        self._emit_all(
            arm, self._tracker.mark_alive(arm, STATE_READY if _OperatingState is None else None))
        for joint, pos, (lo, hi) in joint_limit_violations(
                list(msg.name), list(msg.position), self.limits, self.margin):
            key = (arm, joint)
            if now - self._last_limit_warn.get(key, -1e9) < self.cooldown_s:
                continue
            self._last_limit_warn[key] = now
            self._emit(arm, SEVERITY_WARN, "JOINT_LIMIT_WARN",
                       f"{joint}={pos:.3f} near/outside [{lo:.3f}, {hi:.3f}]")

    def _publish_heartbeat(self) -> None:
        now = time.monotonic()
        for arm, last_rx in self._last_rx.items():
            if is_stale(last_rx, now, self.input_timeout_s):
                self._emit_all(arm, self._tracker.mark_offline(arm))
        if not self._session_started:
            self._session_started = True
            self._emit("", SEVERITY_INFO, "SESSION_START",
                       f"fleet_monitor up for {self.robot_id} ({self.software_version})")
        hb = Heartbeat()
        hb.header.stamp = self.get_clock().now().to_msg()
        hb.header.frame_id = self.robot_id
        hb.robot_id = self.robot_id
        hb.uptime_s = time.monotonic() - self._start_mono
        hb.state = self._tracker.robot_state()
        hb.active_arms = self._tracker.active_arms()
        hb.software_version = self.software_version
        self._hb_pub.publish(hb)
        self._seq += 1

    def end_session(self) -> None:
        if self._session_started:
            self._emit("", SEVERITY_INFO, "SESSION_END", f"fleet_monitor shutting down for {self.robot_id}")
            # Give the reliable publisher a moment to deliver before the context is torn down.
            time.sleep(0.2)


def main(args=None) -> None:
    rclpy.init(args=args)
    node = FleetMonitor()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            node.end_session()
        finally:
            node.destroy_node()
            if rclpy.ok():
                rclpy.shutdown()


if __name__ == "__main__":
    main()
