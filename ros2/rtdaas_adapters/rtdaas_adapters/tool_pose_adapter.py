"""tool_pose_adapter: compose rtdaas_msgs/ToolPose from dVRK Cartesian pose + jaw topics.

For every arm in ``arms`` the node subscribes to

- ``/<source_namespace>/<arm>/measured_cp``      (geometry_msgs/PoseStamped)
- ``/<source_namespace>/<arm>/jaw/measured_js``  (sensor_msgs/JointState; PSMs only)

keeps the latest values, estimates the tip speed by finite differences, and publishes
``rtdaas_msgs/ToolPose`` on ``/<robot_id>/<arm>/rtdaas/tool_pose`` at ``rate_hz``.

Parameters
----------
robot_id : str            fleet-wide robot identifier, e.g. "dv_001" (required)
arms : list[str]          default ["PSM1", "PSM2", "ECM"]
rate_hz : float           publish rate, default 50.0
source_namespace : str    where the dVRK topics live. In the dVRK kinematic simulation
                          the arms publish under the root namespace (``/PSM1/measured_cp``),
                          so the default is "". The fleet launch file sets it to the
                          per-robot namespace it pushes the dVRK nodes into.
speed_filter_alpha : float  low-pass weight for the speed estimate, default 0.3
"""

from __future__ import annotations

import math

import rclpy
from geometry_msgs.msg import PoseStamped
from rclpy.node import Node
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy
from sensor_msgs.msg import JointState

from rtdaas_msgs.msg import ToolPose

from rtdaas_adapters.kinematics import NAN, SpeedEstimator, arm_has_jaw, jaw_angle_from_joint_state


def _topic(namespace: str, *parts: str) -> str:
    ns = namespace.strip("/")
    segments = ([ns] if ns else []) + [p.strip("/") for p in parts]
    return "/" + "/".join(segments)


class _ArmState:
    def __init__(self, alpha: float) -> None:
        self.pose: PoseStamped | None = None
        self.jaw_angle: float = NAN
        self.estimator = SpeedEstimator(alpha=alpha)
        self.speed: float = 0.0


class ToolPoseAdapter(Node):
    def __init__(self) -> None:
        super().__init__("tool_pose_adapter")
        self.declare_parameter("robot_id", "")
        self.declare_parameter("arms", ["PSM1", "PSM2", "ECM"])
        self.declare_parameter("rate_hz", 50.0)
        self.declare_parameter("source_namespace", "")
        self.declare_parameter("speed_filter_alpha", 0.3)

        self.robot_id: str = self.get_parameter("robot_id").value
        if not self.robot_id:
            raise ValueError("parameter 'robot_id' is required")
        self.arms: list[str] = list(self.get_parameter("arms").value)
        rate_hz: float = float(self.get_parameter("rate_hz").value)
        source_ns: str = self.get_parameter("source_namespace").value
        alpha: float = float(self.get_parameter("speed_filter_alpha").value)

        sensor_qos = QoSProfile(
            depth=10, reliability=ReliabilityPolicy.BEST_EFFORT, history=HistoryPolicy.KEEP_LAST
        )
        self._state: dict[str, _ArmState] = {}
        self._pubs: dict[str, rclpy.publisher.Publisher] = {}
        for arm in self.arms:
            st = _ArmState(alpha)
            self._state[arm] = st
            self.create_subscription(
                PoseStamped, _topic(source_ns, arm, "measured_cp"),
                lambda msg, a=arm: self._on_pose(a, msg), sensor_qos)
            if arm_has_jaw(arm):
                self.create_subscription(
                    JointState, _topic(source_ns, arm, "jaw", "measured_js"),
                    lambda msg, a=arm: self._on_jaw(a, msg), sensor_qos)
            self._pubs[arm] = self.create_publisher(
                ToolPose, _topic(self.robot_id, arm, "rtdaas", "tool_pose"), sensor_qos)
            self.get_logger().info(
                f"{arm}: {_topic(source_ns, arm, 'measured_cp')} -> "
                f"{_topic(self.robot_id, arm, 'rtdaas', 'tool_pose')}")

        self._timer = self.create_timer(1.0 / max(rate_hz, 1.0), self._publish_all)

    # --- callbacks -----------------------------------------------------------------------
    def _on_pose(self, arm: str, msg: PoseStamped) -> None:
        st = self._state[arm]
        st.pose = msg
        t = msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9
        if t <= 0.0:  # unstamped publisher: fall back to node time
            t = self.get_clock().now().nanoseconds * 1e-9
        p = msg.pose.position
        st.speed = st.estimator.update(t, p.x, p.y, p.z)

    def _on_jaw(self, arm: str, msg: JointState) -> None:
        self._state[arm].jaw_angle = jaw_angle_from_joint_state(list(msg.name), list(msg.position))

    def _publish_all(self) -> None:
        for arm, st in self._state.items():
            if st.pose is None:
                continue
            out = ToolPose()
            out.header = st.pose.header
            if out.header.stamp.sec == 0 and out.header.stamp.nanosec == 0:
                out.header.stamp = self.get_clock().now().to_msg()
            out.arm = arm
            out.position = st.pose.pose.position
            out.orientation = st.pose.pose.orientation
            out.jaw_angle = st.jaw_angle if arm_has_jaw(arm) else NAN
            out.speed = float(st.speed) if math.isfinite(st.speed) else 0.0
            self._pubs[arm].publish(out)


def main(args=None) -> None:
    rclpy.init(args=args)
    node = ToolPoseAdapter()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
