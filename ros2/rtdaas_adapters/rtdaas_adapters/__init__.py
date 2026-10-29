"""rtdaas_adapters: ROS 2 nodes that turn raw dVRK topics into the rtdaas telemetry contract.

- ``tool_pose_adapter``: ``<arm>/measured_cp`` + ``<arm>/jaw/measured_js`` -> ``rtdaas_msgs/ToolPose``
- ``fleet_monitor``: operating states + joint limits -> ``rtdaas_msgs/Heartbeat`` and ``SystemEvent``

The math lives in :mod:`rtdaas_adapters.kinematics` (no ROS imports) so it is testable with pytest.
"""

__version__ = "0.1.0"
