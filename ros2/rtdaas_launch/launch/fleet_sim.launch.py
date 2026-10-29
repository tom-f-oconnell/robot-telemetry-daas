"""Launch N simulated dVRK robots plus the rtdaas adapter and Kafka bridge nodes.

    ros2 launch rtdaas_launch fleet_sim.launch.py robot_count:=2 \
        kafka_bootstrap:=kafka:9092 \
        schema_registry_url:=http://apicurio:8080/apis/ccompat/v7

Per robot ``i`` (namespace ``dv-00i``):

* for every arm in ``arms``: the dVRK kinematic simulation, included from ``dvrk_model`` and
  pushed into the robot namespace. The documented single-arm command is
  ``ros2 launch dvrk_model arm.launch.py arm:=PSM1 generation:=Classic``
  (https://dvrk.readthedocs.io/main/pages/usage/simulation.html); no hardware or FireWire
  adapter is needed and the ROS topics are retained.
* ``tool_pose_adapter`` and ``fleet_monitor`` (rtdaas_adapters) with ``source_namespace``
  set to the robot namespace;
* one ``kafka_bridge`` (rtdaas_kafka_bridge) with a params file rendered from
  ``config/bridge.yaml.template`` into a temporary directory.

Arguments
---------
robot_count            number of robots (default 2)
arms                   comma-separated arm list (default "PSM1,PSM2,ECM")
generation             dVRK arm generation: Classic | Si (default Classic)
launch_dvrk            include the dVRK simulation (default true; false = adapters + bridge only,
                       e.g. when the dVRK consoles are started separately or on another host)
rviz                   forwarded to the dVRK launch (default false; ignored if the installed
                       dvrk_model launch file has no such argument)
dvrk_launch_file       launch file inside dvrk_model to include per arm (default arm.launch.py)
kafka_bootstrap        default localhost:9092
schema_registry_url    default http://localhost:8081/apis/ccompat/v7
software_version       reported in Heartbeat.software_version (default dvrk-sim)
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, GroupAction, IncludeLaunchDescription, OpaqueFunction
from launch.conditions import IfCondition
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node, PushRosNamespace

ROS_TOPIC_PATTERNS = {
    # (kafka_topic, ros_topic suffix relative to /<robot>/<arm>, type, qos)
    "joint_states": ("rtdaas.telemetry.joint_states", "measured_js", "sensor_msgs/msg/JointState", "best_effort"),
    "tool_pose": ("rtdaas.telemetry.tool_pose", "rtdaas/tool_pose", "rtdaas_msgs/msg/ToolPose", "best_effort"),
}
ROBOT_TOPICS = {
    "events": ("rtdaas.telemetry.events", "rtdaas/events", "rtdaas_msgs/msg/SystemEvent", "reliable"),
    "heartbeat": ("rtdaas.telemetry.heartbeat", "rtdaas/heartbeat", "rtdaas_msgs/msg/Heartbeat", "reliable"),
}


def robot_id_for(index: int) -> str:
    return f"dv-{index:03d}"


def render_topics_block(robot_id: str, arms: list[str], indent: int = 8) -> str:
    """The ``topics:`` parameter tree for the bridge, one mapping per arm × kind + robot-level."""
    pad = " " * indent
    pad2 = " " * (indent + 2)
    lines: list[str] = []
    for arm in arms:
        for kind, (kafka_topic, suffix, msg_type, qos) in ROS_TOPIC_PATTERNS.items():
            lines += [
                f"{pad}{arm.lower()}_{kind}:",
                f"{pad2}ros_topic: /{robot_id}/{arm}/{suffix}",
                f"{pad2}kafka_topic: {kafka_topic}",
                f"{pad2}type: {msg_type}",
                f"{pad2}qos: {qos}",
            ]
    for name, (kafka_topic, suffix, msg_type, qos) in ROBOT_TOPICS.items():
        lines += [
            f"{pad}{name}:",
            f"{pad2}ros_topic: /{robot_id}/{suffix}",
            f"{pad2}kafka_topic: {kafka_topic}",
            f"{pad2}type: {msg_type}",
            f"{pad2}qos: {qos}",
        ]
    return "\n".join(lines)


def render_bridge_params(
    robot_id: str, arms: list[str], kafka_bootstrap: str, schema_registry_url: str, out_dir: Path
) -> Path:
    template = (Path(get_package_share_directory("rtdaas_launch")) / "config" / "bridge.yaml.template").read_text()
    text = template.format(
        robot_id=robot_id,
        kafka_bootstrap=kafka_bootstrap,
        schema_registry_url=schema_registry_url,
        topics_block=render_topics_block(robot_id, arms),
    )
    out = out_dir / f"bridge-{robot_id}.yaml"
    out.write_text(text)
    return out


def robot_actions(
    robot_id: str,
    arms: list[str],
    generation: str,
    launch_dvrk: bool,
    rviz: str,
    dvrk_launch_file: str,
    bridge_params: Path,
    software_version: str,
) -> list:
    actions: list = []
    if launch_dvrk:
        dvrk_share = get_package_share_directory("dvrk_model")
        for arm in arms:
            actions.append(
                GroupAction(
                    [
                        PushRosNamespace(robot_id),
                        IncludeLaunchDescription(
                            PythonLaunchDescriptionSource(os.path.join(dvrk_share, "launch", dvrk_launch_file)),
                            launch_arguments={"arm": arm, "generation": generation, "rviz": rviz}.items(),
                        ),
                    ]
                )
            )
    actions += [
        Node(
            package="rtdaas_adapters",
            executable="tool_pose_adapter",
            name="tool_pose_adapter",
            namespace=robot_id,
            output="screen",
            parameters=[{"robot_id": robot_id, "arms": arms, "source_namespace": robot_id, "rate_hz": 50.0}],
        ),
        Node(
            package="rtdaas_adapters",
            executable="fleet_monitor",
            name="fleet_monitor",
            namespace=robot_id,
            output="screen",
            parameters=[{
                "robot_id": robot_id,
                "arms": arms,
                "source_namespace": robot_id,
                "software_version": software_version,
            }],
        ),
        Node(
            package="rtdaas_kafka_bridge",
            executable="kafka_bridge",
            name="kafka_bridge",
            namespace=robot_id,
            output="screen",
            parameters=[str(bridge_params)],
            respawn=True,
            respawn_delay=5.0,
        ),
    ]
    return actions


def launch_setup(context, *args, **kwargs):
    robot_count = int(LaunchConfiguration("robot_count").perform(context))
    arms = [a.strip() for a in LaunchConfiguration("arms").perform(context).split(",") if a.strip()]
    generation = LaunchConfiguration("generation").perform(context)
    launch_dvrk = LaunchConfiguration("launch_dvrk").perform(context).lower() in ("true", "1", "yes")
    rviz = LaunchConfiguration("rviz").perform(context)
    dvrk_launch_file = LaunchConfiguration("dvrk_launch_file").perform(context)
    kafka_bootstrap = LaunchConfiguration("kafka_bootstrap").perform(context)
    registry_url = LaunchConfiguration("schema_registry_url").perform(context)
    software_version = LaunchConfiguration("software_version").perform(context)

    # Optional explicit ids (comma-separated) override the dv-00i numbering; used by single_robot_sim.
    explicit = [r.strip() for r in LaunchConfiguration("robot_ids").perform(context).split(",") if r.strip()]
    robot_ids = explicit[:robot_count] if explicit else [robot_id_for(i) for i in range(1, robot_count + 1)]

    out_dir = Path(tempfile.mkdtemp(prefix="rtdaas-bridge-"))
    actions: list = []
    for robot_id in robot_ids:
        params = render_bridge_params(robot_id, arms, kafka_bootstrap, registry_url, out_dir)
        actions += robot_actions(
            robot_id, arms, generation, launch_dvrk, rviz, dvrk_launch_file, params, software_version)
    return actions


def generate_launch_description() -> LaunchDescription:
    return LaunchDescription([
        DeclareLaunchArgument("robot_count", default_value="2"),
        DeclareLaunchArgument("robot_ids", default_value="", description="optional comma-separated ids overriding dv-00i"),
        DeclareLaunchArgument("arms", default_value="PSM1,PSM2,ECM"),
        DeclareLaunchArgument("generation", default_value="Classic", description="Classic | Si"),
        DeclareLaunchArgument("launch_dvrk", default_value="true"),
        DeclareLaunchArgument("rviz", default_value="false"),
        DeclareLaunchArgument("dvrk_launch_file", default_value="arm.launch.py"),
        DeclareLaunchArgument("kafka_bootstrap", default_value="localhost:9092"),
        DeclareLaunchArgument(
            "schema_registry_url", default_value="http://localhost:8081/apis/ccompat/v7"),
        DeclareLaunchArgument("software_version", default_value="dvrk-sim"),
        OpaqueFunction(function=launch_setup),
    ])
