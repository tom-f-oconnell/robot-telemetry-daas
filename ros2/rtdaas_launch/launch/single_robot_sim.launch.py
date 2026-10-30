"""One simulated robot: a thin wrapper around ``fleet_sim.launch.py`` with ``robot_count:=1``.

    ros2 launch rtdaas_launch single_robot_sim.launch.py robot_id:=dv_001 arms:=PSM1,ECM \
        kafka_bootstrap:=localhost:29092 schema_registry_url:=http://localhost:8081/apis/ccompat/v7

Everything (dVRK kinematic simulation per arm, tool_pose_adapter, fleet_monitor, kafka_bridge)
comes from the fleet launch; this file only pins the robot count and exposes ``robot_id``.
"""

from __future__ import annotations

import os

from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, IncludeLaunchDescription
from launch.launch_description_sources import PythonLaunchDescriptionSource
from launch.substitutions import LaunchConfiguration


def generate_launch_description() -> LaunchDescription:
    fleet = os.path.join(get_package_share_directory("rtdaas_launch"), "launch", "fleet_sim.launch.py")
    forwarded = [
        "arms", "generation", "launch_dvrk", "rviz", "dvrk_launch_file",
        "kafka_bootstrap", "schema_registry_url", "software_version",
    ]
    return LaunchDescription(
        [
            DeclareLaunchArgument("robot_id", default_value="dv_001", description="namespace of the single robot"),
            DeclareLaunchArgument("arms", default_value="PSM1,PSM2,ECM"),
            DeclareLaunchArgument("generation", default_value="Classic"),
            DeclareLaunchArgument("launch_dvrk", default_value="true"),
            DeclareLaunchArgument("rviz", default_value="false"),
            DeclareLaunchArgument("dvrk_launch_file", default_value="arm.launch.py"),
            DeclareLaunchArgument("kafka_bootstrap", default_value="localhost:9092"),
            DeclareLaunchArgument("schema_registry_url", default_value="http://localhost:8081/apis/ccompat/v7"),
            DeclareLaunchArgument("software_version", default_value="dvrk-sim"),
            IncludeLaunchDescription(
                PythonLaunchDescriptionSource(fleet),
                launch_arguments={
                    "robot_count": "1",
                    "robot_ids": LaunchConfiguration("robot_id"),
                    **{k: LaunchConfiguration(k) for k in forwarded},
                }.items(),
            ),
        ]
    )
