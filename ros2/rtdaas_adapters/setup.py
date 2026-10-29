from setuptools import find_packages, setup

package_name = "rtdaas_adapters"

setup(
    name=package_name,
    version="0.1.0",
    packages=find_packages(exclude=["test"]),
    data_files=[
        ("share/ament_index/resource_index/packages", ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
        ("share/" + package_name + "/config", ["config/adapters.yaml"]),
    ],
    install_requires=["setuptools"],
    zip_safe=True,
    maintainer="Thomas O'Connell",
    maintainer_email="tom.oconn3ll@gmail.com",
    description="Adapter nodes turning raw dVRK topics into the rtdaas telemetry contract "
    "(ToolPose, Heartbeat, SystemEvent).",
    license="MIT",
    tests_require=["pytest"],
    entry_points={
        "console_scripts": [
            "tool_pose_adapter = rtdaas_adapters.tool_pose_adapter:main",
            "fleet_monitor = rtdaas_adapters.fleet_monitor:main",
        ],
    },
)
