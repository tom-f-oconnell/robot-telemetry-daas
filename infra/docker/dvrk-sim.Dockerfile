# dVRK kinematic simulation + the rtdaas ROS 2 packages in one image, to run the fleet launch
# against real dVRK topics (CRTK operating_state, measured_cp / measured_js from dvrk_system).
# Opt-in and not built by CI: the dVRK build (cisst, saw*, dvrk_robot) takes a while.
#
#   docker build -f infra/docker/dvrk-sim.Dockerfile \
#     --build-arg BRIDGE_IMAGE=ghcr.io/tom-f-oconnell/rtdaas-bridge:<sha> -t rtdaas-dvrk-sim .
#   docker run --rm --network rtdaas_default rtdaas-dvrk-sim \
#     ros2 launch rtdaas_launch fleet_sim.launch.py robot_count:=1 arms:=PSM1 \
#       kafka_bootstrap:=kafka:9092 schema_registry_url:=http://apicurio:8080/apis/ccompat/v7
#
# Layered on rtdaas-bridge (ros:jazzy-ros-base + /ws/install with rtdaas_msgs, the adapters,
# the bridge and the launch package); the dVRK goes into a separate overlay, /dvrk_ws, built with
# colcon's default per-package install as documented (cisst's CMake breaks with --merge-install:
# sawKeyboard's install step looks for its library under the merged prefix).
ARG BRIDGE_IMAGE=ghcr.io/tom-f-oconnell/rtdaas-bridge:latest
FROM ${BRIDGE_IMAGE}

# Pinned dVRK release (https://dvrk.readthedocs.io, "ROS 2" compilation page).
ARG DVRK_VCS=https://raw.githubusercontent.com/jhu-saw/vcs/main/ros2-dvrk-2.5.0.vcs

USER root
ENV DEBIAN_FRONTEND=noninteractive
# The dVRK's documented Ubuntu 24.04 build dependencies, minus the stereo-video / IDE extras.
# rviz2 only because dvrk_model's arm.launch.py starts it unconditionally (it cannot render
# here and exits; the rest of the launch is unaffected).
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential git python-is-python3 python3-vcstool python3-pykdl \
      libraw1394-dev libncurses-dev swig sox espeak libcppunit-dev \
      qt6-base-dev qt6-5compat-dev qt6-tools-dev qt6-declarative-dev qt6-svg-dev \
      libgl-dev libegl-dev libhidapi-dev python3-pyudev libgraphviz-dev libjsoncpp-dev \
      ros-jazzy-joint-state-publisher ros-jazzy-robot-state-publisher ros-jazzy-xacro \
      ros-jazzy-rviz2 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /dvrk_ws
RUN mkdir src && vcs import --retry 10 --workers 4 --recursive --input "${DVRK_VCS}" src \
  && . /opt/ros/jazzy/setup.sh \
  && apt-get update \
  && (rosdep update --rosdistro jazzy >/dev/null) \
  && rosdep install --from-paths src --ignore-src -r -y --rosdistro jazzy \
       --skip-keys "joint_state_publisher_gui" \
  && rm -rf /var/lib/apt/lists/* \
  && colcon build \
       --packages-up-to dvrk_robot dvrk_model dvrk_config dvrk_python \
       --cmake-args -DCMAKE_BUILD_TYPE=Release \
  && rm -rf build log

# dvrk_system always builds its Qt GUI, which uses OpenGL widgets: Qt's "offscreen" platform has
# no GL context and the process segfaults. A virtual X server with Mesa's software GL works (rviz2
# too); the entrypoint starts one unless a DISPLAY is given.
RUN apt-get update && apt-get install -y --no-install-recommends xvfb xauth libgl1-mesa-dri \
  && rm -rf /var/lib/apt/lists/*
COPY --chmod=0755 <<'EOF' /entrypoint.sh
#!/usr/bin/env bash
set -e
if [[ -z "${DISPLAY:-}" ]]; then
  Xvfb :99 -screen 0 1280x1024x24 -nolisten tcp >/tmp/xvfb.log 2>&1 &
  export DISPLAY=:99
fi
source /opt/ros/jazzy/setup.bash
source /dvrk_ws/install/setup.bash
source /ws/install/setup.bash
exec "$@"
EOF

USER rtdaas
WORKDIR /home/rtdaas
CMD ["ros2", "launch", "rtdaas_launch", "fleet_sim.launch.py", "robot_count:=1", "arms:=PSM1"]
