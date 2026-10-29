# rtdaas-bridge: ROS 2 Jazzy + the rtdaas ROS packages (C++ Kafka bridge, Python adapters, launch).
#
# avro-cpp is built from the Apache source release because Ubuntu 24.04 does not ship a usable
# libavro-cpp package. Everything else comes from apt. The image runs the bridge by default; the
# adapters and the dVRK simulation are launched with `ros2 launch rtdaas_launch ...` (see ros2/README.md).
FROM ros:jazzy-ros-base AS build

ARG AVRO_VERSION=1.11.3
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential cmake git curl ca-certificates pkg-config \
      librdkafka-dev libcurl4-openssl-dev nlohmann-json3-dev libyaml-cpp-dev \
      libboost-filesystem-dev libboost-iostreams-dev libboost-program-options-dev libboost-regex-dev libboost-system-dev \
      libsnappy-dev zlib1g-dev \
      ros-jazzy-rosbag2-cpp ros-jazzy-rosidl-typesupport-introspection-cpp ros-jazzy-rmw-cyclonedds-cpp \
      ros-jazzy-sensor-msgs ros-jazzy-geometry-msgs ros-jazzy-std-msgs \
      python3-colcon-common-extensions python3-pip python3-numpy \
  && rm -rf /var/lib/apt/lists/*

# avro-cpp from source (installs to /usr/local). 1.11.3 compiles with -Werror and Boost 1.83 emits a
# '#warning' about C++14 under C++11, so build as C++17 and leave -Wcpp out of -Werror.
RUN curl -fsSL "https://archive.apache.org/dist/avro/avro-${AVRO_VERSION}/cpp/avro-cpp-${AVRO_VERSION}.tar.gz" \
      -o /tmp/avro-cpp.tar.gz \
  && mkdir -p /tmp/avro-cpp && tar -xzf /tmp/avro-cpp.tar.gz -C /tmp/avro-cpp --strip-components=1 \
  && sed -i 's/-std=c++11/-std=c++17/g; s/CMAKE_CXX_STANDARD 11/CMAKE_CXX_STANDARD 17/' /tmp/avro-cpp/CMakeLists.txt \
  && cmake -S /tmp/avro-cpp -B /tmp/avro-cpp/build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 \
       -DCMAKE_CXX_FLAGS="-Wno-cpp -Wno-error=cpp" \
  && cmake --build /tmp/avro-cpp/build -j"$(nproc)" \
  && cmake --install /tmp/avro-cpp/build \
  && ldconfig && rm -rf /tmp/avro-cpp /tmp/avro-cpp.tar.gz

WORKDIR /ws
COPY ros2/ /ws/src/rtdaas/
RUN . /opt/ros/jazzy/setup.sh \
  && colcon build --merge-install --cmake-args -DCMAKE_BUILD_TYPE=Release \
       --packages-select rtdaas_msgs rtdaas_kafka_bridge rtdaas_adapters rtdaas_launch

# ---------------------------------------------------------------- runtime
FROM ros:jazzy-ros-base

ENV DEBIAN_FRONTEND=noninteractive \
    RMW_IMPLEMENTATION=rmw_cyclonedds_cpp \
    CYCLONEDDS_URI=file:///config/cyclonedds.xml
RUN apt-get update && apt-get install -y --no-install-recommends \
      librdkafka++1 librdkafka1 libcurl4 libyaml-cpp0.8 \
      libboost-filesystem1.83.0 libboost-iostreams1.83.0 libboost-program-options1.83.0 libboost-regex1.83.0 libboost-system1.83.0 \
      libsnappy1v5 \
      ros-jazzy-rosbag2-cpp ros-jazzy-rosidl-typesupport-introspection-cpp ros-jazzy-rmw-cyclonedds-cpp \
      ros-jazzy-sensor-msgs ros-jazzy-geometry-msgs ros-jazzy-std-msgs python3-numpy \
  && rm -rf /var/lib/apt/lists/* \
  && groupadd --gid 10001 rtdaas && useradd --uid 10001 --gid rtdaas --create-home rtdaas

COPY --from=build /usr/local/lib/libavrocpp* /usr/local/lib/
COPY --from=build /ws/install /ws/install
COPY ros2/rtdaas_kafka_bridge/config/bridge.yaml /config/bridge.yaml
RUN ldconfig && printf '%s\n' \
  '<?xml version="1.0" encoding="UTF-8"?>' \
  '<CycloneDDS><Domain><General><Interfaces><NetworkInterface autodetermine="true"/></Interfaces>' \
  '<AllowMulticast>spdp</AllowMulticast></General></Domain></CycloneDDS>' > /config/cyclonedds.xml \
  && chown -R rtdaas:rtdaas /config

COPY --chmod=0755 <<'EOF' /entrypoint.sh
#!/usr/bin/env bash
set -e
source /opt/ros/jazzy/setup.bash
source /ws/install/setup.bash
exec "$@"
EOF

USER rtdaas
ENTRYPOINT ["/entrypoint.sh"]
# Override the params file via a mounted /config/bridge.yaml (Kubernetes: ConfigMap rtdaas-bridge-config).
CMD ["ros2", "run", "rtdaas_kafka_bridge", "kafka_bridge", "--ros-args", "--params-file", "/config/bridge.yaml"]
