# ROS 2 packages

| Package | Type | What it is |
|---|---|---|
| `rtdaas_msgs` | interfaces | `ToolPose`, `SystemEvent`, `Heartbeat`. Byte-identical to `contracts/msgs/rtdaas_msgs/msg` (`./check-msgs-in-sync.sh` enforces it; CI runs it in `make contracts`). |
| `rtdaas_adapters` | Python (rclpy) | `tool_pose_adapter` composes `ToolPose` from the dVRK `measured_cp` + `jaw/measured_js` topics and estimates tip speed; `fleet_monitor` publishes `Heartbeat` (1 Hz) and `SystemEvent`s (session boundaries, state transitions, joint-limit warnings, e-stop). Pure helpers live in `rtdaas_adapters/kinematics.py` and are tested without ROS. |
| `rtdaas_kafka_bridge` | C++17 | Generic ROS → Kafka bridge: `GenericSubscription` + rosidl introspection walk any message type, build an Avro datum against the schema fetched from the schema registry, and produce Confluent-wire records with librdkafka. New message types need a config entry, not code. |
| `rtdaas_launch` | launch | `fleet_sim.launch.py` (N robots: dVRK kinematic sim per arm + adapters + one bridge each) and `single_robot_sim.launch.py`. |

## Build

The dVRK ROS 2 stack is a separate workspace; follow
[dvrk.readthedocs.io](https://dvrk.readthedocs.io) for the `vcs import` of the dVRK repositories
and the simulation prerequisites. Then add this repo's `ros2/` directory as a second source tree:

```bash
mkdir -p ~/ros2_ws/src && cd ~/ros2_ws/src
git clone https://github.com/tom-f-oconnell/robot-telemetry-daas rtdaas   # or a symlink to ros2/
# ... dVRK repos imported here as documented upstream ...
sudo apt install librdkafka-dev libcurl4-openssl-dev nlohmann-json3-dev libyaml-cpp-dev \
     libboost-all-dev ros-jazzy-rosbag2-cpp ros-jazzy-rosidl-typesupport-introspection-cpp
# avro-cpp 1.11.3 from source (see infra/docker/bridge.Dockerfile for the exact commands)
cd ~/ros2_ws && colcon build --packages-up-to rtdaas_launch
source install/setup.bash
```

Or build the container, which does all of the above: `docker build -f infra/docker/bridge.Dockerfile -t rtdaas-bridge .`

## Run the fleet simulation

```bash
# compose stack up first (Kafka on localhost:29092, registry on :8081)
ros2 launch rtdaas_launch fleet_sim.launch.py robot_count:=2 arms:=PSM1,PSM2,ECM \
    kafka_bootstrap:=localhost:29092 schema_registry_url:=http://localhost:8081/apis/ccompat/v7
```

Per robot `dv_00i` this starts, under that namespace: the documented dVRK simulation per arm
(`ros2 launch dvrk_model arm.launch.py arm:=PSM1 generation:=Classic` — pass `launch_dvrk:=false`
if you already run the dVRK console yourself), `tool_pose_adapter`, `fleet_monitor`, and a
`kafka_bridge` with a generated params file mapping:

| ROS topic | Kafka topic |
|---|---|
| `/dv_00i/<arm>/measured_js` | `rtdaas.telemetry.joint_states` |
| `/dv_00i/<arm>/rtdaas/tool_pose` | `rtdaas.telemetry.tool_pose` |
| `/dv_00i/rtdaas/events` | `rtdaas.telemetry.events` |
| `/dv_00i/rtdaas/heartbeat` | `rtdaas.telemetry.heartbeat` |

Verify:

```bash
ros2 topic hz /dv_001/PSM1/rtdaas/tool_pose
kcat -b localhost:29092 -t rtdaas.telemetry.tool_pose -C -o -5 -e -f 'key=%k headers=%h\n'
ros2 topic echo /dv_001/kafka_bridge/stats       # produced / delivered / failed counters as JSON
```

## Record bags for the batch path

```bash
ros2 bag record -s mcap -o bags/dv_001 \
  /dv_001/PSM1/measured_js /dv_001/PSM1/rtdaas/tool_pose \
  /dv_001/PSM2/measured_js /dv_001/PSM2/rtdaas/tool_pose \
  /dv_001/ECM/measured_js  /dv_001/ECM/rtdaas/tool_pose \
  /dv_001/rtdaas/events    /dv_001/rtdaas/heartbeat
# then land the .mcap under s3://rtdaas-raw/bags/dv_001/<yyyy>/<mm>/<dd>/
```

The batch job decodes bags with the message definitions embedded in the file, so bags recorded
with an older `rtdaas_msgs` version still load.

## Known limitations

- In kinematic simulation the dVRK cannot detect the mounted instrument; select one in the arm
  configuration or GUI or the PSM has no Cartesian pose (then `tool_pose` is empty).
- `rtdaas_kafka_bridge` fetches the subject's latest schema at startup; after registering a new
  schema version, restart the bridge.
- Integer fields wider than Avro `long` (`uint64` above 2^63) are rejected by the mapper.
- DDS discovery across container/pod boundaries needs the CycloneDDS configuration shipped in the
  image (`/config/cyclonedds.xml`) or host networking.
- A robot id is also its ROS namespace (`/dv_001/PSM1/measured_js`) and the `robot_id` of every
  record (the bag pipeline reads it from the topic path), so ids must be valid ROS 2 names:
  letters, digits and `_`, starting with a letter. `dv-001` (the original form) is rejected.
- `tool_pose_adapter` keeps republishing the last pose at its rate after the dVRK topics stop.
- The C++ package is compiled by the `bridge.Dockerfile` build in CI, and the image has run
  against the compose stack (adapters + bridge, `launch_dvrk:=false`, dVRK-shaped topics
  published by hand): `rosbag2_cpp::get_typesupport_handle` and the introspection walk work for
  every message in the contract. The walk's bounded-sequence branch is still unexercised (no
  contract message uses one), and nothing has yet run against the dVRK simulation itself.
