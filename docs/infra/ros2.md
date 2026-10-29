# ROS 2 bridge and dVRK simulation

The real-time path starts in ROS 2. The open-source dVRK stack runs a **kinematic simulation**
of da Vinci research arms without hardware and still publishes the normal ROS topics, so a fleet
of simulated robots is just N namespaced launches.

## Packages (`ros2/`)

| Package | Language | Purpose |
|---|---|---|
| `rtdaas_msgs` | IDL | `ToolPose`, `SystemEvent`, `Heartbeat` — byte-identical to `contracts/msgs/rtdaas_msgs` (`ros2/check-msgs-in-sync.sh` enforces it) |
| `rtdaas_adapters` | Python (rclpy) | `tool_pose_adapter`: `measured_cp` + `jaw/measured_js` → `ToolPose` with speed; `fleet_monitor`: operating state → `Heartbeat` at 1 Hz and `SystemEvent`s (session boundaries, state transitions, joint-limit warnings, e-stop) |
| `rtdaas_kafka_bridge` | C++17 | generic topic → Kafka bridge: `GenericSubscription` + rosidl introspection walks any message and builds an Avro datum against the schema fetched from the registry; librdkafka producer; Confluent wire format |
| `rtdaas_launch` | launch | `fleet_sim.launch.py` (N robots: dVRK arms + adapters + one bridge each), `single_robot_sim.launch.py` |

## Why a generic bridge

A new message type needs **no bridge code** — only a `.msg`, a registry entry and a line in
`bridge.yaml`. The bridge never embeds a schema: it asks the registry for the subject's latest
version at startup and encodes with that. If the ROS definition and the registry disagree the
bridge refuses to start, which is the right failure mode for a contract violation.

## Running the fleet simulation

```bash
# workspace with dVRK ROS 2 packages (follow dvrk.readthedocs.io for the vcs import + build) and ros2/ from this repo
colcon build --packages-up-to rtdaas_launch
source install/setup.bash
ros2 launch rtdaas_launch fleet_sim.launch.py robot_count:=2 \
    kafka_bootstrap:=localhost:29092 \
    schema_registry_url:=http://localhost:8081/apis/ccompat/v7
```

Each robot `dv-00N` runs PSM1, PSM2 and ECM in kinematic simulation (the documented
`ros2 launch dvrk_model arm.launch.py arm:=PSM1 generation:=Classic` per arm), the two adapter
nodes, and a bridge configured for that namespace. Verify with
`kcat -b localhost:29092 -t rtdaas.telemetry.tool_pose -C -c 5 -f '%k: %h\n'`.

Recording bags for the batch path:

```bash
ros2 bag record -s mcap -o bags/dv-001 /dv-001/PSM1/measured_js /dv-001/PSM1/rtdaas/tool_pose \
    /dv-001/PSM2/measured_js /dv-001/PSM2/rtdaas/tool_pose /dv-001/ECM/measured_js \
    /dv-001/rtdaas/events /dv-001/rtdaas/heartbeat
```

then copy the `.mcap` to `s3://rtdaas-raw/bags/dv-001/<yyyy>/<mm>/<dd>/`.

## Without ROS: the simulator

`rtdaas-sim` produces the same four message types through the same Avro envelope (wire-identical
Kafka records) and writes ros2-bag-compatible MCAP files (`cdr` + `ros2msg`), so the whole platform
can be exercised — and is tested — without a ROS installation. The compose `demo` profile uses it.

## Known limitations

- In kinematic simulation the dVRK cannot detect which instrument is mounted; set it in the arm
  config/GUI, otherwise the PSM has no Cartesian pose and `tool_pose` stays empty.
- `rtdaas_kafka_bridge` depends on avro-cpp, which Ubuntu does not package reliably; the
  `bridge.Dockerfile` builds Avro 1.11.3 from the Apache source tarball.
- DDS discovery across pods needs a CycloneDDS configuration (provided as a ConfigMap) or host
  networking; the compose demo does not run ROS at all.
