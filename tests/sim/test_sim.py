from __future__ import annotations

import json

from rtdaas.contracts.avro_binary import encode
from rtdaas.contracts.cdr import CdrCodec
from rtdaas.mcap_lite import read_messages
from rtdaas.sim.cli import main as sim_main
from rtdaas.sim.robot import RobotSim, SimConfig

TOPIC_TYPES = {
    "rtdaas.telemetry.joint_states": "sensor_msgs/JointState",
    "rtdaas.telemetry.tool_pose": "rtdaas_msgs/ToolPose",
    "rtdaas.telemetry.events": "rtdaas_msgs/SystemEvent",
    "rtdaas.telemetry.heartbeat": "rtdaas_msgs/Heartbeat",
}


def _run(seconds=3.0, rate=20.0, seed=0):
    sim = RobotSim(SimConfig(robot_id="dv-009", rate_hz=rate, seed=seed))
    t0 = 1_700_000_000 * 10**9
    out = list(sim.begin(t0))
    n = int(seconds * rate)
    for i in range(n):
        out.extend(sim.tick(t0 + int(i * 1e9 / rate)))
    out.extend(sim.end(t0 + int(n * 1e9 / rate)))
    return out


def test_messages_validate_against_avro_schemas(repo_root):
    msgs = _run()
    schemas = {}
    for topic, type_name in TOPIC_TYPES.items():
        pkg, name = type_name.split("/")
        schemas[topic] = json.loads(
            (repo_root / "contracts" / "avro" / f"{pkg}.{name}.avsc").read_text()
        )
    seen = set()
    for kafka_topic, ros_topic, msg in msgs:
        assert kafka_topic in schemas, kafka_topic
        assert ros_topic.startswith("/dv-009/")
        encode(schemas[kafka_topic], msg)  # raises on any schema violation
        seen.add(kafka_topic)
    assert seen == set(TOPIC_TYPES)


def test_event_and_heartbeat_structure():
    msgs = _run(seconds=4.0)
    events = [m for t, _, m in msgs if t == "rtdaas.telemetry.events"]
    codes = [e["code"] for e in events]
    assert codes[0] == "SESSION_START" and codes[-1] == "SESSION_END"
    heartbeats = [m for t, _, m in msgs if t == "rtdaas.telemetry.heartbeat"]
    assert 3 <= len(heartbeats) <= 6  # 1 Hz over 4 s
    assert all(h["robot_id"] == "dv-009" for h in heartbeats)
    poses = [m for t, _, m in msgs if t == "rtdaas.telemetry.tool_pose"]
    assert {p["arm"] for p in poses} == {"PSM1", "PSM2", "ECM"}
    assert all(p["speed"] >= 0 for p in poses)


def test_deterministic_given_seed():
    def dump(m):  # NaN-safe comparison (ECM has no jaw)
        return json.dumps(m, sort_keys=True)

    a = dump(_run(seconds=1.0, seed=7))
    b = dump(_run(seconds=1.0, seed=7))
    assert a == b
    assert a != dump(_run(seconds=1.0, seed=8))


def test_cli_writes_decodable_bags(tmp_path, msg_registry):
    rc = sim_main(
        [
            "run",
            "--robots",
            "2",
            "--duration",
            "3",
            "--rate",
            "20",
            "--seed",
            "1",
            "--mcap-out",
            str(tmp_path),
            "--no-realtime",
            "--start-time",
            "2026-10-28T10:00:00Z",
        ]
    )
    assert rc == 0
    bags = sorted(tmp_path.rglob("*.mcap"))
    assert [b.relative_to(tmp_path).as_posix() for b in bags] == [
        "dv-001/2026/10/28/dv-001-20261028T100000.mcap",
        "dv-002/2026/10/28/dv-002-20261028T100000.mcap",
    ]
    codec = CdrCodec(msg_registry)
    stamps = []
    counts: dict[str, int] = {}
    for schema, channel, message in read_messages(bags[0]):
        assert schema.encoding == "ros2msg" and channel.message_encoding == "cdr"
        decoded = codec.decode(schema.name, message.data)
        counts[schema.name] = counts.get(schema.name, 0) + 1
        if schema.name == "rtdaas_msgs/ToolPose" and channel.topic.endswith(
            "/PSM1/rtdaas/tool_pose"
        ):
            stamps.append(
                decoded["header"]["stamp"]["sec"] * 10**9 + decoded["header"]["stamp"]["nanosec"]
            )
    assert counts["rtdaas_msgs/ToolPose"] == 3 * 60  # 3 arms x 3 s x 20 Hz
    assert counts["sensor_msgs/JointState"] == 3 * 60
    assert counts["rtdaas_msgs/SystemEvent"] >= 2
    assert stamps == sorted(stamps) and len(set(stamps)) == len(stamps)


def test_cli_rejects_missing_sink():
    assert sim_main(["run", "--robots", "1", "--duration", "1"]) == 1
