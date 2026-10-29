from __future__ import annotations

import json
from pathlib import Path

from rtdaas.contracts import avro_gen
from rtdaas.contracts.avro_binary import parse_schema
from rtdaas.contracts.msgspec import MsgRegistry, parse_msg


def _field(schema: dict, name: str) -> dict:
    return next(f for f in schema["fields"] if f["name"] == name)


def test_primitive_mapping_table():
    expected = {
        "bool": "boolean",
        "int8": "int",
        "uint8": "int",
        "int16": "int",
        "uint16": "int",
        "int32": "int",
        "byte": "int",
        "char": "int",
        "int64": "long",
        "uint32": "long",
        "uint64": "long",
        "float32": "float",
        "float64": "double",
        "string": "string",
        "wstring": "string",
    }
    assert avro_gen.PRIMITIVE_TO_AVRO == expected


def test_tool_pose_schema_shape(msg_registry: MsgRegistry):
    schema = avro_gen.msg_to_avro(msg_registry.get("rtdaas_msgs/ToolPose"), msg_registry)
    assert (schema["type"], schema["name"], schema["namespace"]) == ("record", "ToolPose", "rtdaas_msgs")
    assert [f["name"] for f in schema["fields"]] == ["header", "arm", "position", "orientation", "jaw_angle", "speed"]
    header = _field(schema, "header")["type"]
    assert header["name"] == "Header" and header["namespace"] == "std_msgs"
    stamp = _field(header, "stamp")["type"]
    assert stamp["namespace"] == "builtin_interfaces"
    assert [f["type"] for f in stamp["fields"]] == ["int", "long"]  # int32 -> int, uint32 -> long
    assert _field(schema, "jaw_angle")["type"] == "double"
    quat = _field(schema, "orientation")["type"]
    assert _field(quat, "w")["default"] == 1.0 and _field(quat, "x")["default"] == 0.0
    assert "jaw" in _field(schema, "jaw_angle")["doc"].lower()
    parse_schema(schema)  # must be a valid, non-recursive Avro schema


def test_nested_types_defined_once_then_referenced(msg_registry: MsgRegistry):
    schema = avro_gen.msg_to_avro(msg_registry.get("geometry_msgs/PoseStamped"), msg_registry)
    # Header.stamp defines builtin_interfaces.Time; nothing else references it here, but Pose
    # defines Point and Quaternion inline. Build a type that uses Header twice to check reuse.
    twice = parse_msg("demo_msgs", "Twice", "std_msgs/Header a\nstd_msgs/Header b\n")
    reg = MsgRegistry({twice.full_name: twice, "std_msgs/Header": msg_registry.get("std_msgs/Header"), "builtin_interfaces/Time": msg_registry.get("builtin_interfaces/Time")})
    s2 = avro_gen.msg_to_avro(twice, reg)
    assert isinstance(_field(s2, "a")["type"], dict)
    assert _field(s2, "b")["type"] == "std_msgs.Header"
    parse_schema(s2)
    parse_schema(schema)


def test_arrays_constants_and_docs(msg_registry: MsgRegistry):
    js = avro_gen.msg_to_avro(msg_registry.get("sensor_msgs/JointState"), msg_registry)
    assert _field(js, "name")["type"] == {"type": "array", "items": "string"}
    assert _field(js, "position")["type"] == {"type": "array", "items": "double"}
    ev = avro_gen.msg_to_avro(msg_registry.get("rtdaas_msgs/SystemEvent"), msg_registry)
    assert [f["name"] for f in ev["fields"]] == ["header", "arm", "severity", "code", "message"]  # constants are not fields
    assert "SEVERITY_WARN=2" in ev["doc"]
    assert _field(ev, "severity")["type"] == "int"


def test_kafka_record_schema_envelope(msg_registry: MsgRegistry):
    msg_schema = avro_gen.msg_to_avro(msg_registry.get("rtdaas_msgs/Heartbeat"), msg_registry)
    env = avro_gen.kafka_record_schema(msg_schema, "rtdaas.telemetry.heartbeat")
    assert env["name"] == "HeartbeatRecord" and env["namespace"] == "rtdaas.kafka"
    assert [(f["name"], f["type"] if isinstance(f["type"], str) else "record") for f in env["fields"]] == [
        ("robot_id", "string"),
        ("ros_topic", "string"),
        ("recv_time_ns", "long"),
        ("msg", "record"),
    ]
    assert _field(env, "msg")["type"] is msg_schema
    parse_schema(env)


def test_generate_all_is_deterministic_and_complete(msg_registry: MsgRegistry, registry_yml: Path, tmp_path: Path):
    topics = avro_gen.load_topics(registry_yml)
    assert topics == {
        "rtdaas.telemetry.joint_states": "sensor_msgs/JointState",
        "rtdaas.telemetry.tool_pose": "rtdaas_msgs/ToolPose",
        "rtdaas.telemetry.events": "rtdaas_msgs/SystemEvent",
        "rtdaas.telemetry.heartbeat": "rtdaas_msgs/Heartbeat",
    }
    written = avro_gen.generate_all(msg_registry, topics, tmp_path)
    rel = sorted(p.relative_to(tmp_path).as_posix() for p in written)
    assert len(rel) == len(msg_registry) + len(topics)
    assert "kafka/rtdaas.telemetry.tool_pose.avsc" in rel and "sensor_msgs.JointState.avsc" in rel
    first = {p: p.read_bytes() for p in written}
    avro_gen.generate_all(msg_registry, topics, tmp_path)
    assert {p: p.read_bytes() for p in written} == first
    for p in written:
        text = p.read_text()
        assert text.endswith("\n")
        assert json.loads(text)["type"] == "record"


def test_committed_schemas_are_up_to_date(msg_registry: MsgRegistry, registry_yml: Path, repo_root: Path):
    """CI runs `rtdaas-contracts gen` + git diff; this is the same check in-process."""
    topics = avro_gen.load_topics(registry_yml)
    expected = avro_gen.render_all(msg_registry, topics)
    avro_dir = repo_root / "contracts" / "avro"
    for rel, text in expected.items():
        assert (avro_dir / rel).read_text(encoding="utf-8") == text, f"{rel} is stale: run rtdaas-contracts gen"
