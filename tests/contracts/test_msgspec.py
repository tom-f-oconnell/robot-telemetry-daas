from __future__ import annotations

import pytest

from rtdaas.contracts.msgspec import MsgParseError, MsgRegistry, parse_msg

EXPECTED_TYPES = {
    "builtin_interfaces/Time",
    "std_msgs/Header",
    "sensor_msgs/JointState",
    "geometry_msgs/Point",
    "geometry_msgs/Quaternion",
    "geometry_msgs/Pose",
    "geometry_msgs/PoseStamped",
    "rtdaas_msgs/ToolPose",
    "rtdaas_msgs/SystemEvent",
    "rtdaas_msgs/Heartbeat",
}


def test_parse_fields_constants_defaults_and_docs():
    text = """# A doc line
# second doc line
uint8 LEVEL_A=1
uint8 LEVEL_B=2
string NAME_CONST='x'

std_msgs/Header header
# the arm
string arm
float64 w 1
float64[] values
int32[3] fixed
uint8[<=4] bounded
string<=16 short_name
bool flag true   # inline doc
Point position
Header bare_header
"""
    msg = parse_msg("demo_msgs", "Demo", text)
    assert msg.full_name == "demo_msgs/Demo"
    assert [c.name for c in msg.constants] == ["LEVEL_A", "LEVEL_B", "NAME_CONST"]
    assert msg.constants[0].value == 1 and msg.constants[2].value == "x"
    names = [f.name for f in msg.fields]
    assert names == ["header", "arm", "w", "values", "fixed", "bounded", "short_name", "flag", "position", "bare_header"]
    assert msg.field("header").type == "std_msgs/Header"
    assert msg.field("arm").doc == "the arm"
    assert msg.field("w").default == 1.0
    f = msg.field("values")
    assert f.is_array and f.array_len is None and not f.bounded
    f = msg.field("fixed")
    assert f.is_array and f.array_len == 3 and not f.bounded
    f = msg.field("bounded")
    assert f.is_array and f.array_len == 4 and f.bounded
    assert msg.field("short_name").string_bound == 16
    assert msg.field("flag").default is True and msg.field("flag").doc == "inline doc"
    assert msg.field("position").type == "demo_msgs/Point"  # bare type resolves in-package
    assert msg.field("bare_header").type == "std_msgs/Header"  # ROS convention
    # a blank line resets pending docs: the constants' doc must not leak onto "header"
    assert msg.field("header").doc is None


def test_parse_rejects_garbage():
    with pytest.raises(MsgParseError):
        parse_msg("p", "Bad", "this is not a field\n")


def test_load_dir_finds_all_types(msg_registry: MsgRegistry):
    assert set(msg_registry.names()) == EXPECTED_TYPES
    assert len(msg_registry) == len(EXPECTED_TYPES)
    assert "rtdaas_msgs/ToolPose" in msg_registry
    with pytest.raises(KeyError):
        msg_registry.get("nope/Nope")


def test_dependencies_are_transitive_and_ordered(msg_registry: MsgRegistry):
    assert msg_registry.dependencies("builtin_interfaces/Time") == []
    assert msg_registry.dependencies("std_msgs/Header") == ["builtin_interfaces/Time"]
    deps = msg_registry.dependencies("rtdaas_msgs/ToolPose")
    assert deps == ["std_msgs/Header", "builtin_interfaces/Time", "geometry_msgs/Point", "geometry_msgs/Quaternion"]
    assert msg_registry.dependencies("sensor_msgs/JointState") == ["std_msgs/Header", "builtin_interfaces/Time"]


def test_ros2msg_schema_text_layout(msg_registry: MsgRegistry):
    text = msg_registry.ros2msg_schema_text("std_msgs/Header")
    assert text.startswith("# Standard metadata")
    sep = "=" * 80
    assert text.count(sep) == 1
    assert f"{sep}\nMSG: builtin_interfaces/Time\n" in text
    assert msg_registry.ros2msg_schema_text("builtin_interfaces/Time").count(sep) == 0


@pytest.mark.parametrize("type_name", sorted(EXPECTED_TYPES))
def test_ros2msg_round_trip_every_type(msg_registry: MsgRegistry, type_name: str):
    text = msg_registry.ros2msg_schema_text(type_name)
    parsed = MsgRegistry.from_ros2msg_schema(type_name, text)
    assert set(parsed.names()) == {type_name, *msg_registry.dependencies(type_name)}
    for name in parsed.names():
        a, b = parsed.get(name), msg_registry.get(name)
        assert [(f.name, f.type, f.is_array, f.array_len, f.bounded, f.default) for f in a.fields] == [
            (f.name, f.type, f.is_array, f.array_len, f.bounded, f.default) for f in b.fields
        ]
        assert [(c.name, c.value) for c in a.constants] == [(c.name, c.value) for c in b.constants]
    # and the text re-serialises identically
    assert parsed.ros2msg_schema_text(type_name) == text


def test_from_ros2msg_schema_rejects_missing_msg_header():
    with pytest.raises(MsgParseError):
        MsgRegistry.from_ros2msg_schema("p/A", "int32 x\n" + "=" * 80 + "\nint32 y\n")


def test_merge_prefers_self_by_default(msg_registry: MsgRegistry):
    old = parse_msg("rtdaas_msgs", "ToolPose", "std_msgs/Header header\nstring arm\n")
    bag_registry = MsgRegistry({old.full_name: old, **{n: msg_registry.get(n) for n in msg_registry.dependencies("rtdaas_msgs/ToolPose")}})
    merged = bag_registry.merge(msg_registry)
    assert len(merged.get("rtdaas_msgs/ToolPose").fields) == 2  # bag definition wins
    assert "sensor_msgs/JointState" in merged  # repo types fill the gaps
    merged2 = bag_registry.merge(msg_registry, prefer_self=False)
    assert len(merged2.get("rtdaas_msgs/ToolPose").fields) == 6
