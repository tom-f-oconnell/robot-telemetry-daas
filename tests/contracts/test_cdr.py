from __future__ import annotations

import math
import struct

import pytest

from rtdaas.contracts.cdr import CdrCodec, CdrError
from rtdaas.contracts.msgspec import MsgRegistry, parse_msg


def _registry_with_string(base: MsgRegistry) -> MsgRegistry:
    reg = MsgRegistry({name: base.get(name) for name in base.names()})
    reg.add(parse_msg("std_msgs", "String", "string data\n"))
    return reg


def test_golden_std_msgs_string(msg_registry: MsgRegistry):
    codec = CdrCodec(_registry_with_string(msg_registry))
    data = codec.encode("std_msgs/String", {"data": "hello"})
    assert data == bytes.fromhex("00010000 06000000 68656c6c6f 00".replace(" ", ""))
    assert codec.decode("std_msgs/String", data) == {"data": "hello"}


def test_empty_string_and_big_endian_decode(msg_registry: MsgRegistry):
    codec = CdrCodec(_registry_with_string(msg_registry))
    assert codec.encode("std_msgs/String", {"data": ""}) == bytes.fromhex(
        "00010000 01000000 00".replace(" ", "")
    )
    be = b"\x00\x00\x00\x00" + struct.pack(">I", 3) + b"hi\x00"
    assert codec.decode("std_msgs/String", be) == {"data": "hi"}


def test_time_and_header_layout(msg_registry: MsgRegistry):
    codec = CdrCodec(msg_registry)
    data = codec.encode("std_msgs/Header", {"stamp": {"sec": 1, "nanosec": 2}, "frame_id": "f"})
    # sec(int32) nanosec(uint32) then string len=2 + "f\0"
    assert data == b"\x00\x01\x00\x00" + struct.pack("<iI", 1, 2) + struct.pack("<I", 2) + b"f\x00"


def test_alignment_padding_after_odd_string(msg_registry: MsgRegistry):
    """JointState: header.frame_id of odd length forces 8-byte padding before float64 arrays."""
    codec = CdrCodec(msg_registry)
    msg = {
        "header": {"stamp": {"sec": 0, "nanosec": 0}, "frame_id": "abc"},
        "name": ["j1"],
        "position": [1.5],
        "velocity": [],
        "effort": [],
    }
    data = codec.encode("sensor_msgs/JointState", msg)
    body = data[4:]
    # stamp 8 bytes | frame_id: len(4) + "abc\0"(4) = 16 | name: count(4) + len(4) + "j1\0"(3) = 27
    # position: count at offset 28 (aligned 4) -> 32, then float64 aligned to 8 -> starts at 32
    assert body[16:20] == struct.pack("<I", 1)  # name count
    assert body[27] == 0  # padding byte before position count
    assert body[28:32] == struct.pack("<I", 1)  # position count
    assert body[32:40] == struct.pack("<d", 1.5)
    assert codec.decode("sensor_msgs/JointState", data) == msg


@pytest.mark.parametrize(
    "type_name,msg",
    [
        (
            "rtdaas_msgs/ToolPose",
            {
                "header": {"stamp": {"sec": 123, "nanosec": 456}, "frame_id": "dv_001/PSM1"},
                "arm": "PSM1",
                "position": {"x": 0.01, "y": -0.02, "z": -0.12},
                "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
                "jaw_angle": 0.3,
                "speed": 0.05,
            },
        ),
        (
            "rtdaas_msgs/SystemEvent",
            {
                "header": {"stamp": {"sec": 9, "nanosec": 0}, "frame_id": ""},
                "arm": "",
                "severity": 2,
                "code": "JOINT_LIMIT_WARN",
                "message": "x",
            },
        ),
        (
            "rtdaas_msgs/Heartbeat",
            {
                "header": {"stamp": {"sec": 9, "nanosec": 1}, "frame_id": ""},
                "robot_id": "dv_002",
                "uptime_s": 12.5,
                "state": "teleop",
                "active_arms": ["PSM1", "PSM2", "ECM"],
                "software_version": "sim-1.0",
            },
        ),
        (
            "sensor_msgs/JointState",
            {
                "header": {"stamp": {"sec": 5, "nanosec": 5}, "frame_id": "dv_001/ECM"},
                "name": ["a", "bb", "ccc"],
                "position": [0.1, 0.2, 0.3],
                "velocity": [1.0, 2.0, 3.0],
                "effort": [-0.1, 0.0, 0.1],
            },
        ),
    ],
)
def test_round_trip(msg_registry: MsgRegistry, type_name: str, msg: dict):
    codec = CdrCodec(msg_registry)
    data = codec.encode(type_name, msg)
    assert data[:4] == b"\x00\x01\x00\x00"
    assert codec.decode(type_name, data) == msg


def test_nan_and_defaults(msg_registry: MsgRegistry):
    codec = CdrCodec(msg_registry)
    msg = codec.empty_message("rtdaas_msgs/ToolPose")
    assert msg["orientation"]["w"] == 1.0  # .msg default honoured
    msg["jaw_angle"] = float("nan")
    out = codec.decode("rtdaas_msgs/ToolPose", codec.encode("rtdaas_msgs/ToolPose", msg))
    assert math.isnan(out["jaw_angle"])
    assert out["orientation"] == {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}


def test_fixed_and_bounded_arrays():
    reg = MsgRegistry()
    reg.add(parse_msg("p", "Arr", "int16[3] fixed\nuint8[<=2] bounded\n"))
    codec = CdrCodec(reg)
    data = codec.encode("p/Arr", {"fixed": [1, 2, 3], "bounded": [7]})
    assert data[4:] == struct.pack("<hhh", 1, 2, 3) + b"\x00\x00" + struct.pack("<I", 1) + b"\x07"
    assert codec.decode("p/Arr", data) == {"fixed": [1, 2, 3], "bounded": [7]}
    with pytest.raises(CdrError):
        codec.encode("p/Arr", {"fixed": [1, 2], "bounded": []})
    with pytest.raises(CdrError):
        codec.encode("p/Arr", {"fixed": [1, 2, 3], "bounded": [1, 2, 3]})


def test_truncated_payload_raises(msg_registry: MsgRegistry):
    codec = CdrCodec(msg_registry)
    data = codec.encode("builtin_interfaces/Time", {"sec": 1, "nanosec": 2})
    with pytest.raises(CdrError):
        codec.decode("builtin_interfaces/Time", data[:-1])
    with pytest.raises(CdrError):
        codec.decode("builtin_interfaces/Time", b"\x00\x02\x00\x00" + data[4:])
