from __future__ import annotations

import math
import struct

import pytest

from rtdaas.contracts import avro_binary as ab
from rtdaas.contracts.avro_gen import kafka_record_schema, msg_to_avro
from rtdaas.contracts.msgspec import MsgRegistry
from rtdaas.sim.robot import TOPIC_TYPES, RobotSim, SimConfig


@pytest.mark.parametrize(
    "value,expected",
    [
        (0, b"\x00"),
        (-1, b"\x01"),
        (1, b"\x02"),
        (-2, b"\x03"),
        (2, b"\x04"),
        (63, b"\x7e"),
        (64, b"\x80\x01"),
        (-64, b"\x7f"),
        (-65, b"\x81\x01"),
        (2**31 - 1, b"\xfe\xff\xff\xff\x0f"),
        (2**63 - 1, b"\xfe\xff\xff\xff\xff\xff\xff\xff\xff\x01"),
        (-(2**63), b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01"),
    ],
)
def test_long_zigzag_golden(value: int, expected: bytes):
    assert ab.encode_pure("long", value) == expected
    assert ab.decode_pure("long", expected) == value


def test_int_range_enforced():
    with pytest.raises(ab.AvroError):
        ab.encode_pure("int", 2**31)
    with pytest.raises(ab.AvroError):
        ab.encode_pure("long", 2**63)


def test_primitive_golden_vectors():
    assert ab.encode_pure("string", "foo") == b"\x06foo"
    assert ab.encode_pure("bytes", b"\x01\x02") == b"\x04\x01\x02"
    assert ab.encode_pure("boolean", True) == b"\x01"
    assert ab.encode_pure("boolean", False) == b"\x00"
    assert ab.encode_pure("null", None) == b""
    assert ab.encode_pure("float", 1.5) == struct.pack("<f", 1.5)
    assert ab.encode_pure("double", -2.25) == struct.pack("<d", -2.25)
    assert ab.decode_pure("string", b"\x06foo") == "foo"
    assert ab.decode_pure("double", struct.pack("<d", 7.0)) == 7.0


def test_array_and_map_blocks():
    assert ab.encode_pure({"type": "array", "items": "long"}, [3, 27]) == b"\x04\x06\x36\x00"
    assert ab.encode_pure({"type": "array", "items": "long"}, []) == b"\x00"
    assert ab.decode_pure({"type": "array", "items": "long"}, b"\x04\x06\x36\x00") == [3, 27]
    # negative block count with byte size is accepted on decode
    assert ab.decode_pure({"type": "array", "items": "long"}, b"\x03\x04\x06\x36\x00") == [3, 27]
    enc = ab.encode_pure({"type": "map", "values": "int"}, {"a": 1})
    assert enc == b"\x02\x02a\x02\x00"
    assert ab.decode_pure({"type": "map", "values": "int"}, enc) == {"a": 1}


def test_union_enum_fixed_record_round_trip():
    schema = {
        "type": "record",
        "name": "R",
        "namespace": "t",
        "fields": [
            {"name": "u", "type": ["null", "string", "long"]},
            {"name": "e", "type": {"type": "enum", "name": "Color", "symbols": ["RED", "GREEN"]}},
            {"name": "f", "type": {"type": "fixed", "name": "Hash", "size": 2}},
            {
                "name": "nested",
                "type": {
                    "type": "record",
                    "name": "N",
                    "fields": [{"name": "x", "type": "double"}],
                },
            },
            {"name": "again", "type": "N"},
            {"name": "d", "type": "int", "default": 7},
        ],
    }
    datum = {"u": "hi", "e": "GREEN", "f": b"\xab\xcd", "nested": {"x": 1.0}, "again": {"x": 2.0}}
    enc = ab.encode_pure(schema, datum)
    # union index 1 (string) then "hi"
    assert enc.startswith(b"\x02\x04hi")
    out = ab.decode_pure(schema, enc)
    assert out == {**datum, "d": 7}
    assert ab.decode_pure(schema, ab.encode_pure(schema, {**datum, "u": None}))["u"] is None
    assert ab.decode_pure(schema, ab.encode_pure(schema, {**datum, "u": 5}))["u"] == 5


def test_record_missing_field_without_default_fails():
    schema = {"type": "record", "name": "R", "fields": [{"name": "x", "type": "int"}]}
    with pytest.raises(ab.AvroError):
        ab.encode_pure(schema, {})
    with pytest.raises(ab.AvroError):
        ab.encode_pure(schema, {"x": "no"})


def test_parse_schema_rejects_unknown_and_recursive():
    with pytest.raises(ab.AvroError):
        ab.parse_schema(
            {"type": "record", "name": "R", "fields": [{"name": "x", "type": "Missing"}]}
        )
    with pytest.raises(ab.AvroError):
        ab.parse_schema({"type": "record", "name": "R", "fields": [{"name": "x", "type": "R"}]})


def test_public_encode_decode_match_pure_path():
    schema = {
        "type": "record",
        "name": "P",
        "fields": [
            {"name": "a", "type": {"type": "array", "items": "double"}},
            {"name": "s", "type": "string"},
        ],
    }
    datum = {"a": [1.0, 2.5, -3.0], "s": "héllo"}
    assert ab.encode(schema, datum) == ab.encode_pure(schema, datum)
    assert ab.decode(schema, ab.encode(schema, datum)) == datum


def test_nan_survives_round_trip():
    out = ab.decode_pure("double", ab.encode_pure("double", float("nan")))
    assert math.isnan(out)


def test_every_simulator_message_validates_against_its_schema(msg_registry: MsgRegistry):
    sim = RobotSim(SimConfig(robot_id="dv-001", rate_hz=20.0, seed=3))
    schemas = {
        topic: msg_to_avro(msg_registry.get(t), msg_registry) for topic, t in TOPIC_TYPES.items()
    }
    envelopes = {topic: kafka_record_schema(s, topic) for topic, s in schemas.items()}
    seen: set[str] = set()
    t0 = 1_700_000_000 * 10**9
    for k in range(40):
        for kafka_topic, ros_topic, msg in sim.tick(t0 + k * 50_000_000):
            seen.add(kafka_topic)
            ab.validate(schemas[kafka_topic], msg)
            env = {"robot_id": "dv-001", "ros_topic": ros_topic, "recv_time_ns": t0, "msg": msg}
            assert (
                ab.decode_pure(envelopes[kafka_topic], ab.encode_pure(envelopes[kafka_topic], env))[
                    "msg"
                ]["header"]
                == msg["header"]
            )
    for kafka_topic, _ros_topic, msg in sim.end(t0 + 41 * 50_000_000):
        ab.validate(schemas[kafka_topic], msg)
    assert seen == set(TOPIC_TYPES)
