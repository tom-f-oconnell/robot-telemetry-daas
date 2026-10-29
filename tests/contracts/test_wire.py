from __future__ import annotations

import pytest

from rtdaas.contracts import wire

SCHEMA = {"type": "record", "name": "R", "fields": [{"name": "x", "type": "long"}]}


def test_wire_layout():
    data = wire.encode_wire(42, SCHEMA, {"x": 1})
    assert data[:5] == b"\x00\x00\x00\x00\x2a"
    assert data[5:] == b"\x02"
    assert wire.split_wire(data) == (42, b"\x02")
    assert wire.decode_wire(data, lambda sid: SCHEMA) == (42, {"x": 1})


def test_schema_id_is_big_endian_int32():
    data = wire.encode_wire(0x01020304, SCHEMA, {"x": 0})
    assert data[1:5] == b"\x01\x02\x03\x04"
    with pytest.raises(wire.WireFormatError):
        wire.encode_wire(2**31, SCHEMA, {"x": 0})


def test_bad_magic_rejected():
    with pytest.raises(wire.WireFormatError):
        wire.split_wire(b"\x01\x00\x00\x00\x01\x02")
    with pytest.raises(wire.WireFormatError):
        wire.split_wire(b"\x00\x00")
