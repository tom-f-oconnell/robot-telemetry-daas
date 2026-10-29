"""Confluent Schema Registry wire format (spec §2.2).

``value = 0x00 | int32 big-endian schema id | Avro binary``. Spark's ``from_avro``
does not understand the 5-byte prefix, which is why ``rtdaas.spark.common``
strips it with ``substring(value, 6, length(value) - 5)`` before decoding.
"""

from __future__ import annotations

import struct
from collections.abc import Callable
from typing import Any

from rtdaas.contracts import avro_binary

MAGIC = b"\x00"
HEADER_LEN = 5


class WireFormatError(ValueError):
    pass


def encode_wire(schema_id: int, schema: dict[str, Any], datum: Any) -> bytes:
    if not 0 <= schema_id < 2**31:
        raise WireFormatError(f"schema id out of range: {schema_id}")
    return MAGIC + struct.pack(">I", schema_id) + avro_binary.encode(schema, datum)


def split_wire(data: bytes) -> tuple[int, bytes]:
    """Return ``(schema_id, avro_payload)`` without decoding the payload."""
    if len(data) < HEADER_LEN or data[0:1] != MAGIC:
        raise WireFormatError("not a Confluent wire-format message (bad magic byte)")
    (schema_id,) = struct.unpack(">I", data[1:HEADER_LEN])
    return schema_id, data[HEADER_LEN:]


def decode_wire(data: bytes, schema_for_id: Callable[[int], dict[str, Any]]) -> tuple[int, Any]:
    schema_id, payload = split_wire(data)
    schema = schema_for_id(schema_id)
    return schema_id, avro_binary.decode(schema, payload)
