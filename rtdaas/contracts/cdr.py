"""CDR (XCDR1) codec for ROS 2 message payloads (spec §2.5).

``ros2 bag record`` stores every message exactly as the RMW layer serialised it:
a 4-byte encapsulation header followed by the OMG CDR encoding of the message.

* header ``00 01 00 00`` = ``CDR_LE`` (``00 00 00 00`` = ``CDR_BE``), options = 0
* alignment origin is byte 4, i.e. the first byte after the header
* primitives are aligned to ``min(size, 8)``; bool and 8-bit ints are 1 byte,
  16-bit ints 2, 32-bit ints and float32 4, 64-bit ints and float64 8
* ``string`` = ``uint32`` length *including* the NUL terminator, bytes, NUL
* unbounded / bounded sequences = ``uint32`` count followed by the elements
  (each aligned by its own rule); fixed-size arrays are just the elements
* nested messages are encoded inline, field by field

The codec is driven by :class:`~rtdaas.contracts.msgspec.MsgRegistry`, so adding a
message type never requires code changes. Golden vector used by the tests::

    std_msgs/String{data: "hello"} -> 00 01 00 00 06 00 00 00 68 65 6c 6c 6f 00
"""

from __future__ import annotations

import struct
from typing import Any

from rtdaas.contracts.msgspec import Field, MsgRegistry

ENCAPSULATION_LE = b"\x00\x01\x00\x00"
ENCAPSULATION_BE = b"\x00\x00\x00\x00"

# (struct format char, size, alignment)
_PRIMITIVE_FMT: dict[str, tuple[str, int]] = {
    "bool": ("?", 1),
    "byte": ("B", 1),
    "char": ("B", 1),
    "int8": ("b", 1),
    "uint8": ("B", 1),
    "int16": ("h", 2),
    "uint16": ("H", 2),
    "int32": ("i", 4),
    "uint32": ("I", 4),
    "int64": ("q", 8),
    "uint64": ("Q", 8),
    "float32": ("f", 4),
    "float64": ("d", 8),
}


class CdrError(ValueError):
    pass


class _Writer:
    def __init__(self, endian: str = "<") -> None:
        self.buf = bytearray()
        self.endian = endian

    def align(self, n: int) -> None:
        pad = (-len(self.buf)) % n
        if pad:
            self.buf.extend(b"\x00" * pad)

    def primitive(self, type_name: str, value: Any) -> None:
        fmt, size = _PRIMITIVE_FMT[type_name]
        self.align(size)
        try:
            self.buf.extend(struct.pack(self.endian + fmt, value))
        except struct.error as exc:
            raise CdrError(f"cannot encode {value!r} as {type_name}: {exc}") from exc

    def string(self, value: str) -> None:
        data = value.encode("utf-8")
        self.primitive("uint32", len(data) + 1)
        self.buf.extend(data)
        self.buf.append(0)

    def payload(self) -> bytes:
        return bytes(self.buf)


class _Reader:
    def __init__(self, data: bytes, endian: str = "<") -> None:
        self.data = data
        self.pos = 0
        self.endian = endian

    def align(self, n: int) -> None:
        self.pos += (-self.pos) % n

    def primitive(self, type_name: str) -> Any:
        fmt, size = _PRIMITIVE_FMT[type_name]
        self.align(size)
        end = self.pos + size
        if end > len(self.data):
            raise CdrError(f"truncated payload reading {type_name} at offset {self.pos}")
        (value,) = struct.unpack(self.endian + fmt, self.data[self.pos : end])
        self.pos = end
        return value

    def string(self) -> str:
        n = self.primitive("uint32")
        if n == 0:
            return ""
        end = self.pos + n
        if end > len(self.data):
            raise CdrError(f"truncated string at offset {self.pos}")
        raw = self.data[self.pos : end]
        self.pos = end
        if raw.endswith(b"\x00"):
            raw = raw[:-1]
        return raw.decode("utf-8")


class CdrCodec:
    """Encode / decode message dicts for any type known to the registry."""

    def __init__(self, registry: MsgRegistry) -> None:
        self.registry = registry

    # -- encode --------------------------------------------------------------------------
    def encode(self, type_name: str, msg: dict[str, Any]) -> bytes:
        w = _Writer("<")
        self._encode_struct(w, type_name, msg, type_name)
        return ENCAPSULATION_LE + w.payload()

    def _encode_struct(self, w: _Writer, type_name: str, msg: dict[str, Any], path: str) -> None:
        if not isinstance(msg, dict):
            raise CdrError(f"{path}: expected dict for {type_name}, got {type(msg).__name__}")
        for f in self.registry.get(type_name).fields:
            if f.name in msg:
                value = msg[f.name]
            elif f.default is not None:
                value = f.default
            else:
                value = self.default_value(f)
            self._encode_field(w, f, value, f"{path}.{f.name}")

    def _encode_field(self, w: _Writer, f: Field, value: Any, path: str) -> None:
        if f.is_array:
            items = list(value) if value is not None else []
            if f.array_len is not None and not f.bounded:
                if len(items) != f.array_len:
                    raise CdrError(
                        f"{path}: fixed array expects {f.array_len} items, got {len(items)}"
                    )
            else:
                if f.bounded and f.array_len is not None and len(items) > f.array_len:
                    raise CdrError(f"{path}: bounded sequence allows at most {f.array_len} items")
                w.primitive("uint32", len(items))
            for i, item in enumerate(items):
                self._encode_scalar(w, f, item, f"{path}[{i}]")
        else:
            self._encode_scalar(w, f, value, path)

    def _encode_scalar(self, w: _Writer, f: Field, value: Any, path: str) -> None:
        t = f.type
        if t in _PRIMITIVE_FMT:
            if t == "bool":
                w.primitive(t, bool(value))
            elif t in ("float32", "float64"):
                w.primitive(t, float(value))
            else:
                if isinstance(value, float) and value.is_integer():
                    value = int(value)
                w.primitive(t, int(value))
        elif t in ("string", "wstring"):
            w.string("" if value is None else str(value))
        else:
            self._encode_struct(w, t, value if value is not None else {}, path)

    # -- decode --------------------------------------------------------------------------
    def decode(self, type_name: str, data: bytes) -> dict[str, Any]:
        if len(data) < 4:
            raise CdrError("payload shorter than the encapsulation header")
        header = data[:4]
        if header[1] == 0x01:
            endian = "<"
        elif header[1] == 0x00:
            endian = ">"
        else:
            raise CdrError(f"unsupported encapsulation kind 0x{header[0]:02x}{header[1]:02x}")
        r = _Reader(data[4:], endian)
        return self._decode_struct(r, type_name)

    def _decode_struct(self, r: _Reader, type_name: str) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for f in self.registry.get(type_name).fields:
            out[f.name] = self._decode_field(r, f)
        return out

    def _decode_field(self, r: _Reader, f: Field) -> Any:
        if f.is_array:
            if f.array_len is not None and not f.bounded:
                n = f.array_len
            else:
                n = r.primitive("uint32")
            return [self._decode_scalar(r, f) for _ in range(n)]
        return self._decode_scalar(r, f)

    def _decode_scalar(self, r: _Reader, f: Field) -> Any:
        t = f.type
        if t in _PRIMITIVE_FMT:
            return r.primitive(t)
        if t in ("string", "wstring"):
            return r.string()
        return self._decode_struct(r, t)

    # -- helpers -------------------------------------------------------------------------
    def default_value(self, f: Field) -> Any:
        if f.is_array:
            if f.array_len is not None and not f.bounded:
                return [
                    self.default_value(Field(name=f.name, type=f.type)) for _ in range(f.array_len)
                ]
            return []
        if f.type == "bool":
            return False
        if f.type in _PRIMITIVE_FMT:
            return 0.0 if f.type in ("float32", "float64") else 0
        if f.type in ("string", "wstring"):
            return ""
        return {
            sub.name: (sub.default if sub.default is not None else self.default_value(sub))
            for sub in self.registry.get(f.type).fields
        }

    def empty_message(self, type_name: str) -> dict[str, Any]:
        return {
            f.name: (f.default if f.default is not None else self.default_value(f))
            for f in self.registry.get(type_name).fields
        }
