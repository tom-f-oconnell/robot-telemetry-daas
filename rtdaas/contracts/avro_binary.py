"""Avro binary encoding/decoding.

A dependency-free implementation of the Avro 1.11 binary encoding that is used by
the simulator, the registry tooling and the tests. When ``fastavro`` is importable
the public :func:`encode` / :func:`decode` delegate to it (it is much faster);
:func:`encode_pure` / :func:`decode_pure` always use the Python implementation so
that both paths can be tested against each other.

Supported types: null, boolean, int, long, float, double, bytes, string, record,
enum, array, map, union, fixed, and references to previously-defined named types.
Arrays and maps are written as a single block followed by the ``0`` terminator;
negative block counts (count followed by byte size) are accepted on decode.
"""

from __future__ import annotations

import io
import json
import struct
from collections.abc import Callable
from typing import Any

try:  # pragma: no cover - exercised only when fastavro is installed
    import fastavro as _fastavro
except Exception:  # pragma: no cover
    _fastavro = None

PRIMITIVE_TYPES = frozenset(
    {"null", "boolean", "int", "long", "float", "double", "bytes", "string"}
)
NAMED_TYPES = frozenset({"record", "enum", "fixed"})

INT_MIN, INT_MAX = -(2**31), 2**31 - 1
LONG_MIN, LONG_MAX = -(2**63), 2**63 - 1


class AvroError(ValueError):
    """Schema or datum problem."""


# --------------------------------------------------------------------------- schema handling


def _fullname(schema: dict[str, Any], enclosing_ns: str | None) -> str:
    name = schema["name"]
    if "." in name:
        return name
    ns = schema.get("namespace", enclosing_ns)
    return f"{ns}.{name}" if ns else name


def _collect_names(schema: Any, names: dict[str, dict[str, Any]], enclosing_ns: str | None) -> None:
    if isinstance(schema, list):
        for branch in schema:
            _collect_names(branch, names, enclosing_ns)
        return
    if isinstance(schema, str):
        return
    if not isinstance(schema, dict):
        raise AvroError(f"invalid schema node: {schema!r}")
    t = schema["type"]
    if isinstance(t, (dict, list)):
        _collect_names(t, names, enclosing_ns)
        return
    if t in NAMED_TYPES:
        full = _fullname(schema, enclosing_ns)
        names[full] = schema
        ns = full.rsplit(".", 1)[0] if "." in full else None
        if t == "record":
            for f in schema["fields"]:
                _collect_names(f["type"], names, ns)
    elif t == "array":
        _collect_names(schema["items"], names, enclosing_ns)
    elif t == "map":
        _collect_names(schema["values"], names, enclosing_ns)


def parse_schema(schema: dict[str, Any] | list[Any] | str) -> dict[str, Any] | list[Any] | str:
    """Validate a schema and expand references to named types.

    Returns a deep copy of ``schema`` in which every reference to a previously
    defined named type is replaced by the definition itself, so the result can be
    walked without a names table. Recursive types are rejected.
    """
    names: dict[str, dict[str, Any]] = {}
    _collect_names(schema, names, None)

    def expand(node: Any, enclosing_ns: str | None, stack: tuple[str, ...]) -> Any:
        if isinstance(node, list):
            return [expand(b, enclosing_ns, stack) for b in node]
        if isinstance(node, str):
            if node in PRIMITIVE_TYPES:
                return node
            full = (
                node
                if "." in node or node in names
                else (f"{enclosing_ns}.{node}" if enclosing_ns else node)
            )
            if full not in names:
                raise AvroError(f"unknown type reference {node!r}")
            if full in stack:
                raise AvroError(f"recursive schema not supported: {full}")
            return expand(names[full], enclosing_ns, stack)
        if not isinstance(node, dict):
            raise AvroError(f"invalid schema node: {node!r}")
        t = node["type"]
        if isinstance(t, (dict, list)):
            return expand(t, enclosing_ns, stack)
        out = dict(node)
        if t in NAMED_TYPES:
            full = _fullname(node, enclosing_ns)
            out["name"] = full
            out.pop("namespace", None)
            ns = full.rsplit(".", 1)[0] if "." in full else None
            if t == "record":
                out["fields"] = [
                    {**f, "type": expand(f["type"], ns, stack + (full,))} for f in node["fields"]
                ]
            elif t == "enum":
                if "symbols" not in node:
                    raise AvroError(f"enum {full} has no symbols")
            elif t == "fixed":
                if "size" not in node:
                    raise AvroError(f"fixed {full} has no size")
            return out
        if t == "array":
            out["items"] = expand(node["items"], enclosing_ns, stack)
            return out
        if t == "map":
            out["values"] = expand(node["values"], enclosing_ns, stack)
            return out
        if t in PRIMITIVE_TYPES:
            return t
        if t in names:
            return expand(t, enclosing_ns, stack)
        raise AvroError(f"unknown type {t!r}")

    return expand(schema, None, ())


_parsed_cache: dict[str, Any] = {}


def _parsed(schema: Any) -> Any:
    key = json.dumps(schema, sort_keys=True, separators=(",", ":"))
    hit = _parsed_cache.get(key)
    if hit is None:
        hit = parse_schema(schema)
        if len(_parsed_cache) > 256:
            _parsed_cache.clear()
        _parsed_cache[key] = hit
    return hit


def _type_of(node: Any) -> str:
    if isinstance(node, list):
        return "union"
    if isinstance(node, str):
        return node
    return node["type"]


# --------------------------------------------------------------------------- low-level writers


def write_long(out: io.BytesIO, value: int) -> None:
    if not LONG_MIN <= value <= LONG_MAX:
        raise AvroError(f"long out of range: {value}")
    n = (value << 1) ^ (value >> 63)  # zigzag (Python ints are unbounded; >> is arithmetic)
    n &= (1 << 64) - 1
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.write(bytes((byte | 0x80,)))
        else:
            out.write(bytes((byte,)))
            return


def write_int(out: io.BytesIO, value: int) -> None:
    if not INT_MIN <= value <= INT_MAX:
        raise AvroError(f"int out of range: {value}")
    write_long(out, value)


def read_long(inp: io.BytesIO) -> int:
    shift = 0
    n = 0
    while True:
        b = inp.read(1)
        if not b:
            raise AvroError("unexpected end of data while reading varint")
        byte = b[0]
        n |= (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
        if shift > 70:
            raise AvroError("varint too long")
    return (n >> 1) ^ -(n & 1)


def write_bytes(out: io.BytesIO, value: bytes) -> None:
    write_long(out, len(value))
    out.write(value)


def read_bytes(inp: io.BytesIO) -> bytes:
    n = read_long(inp)
    if n < 0:
        raise AvroError("negative length")
    data = inp.read(n)
    if len(data) != n:
        raise AvroError("unexpected end of data while reading bytes")
    return data


# --------------------------------------------------------------------------- encode


def _union_index(branches: list[Any], datum: Any) -> int:
    """Pick the union branch for a datum (Python-type driven, Avro-style)."""
    kinds = [_type_of(b) for b in branches]

    def first(*wanted: str) -> int | None:
        for w in wanted:
            if w in kinds:
                return kinds.index(w)
        return None

    idx: int | None
    if datum is None:
        idx = first("null")
    elif isinstance(datum, bool):
        idx = first("boolean")
    elif isinstance(datum, int):
        idx = (
            first("int", "long", "float", "double")
            if INT_MIN <= datum <= INT_MAX
            else first("long", "double", "float")
        )
    elif isinstance(datum, float):
        idx = first("double", "float")
    elif isinstance(datum, str):
        idx = first("string", "enum", "bytes")
    elif isinstance(datum, (bytes, bytearray)):
        idx = first("bytes", "fixed", "string")
    elif isinstance(datum, (list, tuple)):
        idx = first("array")
    elif isinstance(datum, dict):
        idx = None
        for i, b in enumerate(branches):
            if kinds[i] == "record" and all(
                f["name"] in datum or "default" in f for f in b["fields"]
            ):
                idx = i
                break
        if idx is None:
            idx = first("map")
    else:
        idx = None
    if idx is None:
        raise AvroError(f"no union branch for datum {datum!r} among {kinds}")
    return idx


def _encode(out: io.BytesIO, node: Any, datum: Any, path: str) -> None:
    t = _type_of(node)
    try:
        if t == "null":
            if datum is not None:
                raise AvroError(f"{path}: expected None")
        elif t == "boolean":
            if not isinstance(datum, bool):
                raise AvroError(f"{path}: expected bool, got {type(datum).__name__}")
            out.write(b"\x01" if datum else b"\x00")
        elif t == "int":
            if isinstance(datum, bool) or not isinstance(datum, int):
                raise AvroError(f"{path}: expected int, got {type(datum).__name__}")
            write_int(out, datum)
        elif t == "long":
            if isinstance(datum, bool) or not isinstance(datum, int):
                raise AvroError(f"{path}: expected int, got {type(datum).__name__}")
            write_long(out, datum)
        elif t == "float":
            out.write(struct.pack("<f", float(datum)))
        elif t == "double":
            out.write(struct.pack("<d", float(datum)))
        elif t == "bytes":
            write_bytes(out, bytes(datum))
        elif t == "string":
            if not isinstance(datum, str):
                raise AvroError(f"{path}: expected str, got {type(datum).__name__}")
            write_bytes(out, datum.encode("utf-8"))
        elif t == "fixed":
            data = bytes(datum)
            if len(data) != node["size"]:
                raise AvroError(f"{path}: fixed size mismatch")
            out.write(data)
        elif t == "enum":
            write_long(out, node["symbols"].index(datum))
        elif t == "array":
            items = list(datum)
            if items:
                write_long(out, len(items))
                for i, item in enumerate(items):
                    _encode(out, node["items"], item, f"{path}[{i}]")
            write_long(out, 0)
        elif t == "map":
            if datum:
                write_long(out, len(datum))
                for k, v in datum.items():
                    write_bytes(out, str(k).encode("utf-8"))
                    _encode(out, node["values"], v, f"{path}[{k!r}]")
            write_long(out, 0)
        elif t == "union":
            idx = _union_index(node, datum)
            write_long(out, idx)
            _encode(out, node[idx], datum, f"{path}<{idx}>")
        elif t == "record":
            if not isinstance(datum, dict):
                raise AvroError(
                    f"{path}: expected dict for record {node['name']}, got {type(datum).__name__}"
                )
            for f in node["fields"]:
                name = f["name"]
                if name in datum:
                    value = datum[name]
                elif "default" in f:
                    value = f["default"]
                else:
                    raise AvroError(f"{path}.{name}: missing field with no default")
                _encode(out, f["type"], value, f"{path}.{name}")
        else:
            raise AvroError(f"{path}: unsupported type {t!r}")
    except (ValueError, TypeError, struct.error) as exc:
        if isinstance(exc, AvroError):
            raise
        raise AvroError(f"{path}: {exc}") from exc


def encode_pure(schema: Any, datum: Any) -> bytes:
    out = io.BytesIO()
    _encode(out, _parsed(schema), datum, "$")
    return out.getvalue()


# --------------------------------------------------------------------------- decode


def _decode(inp: io.BytesIO, node: Any) -> Any:
    t = _type_of(node)
    if t == "null":
        return None
    if t == "boolean":
        b = inp.read(1)
        if not b:
            raise AvroError("unexpected end of data")
        return b[0] != 0
    if t in ("int", "long"):
        return read_long(inp)
    if t == "float":
        return struct.unpack("<f", _read_exact(inp, 4))[0]
    if t == "double":
        return struct.unpack("<d", _read_exact(inp, 8))[0]
    if t == "bytes":
        return read_bytes(inp)
    if t == "string":
        return read_bytes(inp).decode("utf-8")
    if t == "fixed":
        return _read_exact(inp, node["size"])
    if t == "enum":
        return node["symbols"][read_long(inp)]
    if t == "array":
        items: list[Any] = []
        while True:
            count = read_long(inp)
            if count == 0:
                return items
            if count < 0:
                count = -count
                read_long(inp)  # byte size of the block, unused
            for _ in range(count):
                items.append(_decode(inp, node["items"]))
    if t == "map":
        result: dict[str, Any] = {}
        while True:
            count = read_long(inp)
            if count == 0:
                return result
            if count < 0:
                count = -count
                read_long(inp)
            for _ in range(count):
                key = read_bytes(inp).decode("utf-8")
                result[key] = _decode(inp, node["values"])
    if t == "union":
        idx = read_long(inp)
        if not 0 <= idx < len(node):
            raise AvroError(f"union index {idx} out of range")
        return _decode(inp, node[idx])
    if t == "record":
        return {f["name"]: _decode(inp, f["type"]) for f in node["fields"]}
    raise AvroError(f"unsupported type {t!r}")


def _read_exact(inp: io.BytesIO, n: int) -> bytes:
    data = inp.read(n)
    if len(data) != n:
        raise AvroError("unexpected end of data")
    return data


def decode_pure(schema: Any, data: bytes) -> Any:
    inp = io.BytesIO(data)
    value = _decode(inp, _parsed(schema))
    return value


# --------------------------------------------------------------------------- public API


def encode(schema: Any, datum: Any) -> bytes:
    """Avro binary encoding of ``datum`` under ``schema`` (fastavro when available)."""
    if _fastavro is not None:  # pragma: no cover
        try:
            out = io.BytesIO()
            _fastavro.schemaless_writer(out, _fastavro.parse_schema(schema), datum)
            return out.getvalue()
        except Exception as exc:  # fall back so error messages are consistent
            raise AvroError(str(exc)) from exc
    return encode_pure(schema, datum)


def decode(schema: Any, data: bytes) -> Any:
    if _fastavro is not None:  # pragma: no cover
        try:
            return _fastavro.schemaless_reader(io.BytesIO(data), _fastavro.parse_schema(schema))
        except Exception as exc:
            raise AvroError(str(exc)) from exc
    return decode_pure(schema, data)


def validate(schema: Any, datum: Any) -> None:
    """Raise :class:`AvroError` if ``datum`` does not conform to ``schema``."""
    encode_pure(schema, datum)


Encoder = Callable[[Any, Any], bytes]
Decoder = Callable[[Any, bytes], Any]
