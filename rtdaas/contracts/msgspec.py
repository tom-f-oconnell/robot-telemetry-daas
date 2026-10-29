"""ROS 2 ``.msg`` definition parser.

The ``.msg`` files under ``contracts/msgs/<pkg>/msg/<Type>.msg`` are the source of
truth for every data contract in the platform. This module parses them into a
small typed model (:class:`MsgDef`, :class:`Field`, :class:`Constant`) that the
Avro generator, the CDR codec and the MCAP recorder all consume.

Grammar supported (the subset ROS 2 actually uses):

* ``# comment`` lines; comments directly above a field become its ``doc``
* constants ``TYPE NAME=VALUE`` (``NAME`` is upper-case)
* fields ``TYPE name [default]`` with optional trailing ``# comment``
* arrays ``T[]`` (unbounded), ``T[N]`` (fixed), ``T[<=N]`` (bounded)
* bounded strings ``string<=N``
* nested types ``pkg/Type``; bare ``Type`` resolves within the same package,
  except bare ``Header`` which resolves to ``std_msgs/Header`` (ROS convention).
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

PRIMITIVES: frozenset[str] = frozenset(
    {
        "bool",
        "byte",
        "char",
        "int8",
        "uint8",
        "int16",
        "uint16",
        "int32",
        "uint32",
        "int64",
        "uint64",
        "float32",
        "float64",
        "string",
        "wstring",
    }
)

INTEGER_TYPES: frozenset[str] = frozenset(
    {"byte", "char", "int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64"}
)
FLOAT_TYPES: frozenset[str] = frozenset({"float32", "float64"})
STRING_TYPES: frozenset[str] = frozenset({"string", "wstring"})

SEPARATOR = "=" * 80

_CONSTANT_RE = re.compile(
    r"^(?P<type>[A-Za-z_][\w/]*)\s+(?P<name>[A-Z_][A-Z0-9_]*)\s*=\s*(?P<value>.+?)\s*$"
)
_FIELD_RE = re.compile(
    r"^(?P<base>[A-Za-z_][\w/]*)"
    r"(?:<=(?P<strbound>\d+))?"
    r"(?P<array>\[(?P<bounded><=)?(?P<len>\d*)\])?"
    r"\s+(?P<name>[a-z][a-z0-9_]*)"
    r"(?:\s+(?P<default>.+?))?\s*$"
)


class MsgParseError(ValueError):
    """Raised for malformed .msg content."""


@dataclass
class Field:
    name: str
    type: str  # primitive name or fully-qualified "pkg/Type"
    is_array: bool = False
    array_len: int | None = None  # fixed length N for T[N], upper bound for T[<=N]
    bounded: bool = False  # True for T[<=N]
    default: Any | None = None
    doc: str | None = None
    string_bound: int | None = None  # for string<=N

    @property
    def is_primitive(self) -> bool:
        return self.type in PRIMITIVES


@dataclass
class Constant:
    name: str
    type: str
    value: Any


@dataclass
class MsgDef:
    package: str
    name: str
    fields: list[Field] = field(default_factory=list)
    constants: list[Constant] = field(default_factory=list)
    raw: str = ""

    @property
    def full_name(self) -> str:
        return f"{self.package}/{self.name}"

    @property
    def avro_namespace(self) -> str:
        return self.package

    def field(self, name: str) -> Field:
        for f in self.fields:
            if f.name == name:
                return f
        raise KeyError(name)


def _strip_inline_comment(text: str) -> tuple[str, str | None]:
    """Split ``value # comment`` respecting quoted strings."""
    in_quote: str | None = None
    for i, ch in enumerate(text):
        if in_quote:
            if ch == in_quote:
                in_quote = None
        elif ch in ("'", '"'):
            in_quote = ch
        elif ch == "#":
            return text[:i].rstrip(), text[i + 1 :].strip()
    return text.rstrip(), None


def _parse_scalar(type_name: str, text: str) -> Any:
    text = text.strip()
    if type_name == "bool":
        if text.lower() in ("true", "1"):
            return True
        if text.lower() in ("false", "0"):
            return False
        raise MsgParseError(f"bad bool literal {text!r}")
    if type_name in INTEGER_TYPES:
        return int(text, 0)
    if type_name in FLOAT_TYPES:
        return float(text)
    if type_name in STRING_TYPES:
        if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
            return text[1:-1]
        return text
    raise MsgParseError(f"default values are only supported for primitives, got {type_name}")


def _parse_value(type_name: str, text: str, is_array: bool) -> Any:
    text = text.strip()
    if is_array:
        if not (text.startswith("[") and text.endswith("]")):
            raise MsgParseError(f"array default must be bracketed: {text!r}")
        inner = text[1:-1].strip()
        if not inner:
            return []
        parts = [p.strip() for p in re.split(r",(?=(?:[^'\"]*['\"][^'\"]*['\"])*[^'\"]*$)", inner)]
        return [_parse_scalar(type_name, p) for p in parts]
    return _parse_scalar(type_name, text)


def resolve_type(base: str, package: str) -> str:
    """Resolve a type token to a primitive name or a ``pkg/Type`` full name."""
    if base in PRIMITIVES:
        return base
    if "/" in base:
        return base
    if base == "Header":
        return "std_msgs/Header"
    return f"{package}/{base}"


def parse_msg(package: str, name: str, text: str) -> MsgDef:
    """Parse the text of one ``.msg`` file."""
    msg = MsgDef(package=package, name=name, raw=text if text.endswith("\n") else text + "\n")
    pending_doc: list[str] = []
    for lineno, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            pending_doc = []
            continue
        if line.startswith("#"):
            pending_doc.append(line[1:].strip())
            continue
        body, inline_comment = _strip_inline_comment(line)
        m = _CONSTANT_RE.match(body)
        if m and m.group("type") in PRIMITIVES:
            ctype = m.group("type")
            msg.constants.append(
                Constant(
                    name=m.group("name"), type=ctype, value=_parse_scalar(ctype, m.group("value"))
                )
            )
            pending_doc = []
            continue
        m = _FIELD_RE.match(body)
        if not m:
            raise MsgParseError(f"{package}/{name}.msg:{lineno}: cannot parse {raw_line!r}")
        base = m.group("base")
        ftype = resolve_type(base, package)
        is_array = m.group("array") is not None
        array_len: int | None = None
        bounded = False
        if is_array:
            if m.group("len"):
                array_len = int(m.group("len"))
                bounded = m.group("bounded") is not None
        string_bound = int(m.group("strbound")) if m.group("strbound") else None
        if string_bound is not None and ftype not in STRING_TYPES:
            raise MsgParseError(f"{package}/{name}.msg:{lineno}: <= bound only valid on strings")
        default_text = m.group("default")
        default = _parse_value(ftype, default_text, is_array) if default_text else None
        doc_parts = list(pending_doc)
        if inline_comment:
            doc_parts.append(inline_comment)
        msg.fields.append(
            Field(
                name=m.group("name"),
                type=ftype,
                is_array=is_array,
                array_len=array_len,
                bounded=bounded,
                default=default,
                doc=" ".join(doc_parts) if doc_parts else None,
                string_bound=string_bound,
            )
        )
        pending_doc = []
    return msg


class MsgRegistry:
    """All message definitions found under a ``contracts/msgs``-style directory."""

    def __init__(self, msgs: dict[str, MsgDef] | None = None) -> None:
        self._msgs: dict[str, MsgDef] = dict(msgs or {})

    @classmethod
    def load_dir(cls, root: str | Path) -> MsgRegistry:
        root = Path(root)
        if not root.is_dir():
            raise FileNotFoundError(f"message directory not found: {root}")
        msgs: dict[str, MsgDef] = {}
        for path in sorted(root.glob("*/msg/*.msg")):
            package = path.parent.parent.name
            msg = parse_msg(package, path.stem, path.read_text(encoding="utf-8"))
            msgs[msg.full_name] = msg
        registry = cls(msgs)
        registry.validate()
        return registry

    def add(self, msg: MsgDef) -> None:
        self._msgs[msg.full_name] = msg

    def get(self, full_name: str) -> MsgDef:
        try:
            return self._msgs[full_name]
        except KeyError as exc:
            raise KeyError(f"unknown message type {full_name!r}") from exc

    def __contains__(self, full_name: str) -> bool:
        return full_name in self._msgs

    def __iter__(self) -> Iterator[MsgDef]:
        for name in sorted(self._msgs):
            yield self._msgs[name]

    def __len__(self) -> int:
        return len(self._msgs)

    def names(self) -> list[str]:
        return sorted(self._msgs)

    def validate(self) -> None:
        for msg in self._msgs.values():
            for f in msg.fields:
                if not f.is_primitive and f.type not in self._msgs:
                    raise MsgParseError(f"{msg.full_name}.{f.name}: unknown type {f.type!r}")

    def dependencies(self, full_name: str) -> list[str]:
        """Transitive dependencies in deterministic depth-first order of first use."""
        out: list[str] = []
        seen: set[str] = {full_name}

        def visit(name: str) -> None:
            for f in self.get(name).fields:
                if f.is_primitive or f.type in seen:
                    continue
                seen.add(f.type)
                out.append(f.type)
                visit(f.type)

        visit(full_name)
        return out

    def ros2msg_schema_text(self, full_name: str) -> str:
        """Schema text in the ``ros2msg`` encoding used by rosbag2's MCAP storage plugin."""
        parts = [self.get(full_name).raw]
        for dep in self.dependencies(full_name):
            parts.append(f"{SEPARATOR}\nMSG: {dep}\n{self.get(dep).raw}")
        return "".join(parts)

    @classmethod
    def from_ros2msg_schema(cls, full_name: str, text: str) -> MsgRegistry:
        """Inverse of :meth:`ros2msg_schema_text`.

        Parses an MCAP ``ros2msg`` schema blob (top-level definition first, then each
        dependency introduced by the 80-``=`` separator and a ``MSG: pkg/Type`` line)
        into a registry holding the top-level type and all its dependencies. This lets
        a bag be decoded with the exact message definitions it was recorded with.
        """
        package, name = full_name.split("/", 1)
        blocks = text.split(SEPARATOR)
        msgs: dict[str, MsgDef] = {}
        top = parse_msg(package, name, blocks[0].strip("\n") + "\n")
        msgs[top.full_name] = top
        for block in blocks[1:]:
            lines = block.lstrip("\n").split("\n", 1)
            header = lines[0].strip()
            if not header.startswith("MSG:"):
                raise MsgParseError(f"expected 'MSG: pkg/Type' after separator, got {header!r}")
            dep_full = header[len("MSG:") :].strip()
            dep_pkg, dep_name = dep_full.split("/", 1)
            body = lines[1] if len(lines) > 1 else ""
            dep = parse_msg(dep_pkg, dep_name, body.strip("\n") + "\n")
            msgs[dep.full_name] = dep
        registry = cls(msgs)
        registry.validate()
        return registry

    def merge(self, other: MsgRegistry, prefer_self: bool = True) -> MsgRegistry:
        """Return a new registry with the union of both; on conflict ``self`` wins by default."""
        merged: dict[str, MsgDef] = {}
        if prefer_self:
            merged.update(other._msgs)
            merged.update(self._msgs)
        else:
            merged.update(self._msgs)
            merged.update(other._msgs)
        result = MsgRegistry(merged)
        result.validate()
        return result
