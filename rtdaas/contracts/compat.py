"""Avro schema compatibility checking (spec §2.4).

Implements the Avro *schema resolution* rules as a static check, the same way the
Confluent Schema Registry does:

* ``BACKWARD``: a consumer using the **new** schema (reader) can read data written
  with the **old** schema (writer). This is the mode every subject uses.
* ``FORWARD``: the old schema can read data written with the new schema.
* ``FULL``: both.

Rules (reader vs writer):

* primitives must match, or be a legal promotion of the writer type:
  ``int -> long/float/double``, ``long -> float/double``, ``float -> double``,
  ``string <-> bytes``
* records: names must match (aliases honoured); every reader field must exist in
  the writer (by name or reader alias) or carry a default
* arrays / maps recurse on items / values
* enums: names match and writer symbols are a subset of reader symbols, or the
  reader enum declares a ``default``
* fixed: names match and sizes are equal
* unions: every writer branch must resolve against some reader branch; a non-union
  writer must match some branch of a union reader
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rtdaas.contracts.avro_binary import NAMED_TYPES, PRIMITIVE_TYPES, _fullname

PROMOTIONS: dict[str, frozenset[str]] = {
    "int": frozenset({"int", "long", "float", "double"}),
    "long": frozenset({"long", "float", "double"}),
    "float": frozenset({"float", "double"}),
    "double": frozenset({"double"}),
    "string": frozenset({"string", "bytes"}),
    "bytes": frozenset({"bytes", "string"}),
    "null": frozenset({"null"}),
    "boolean": frozenset({"boolean"}),
}

MODES = ("BACKWARD", "FORWARD", "FULL", "NONE")


@dataclass(frozen=True)
class Incompatibility:
    path: str
    kind: str
    detail: str

    def __str__(self) -> str:
        return f"{self.path}: {self.kind} — {self.detail}"


class _Names:
    """Named-type table for one schema document."""

    def __init__(self, schema: Any) -> None:
        self.table: dict[str, dict[str, Any]] = {}
        self._collect(schema, None)

    def _collect(self, node: Any, ns: str | None) -> None:
        if isinstance(node, list):
            for b in node:
                self._collect(b, ns)
        elif isinstance(node, dict):
            t = node["type"]
            if isinstance(t, (dict, list)):
                self._collect(t, ns)
            elif t in NAMED_TYPES:
                full = _fullname(node, ns)
                self.table[full] = node
                inner_ns = full.rsplit(".", 1)[0] if "." in full else None
                if t == "record":
                    for f in node["fields"]:
                        self._collect(f["type"], inner_ns)
            elif t == "array":
                self._collect(node["items"], ns)
            elif t == "map":
                self._collect(node["values"], ns)

    def resolve(self, node: Any, ns: str | None) -> Any:
        """Replace a name reference by its definition; returns other nodes unchanged."""
        if isinstance(node, str) and node not in PRIMITIVE_TYPES:
            if node in self.table:
                return self.table[node]
            full = f"{ns}.{node}" if ns else node
            if full in self.table:
                return self.table[full]
            raise ValueError(f"unknown named type {node!r}")
        if isinstance(node, dict) and isinstance(node["type"], (dict, list)):
            return node["type"]
        if isinstance(node, dict) and isinstance(node["type"], str) and node["type"] in self.table:
            return self.table[node["type"]]
        return node


def _kind(node: Any) -> str:
    if isinstance(node, list):
        return "union"
    if isinstance(node, str):
        return node
    return node["type"]


def _names_of(node: dict[str, Any], ns: str | None) -> set[str]:
    full = _fullname(node, ns)
    out = {full, node["name"]}
    for alias in node.get("aliases", []):
        out.add(alias)
        if "." not in alias and "." in full:
            out.add(full.rsplit(".", 1)[0] + "." + alias)
    return out


def _check(
    reader: Any,
    writer: Any,
    rnames: _Names,
    wnames: _Names,
    rns: str | None,
    wns: str | None,
    path: str,
    out: list[Incompatibility],
) -> None:
    reader = rnames.resolve(reader, rns)
    writer = wnames.resolve(writer, wns)
    rk, wk = _kind(reader), _kind(writer)

    if rk == "union":
        if wk == "union":
            for i, wb in enumerate(writer):
                if not _matches_any_branch(reader, wb, rnames, wnames, rns, wns):
                    out.append(
                        Incompatibility(
                            f"{path}<{i}>",
                            "missing_union_branch",
                            f"writer branch {_kind(wnames.resolve(wb, wns))!r} has no compatible reader branch",
                        )
                    )
        elif not _matches_any_branch(reader, writer, rnames, wnames, rns, wns):
            out.append(
                Incompatibility(
                    path,
                    "missing_union_branch",
                    f"writer type {wk!r} has no compatible branch in reader union",
                )
            )
        return
    if wk == "union":
        for i, wb in enumerate(writer):
            _check(reader, wb, rnames, wnames, rns, wns, f"{path}<{i}>", out)
        return

    if rk in PRIMITIVE_TYPES or wk in PRIMITIVE_TYPES:
        if rk not in PROMOTIONS.get(wk, frozenset()):
            out.append(
                Incompatibility(
                    path, "type_mismatch", f"reader type {rk!r} cannot read writer type {wk!r}"
                )
            )
        return

    if rk != wk:
        out.append(
            Incompatibility(
                path, "type_mismatch", f"reader type {rk!r} cannot read writer type {wk!r}"
            )
        )
        return

    if rk == "array":
        _check(reader["items"], writer["items"], rnames, wnames, rns, wns, f"{path}[]", out)
        return
    if rk == "map":
        _check(reader["values"], writer["values"], rnames, wnames, rns, wns, f"{path}{{}}", out)
        return

    # named types from here on
    rfull, wfull = _fullname(reader, rns), _fullname(writer, wns)
    if not (_names_of(reader, rns) & _names_of(writer, wns)):
        out.append(
            Incompatibility(
                path,
                "name_mismatch",
                f"reader {rk} {rfull!r} vs writer {wfull!r} (no alias matches)",
            )
        )
        return
    inner_rns = rfull.rsplit(".", 1)[0] if "." in rfull else rns
    inner_wns = wfull.rsplit(".", 1)[0] if "." in wfull else wns

    if rk == "fixed":
        if reader["size"] != writer["size"]:
            out.append(
                Incompatibility(
                    path, "fixed_size_mismatch", f"{reader['size']} != {writer['size']}"
                )
            )
        return
    if rk == "enum":
        missing = [s for s in writer["symbols"] if s not in reader["symbols"]]
        if missing and "default" not in reader:
            out.append(
                Incompatibility(
                    path,
                    "missing_enum_symbols",
                    f"reader lacks writer symbols {missing} and has no default",
                )
            )
        return
    if rk == "record":
        wfields = {f["name"]: f for f in writer["fields"]}
        for rf in reader["fields"]:
            wf = wfields.get(rf["name"])
            if wf is None:
                for alias in rf.get("aliases", []):
                    if alias in wfields:
                        wf = wfields[alias]
                        break
            fpath = f"{path}.{rf['name']}"
            if wf is None:
                if "default" not in rf:
                    out.append(
                        Incompatibility(
                            fpath,
                            "reader_field_missing_default",
                            "field added without a default; old data cannot be read",
                        )
                    )
                continue
            _check(rf["type"], wf["type"], rnames, wnames, inner_rns, inner_wns, fpath, out)
        return
    out.append(Incompatibility(path, "unsupported", f"cannot compare {rk!r}"))


def _matches_any_branch(
    reader_union: list[Any],
    writer: Any,
    rnames: _Names,
    wnames: _Names,
    rns: str | None,
    wns: str | None,
) -> bool:
    for rb in reader_union:
        probe: list[Incompatibility] = []
        _check(rb, writer, rnames, wnames, rns, wns, "", probe)
        if not probe:
            return True
    return False


def check_backward(reader: dict[str, Any], writer: dict[str, Any]) -> list[Incompatibility]:
    """Incompatibilities preventing ``reader`` (new) from reading data written with ``writer`` (old)."""
    out: list[Incompatibility] = []
    _check(reader, writer, _Names(reader), _Names(writer), None, None, "$", out)
    return out


def check_forward(reader: dict[str, Any], writer: dict[str, Any]) -> list[Incompatibility]:
    """Forward check with the same argument convention as :func:`check_backward`.

    ``reader`` is the **new** schema and ``writer`` the **old** one; forward
    compatibility means the old schema can read data produced with the new one,
    so the roles are swapped internally.
    """
    return check_backward(writer, reader)


def check(
    new: dict[str, Any], old: dict[str, Any], mode: str = "BACKWARD"
) -> list[Incompatibility]:
    mode = mode.upper()
    if mode not in MODES:
        raise ValueError(f"unknown compatibility mode {mode!r}; expected one of {MODES}")
    if mode == "NONE":
        return []
    out: list[Incompatibility] = []
    if mode in ("BACKWARD", "FULL"):
        out.extend(check_backward(new, old))
    if mode in ("FORWARD", "FULL"):
        out.extend(
            Incompatibility(f"forward:{i.path}", i.kind, i.detail) for i in check_forward(new, old)
        )
    return out


def is_compatible(new: dict[str, Any], old: dict[str, Any], mode: str = "BACKWARD") -> bool:
    return not check(new, old, mode)


def flatten_fields(schema: Any, prefix: str = "") -> dict[str, str]:
    """``{"msg.header.stamp.sec": "int", ...}`` — used by the ``diff`` CLI command."""
    names = _Names(schema)
    out: dict[str, str] = {}

    def walk(node: Any, ns: str | None, path: str) -> None:
        node = names.resolve(node, ns)
        k = _kind(node)
        if k == "record":
            full = _fullname(node, ns)
            inner = full.rsplit(".", 1)[0] if "." in full else ns
            for f in node["fields"]:
                walk(f["type"], inner, f"{path}.{f['name']}" if path else f["name"])
        elif k == "array":
            walk(node["items"], ns, f"{path}[]")
        elif k == "map":
            walk(node["values"], ns, f"{path}{{}}")
        elif k == "union":
            out[path] = "union<" + ",".join(_kind(names.resolve(b, ns)) for b in node) + ">"
        elif k == "enum":
            out[path] = "enum(" + ",".join(node["symbols"]) + ")"
        elif k == "fixed":
            out[path] = f"fixed({node['size']})"
        else:
            out[path] = k

    walk(schema, None, prefix)
    return out
