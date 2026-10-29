from __future__ import annotations

import copy

import pytest

from rtdaas.contracts import avro_gen, compat
from rtdaas.contracts.msgspec import MsgRegistry


def _record(fields: list[dict], name: str = "R", ns: str = "t") -> dict:
    return {"type": "record", "name": name, "namespace": ns, "fields": fields}


BASE = _record([{"name": "a", "type": "int"}, {"name": "b", "type": "string"}])


def kinds(problems: list[compat.Incompatibility]) -> list[tuple[str, str]]:
    return [(p.path, p.kind) for p in problems]


def test_identical_is_compatible_in_every_mode():
    for mode in compat.MODES:
        assert compat.check(BASE, BASE, mode) == []


def test_add_field_with_default_is_backward_compatible():
    new = _record(BASE["fields"] + [{"name": "c", "type": "double", "default": 0.0}])
    assert compat.check_backward(new, BASE) == []
    assert compat.is_compatible(new, BASE, "BACKWARD")
    # forward: old reader lacks c -> fine (ignored); FULL also passes
    assert compat.check(new, BASE, "FULL") == []


def test_add_field_without_default_breaks_backward():
    new = _record(BASE["fields"] + [{"name": "c", "type": "double"}])
    problems = compat.check_backward(new, BASE)
    assert kinds(problems) == [("$.c", "reader_field_missing_default")]
    # but it is forward compatible (old reader ignores the new field)
    assert compat.check_forward(new, BASE) == []


def test_remove_field_backward_ok_forward_breaks():
    new = _record([{"name": "a", "type": "int"}])
    assert compat.check_backward(new, BASE) == []
    fwd = compat.check(new, BASE, "FORWARD")
    assert kinds(fwd) == [("forward:$.b", "reader_field_missing_default")]


def test_type_promotions():
    widen = _record([{"name": "a", "type": "long"}, {"name": "b", "type": "string"}])
    assert compat.check_backward(widen, BASE) == []
    narrow = _record([{"name": "a", "type": "int"}, {"name": "b", "type": "string"}])
    assert kinds(compat.check_backward(narrow, widen)) == [("$.a", "type_mismatch")]
    dbl = _record([{"name": "x", "type": "double"}])
    flt = _record([{"name": "x", "type": "float"}])
    assert compat.check_backward(dbl, flt) == []
    assert kinds(compat.check_backward(flt, dbl)) == [("$.x", "type_mismatch")]
    assert (
        compat.check_backward(
            _record([{"name": "b", "type": "bytes"}]), _record([{"name": "b", "type": "string"}])
        )
        == []
    )


def test_record_rename_breaks_unless_aliased():
    renamed = _record(BASE["fields"], name="Other")
    assert kinds(compat.check_backward(renamed, BASE)) == [("$", "name_mismatch")]
    aliased = {**renamed, "aliases": ["R"]}
    assert compat.check_backward(aliased, BASE) == []


def test_field_rename_breaks_unless_aliased():
    renamed = _record([{"name": "a", "type": "int"}, {"name": "bb", "type": "string"}])
    assert kinds(compat.check_backward(renamed, BASE)) == [("$.bb", "reader_field_missing_default")]
    aliased = _record(
        [{"name": "a", "type": "int"}, {"name": "bb", "type": "string", "aliases": ["b"]}]
    )
    assert compat.check_backward(aliased, BASE) == []


def test_nested_record_and_array_changes_are_reported_with_paths():
    inner_old = {
        "type": "record",
        "name": "P",
        "fields": [{"name": "x", "type": "double"}, {"name": "y", "type": "double"}],
    }
    inner_new = {
        "type": "record",
        "name": "P",
        "fields": [{"name": "x", "type": "float"}, {"name": "y", "type": "double"}],
    }
    old = _record([{"name": "pts", "type": {"type": "array", "items": inner_old}}])
    new = _record([{"name": "pts", "type": {"type": "array", "items": inner_new}}])
    assert kinds(compat.check_backward(new, old)) == [("$.pts[].x", "type_mismatch")]


def test_union_rules():
    old = _record([{"name": "u", "type": ["null", "int"]}])
    wider = _record([{"name": "u", "type": ["null", "int", "string"]}])
    assert compat.check_backward(wider, old) == []
    narrower = _record([{"name": "u", "type": ["null"]}])
    assert kinds(compat.check_backward(narrower, old)) == [("$.u<1>", "missing_union_branch")]
    non_union_reader = _record([{"name": "u", "type": "int"}])
    assert kinds(compat.check_backward(non_union_reader, old)) == [("$.u<0>", "type_mismatch")]


def test_enum_and_fixed_rules():
    old = _record([{"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["A", "B"]}}])
    fewer = _record([{"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["A"]}}])
    assert kinds(compat.check_backward(fewer, old)) == [("$.e", "missing_enum_symbols")]
    fewer_default = _record(
        [{"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["A"], "default": "A"}}]
    )
    assert compat.check_backward(fewer_default, old) == []
    f2 = _record([{"name": "f", "type": {"type": "fixed", "name": "F", "size": 2}}])
    f4 = _record([{"name": "f", "type": {"type": "fixed", "name": "F", "size": 4}}])
    assert kinds(compat.check_backward(f4, f2)) == [("$.f", "fixed_size_mismatch")]


def test_unknown_mode_rejected():
    with pytest.raises(ValueError):
        compat.check(BASE, BASE, "SIDEWAYS")


def test_real_envelope_evolution(msg_registry: MsgRegistry):
    """Adding `speed 0` to ToolPose must pass, narrowing jaw_angle must fail, with the nested path."""
    old = avro_gen.kafka_record_schema(
        avro_gen.msg_to_avro(msg_registry.get("rtdaas_msgs/ToolPose"), msg_registry), "t"
    )
    new = copy.deepcopy(old)
    msg_fields = new["fields"][3]["type"]["fields"]
    msg_fields.append({"name": "tool_id", "type": "string", "default": ""})
    assert compat.check(new, old, "BACKWARD") == []
    broken = copy.deepcopy(old)
    for f in broken["fields"][3]["type"]["fields"]:
        if f["name"] == "jaw_angle":
            f["type"] = "float"
    assert kinds(compat.check(broken, old, "BACKWARD")) == [("$.msg.jaw_angle", "type_mismatch")]


def test_flatten_fields(msg_registry: MsgRegistry):
    schema = avro_gen.kafka_record_schema(
        avro_gen.msg_to_avro(msg_registry.get("rtdaas_msgs/ToolPose"), msg_registry), "t"
    )
    flat = compat.flatten_fields(schema)
    assert flat["msg.header.stamp.sec"] == "int"
    assert flat["msg.header.stamp.nanosec"] == "long"
    assert flat["msg.position.x"] == "double"
    assert flat["recv_time_ns"] == "long"
