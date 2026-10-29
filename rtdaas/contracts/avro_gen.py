"""Generate Avro schemas from ROS 2 message definitions (spec §2.3).

Mapping::

    bool                                  -> boolean
    int8/int16/int32/uint8/uint16/byte/char -> int
    int64/uint32/uint64                   -> long
    float32                               -> float
    float64                               -> double
    string/wstring                        -> string
    T[] / T[N] / T[<=N]                   -> {"type": "array", "items": T}
    pkg/Type                              -> nested record, defined once per schema
                                             document and referenced by full name after

Constants are not fields; they are listed in the record ``doc``. Field defaults
from the ``.msg`` become Avro ``default`` values. Field order is preserved.

The Kafka *record* schema wraps a message schema in the envelope used on the wire::

    {"type": "record", "name": "<Type>Record", "namespace": "rtdaas.kafka",
     "fields": [robot_id, ros_topic, recv_time_ns, msg]}
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from rtdaas.contracts.msgspec import MsgDef, MsgRegistry

PRIMITIVE_TO_AVRO: dict[str, str] = {
    "bool": "boolean",
    "byte": "int",
    "char": "int",
    "int8": "int",
    "uint8": "int",
    "int16": "int",
    "uint16": "int",
    "int32": "int",
    "uint32": "long",
    "int64": "long",
    "uint64": "long",
    "float32": "float",
    "float64": "double",
    "string": "string",
    "wstring": "string",
}

KAFKA_NAMESPACE = "rtdaas.kafka"


def avro_full_name(msg_full_name: str) -> str:
    """``sensor_msgs/JointState`` -> ``sensor_msgs.JointState``."""
    package, name = msg_full_name.split("/", 1)
    return f"{package}.{name}"


def _record_doc(msg: MsgDef) -> str:
    doc = f"Generated from ROS 2 message {msg.full_name}."
    if msg.constants:
        consts = ", ".join(f"{c.name}={c.value!r}" for c in msg.constants)
        doc += f" Constants: {consts}."
    return doc


def _field_type(ftype: str, registry: MsgRegistry, defined: set[str]) -> Any:
    if ftype in PRIMITIVE_TO_AVRO:
        return PRIMITIVE_TO_AVRO[ftype]
    full = avro_full_name(ftype)
    if full in defined:
        return full
    return _record(registry.get(ftype), registry, defined)


def _record(msg: MsgDef, registry: MsgRegistry, defined: set[str]) -> dict[str, Any]:
    defined.add(avro_full_name(msg.full_name))
    fields: list[dict[str, Any]] = []
    for f in msg.fields:
        item_type = _field_type(f.type, registry, defined)
        avro_type: Any = {"type": "array", "items": item_type} if f.is_array else item_type
        entry: dict[str, Any] = {"name": f.name, "type": avro_type}
        if f.doc:
            entry["doc"] = f.doc
        if f.default is not None:
            entry["default"] = f.default
        fields.append(entry)
    return {
        "type": "record",
        "name": msg.name,
        "namespace": msg.avro_namespace,
        "doc": _record_doc(msg),
        "fields": fields,
    }


def msg_to_avro(msg: MsgDef, registry: MsgRegistry) -> dict[str, Any]:
    """Avro record schema for one message type, with nested types defined inline once."""
    return _record(msg, registry, set())


def kafka_record_schema(msg_schema: dict[str, Any], topic: str) -> dict[str, Any]:
    """Envelope schema for a Kafka topic value (spec §2.3)."""
    return {
        "type": "record",
        "name": f"{msg_schema['name']}Record",
        "namespace": KAFKA_NAMESPACE,
        "doc": f"Kafka value for topic {topic}: bridge envelope around {msg_schema.get('namespace')}.{msg_schema['name']}.",
        "fields": [
            {"name": "robot_id", "type": "string", "doc": "Fleet-wide robot identifier, e.g. dv-001."},
            {"name": "ros_topic", "type": "string", "doc": "Fully-qualified ROS 2 topic the message came from."},
            {"name": "recv_time_ns", "type": "long", "doc": "bridge receive time, UNIX ns"},
            {"name": "msg", "type": msg_schema},
        ],
    }


def dump_schema(schema: dict[str, Any]) -> str:
    return json.dumps(schema, indent=2, ensure_ascii=False) + "\n"


def load_topics(registry_yml: str | Path) -> dict[str, str]:
    """Return ``{kafka_topic: msg_full_name}`` from ``contracts/registry.yml``."""
    import yaml

    with open(registry_yml, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return {topic: spec["type"] for topic, spec in data["topics"].items()}


def generate_all(registry: MsgRegistry, topics: dict[str, str], out_dir: str | Path) -> list[Path]:
    """Write every message schema and every Kafka envelope schema under ``out_dir``."""
    out_dir = Path(out_dir)
    (out_dir / "kafka").mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for msg in registry:  # sorted by full name
        path = out_dir / f"{msg.package}.{msg.name}.avsc"
        path.write_text(dump_schema(msg_to_avro(msg, registry)), encoding="utf-8")
        written.append(path)
    for topic in sorted(topics):
        msg = registry.get(topics[topic])
        path = out_dir / "kafka" / f"{topic}.avsc"
        path.write_text(dump_schema(kafka_record_schema(msg_to_avro(msg, registry), topic)), encoding="utf-8")
        written.append(path)
    return written


def render_all(registry: MsgRegistry, topics: dict[str, str]) -> dict[str, str]:
    """Same content as :func:`generate_all` but returned as ``{relative_path: text}``."""
    out: dict[str, str] = {}
    for msg in registry:
        out[f"{msg.package}.{msg.name}.avsc"] = dump_schema(msg_to_avro(msg, registry))
    for topic in sorted(topics):
        msg = registry.get(topics[topic])
        out[f"kafka/{topic}.avsc"] = dump_schema(kafka_record_schema(msg_to_avro(msg, registry), topic))
    return out
