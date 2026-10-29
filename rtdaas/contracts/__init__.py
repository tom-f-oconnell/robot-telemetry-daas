"""Data contracts: ROS 2 message definitions -> Avro schemas, codecs and registry tooling."""

from rtdaas.contracts.avro_gen import generate_all, kafka_record_schema, load_topics, msg_to_avro
from rtdaas.contracts.cdr import CdrCodec, CdrError
from rtdaas.contracts.compat import (
    Incompatibility,
    check,
    check_backward,
    check_forward,
    is_compatible,
)
from rtdaas.contracts.msgspec import PRIMITIVES, Constant, Field, MsgDef, MsgRegistry, parse_msg
from rtdaas.contracts.registry import RegistryClient, SchemaCache, subject_for_topic
from rtdaas.contracts.wire import decode_wire, encode_wire, split_wire

__all__ = [
    "PRIMITIVES",
    "CdrCodec",
    "CdrError",
    "Constant",
    "Field",
    "Incompatibility",
    "MsgDef",
    "MsgRegistry",
    "RegistryClient",
    "SchemaCache",
    "check",
    "check_backward",
    "check_forward",
    "decode_wire",
    "encode_wire",
    "generate_all",
    "is_compatible",
    "kafka_record_schema",
    "load_topics",
    "msg_to_avro",
    "parse_msg",
    "split_wire",
    "subject_for_topic",
]
