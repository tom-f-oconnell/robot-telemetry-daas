"""Decode a ROS 2 MCAP bag into flat rows for the raw telemetry tables (spec §3.2).

Pure Python, no Spark import: this is the function :mod:`rtdaas.spark.batch_mcap` runs
inside ``mapPartitions`` and what unit tests exercise directly.

Each channel is decoded with the message definition **embedded in the bag** (MCAP schema
with ``encoding == "ros2msg"``) so bags recorded with older message versions still decode;
the repo registry (``codec`` argument) is only a fallback for schemas that are missing or
not ``ros2msg``. Columns that an older definition lacks come out as ``None``.
"""

from __future__ import annotations

import datetime as dt
import logging
import math
from collections.abc import Iterable
from typing import Any

from rtdaas.contracts.cdr import CdrCodec
from rtdaas.contracts.msgspec import MsgRegistry
from rtdaas.mcap_lite import read_messages

log = logging.getLogger(__name__)

TYPE_TO_TABLE: dict[str, str] = {
    "sensor_msgs/JointState": "joint_states",
    "rtdaas_msgs/ToolPose": "tool_pose",
    "rtdaas_msgs/SystemEvent": "events",
    "rtdaas_msgs/Heartbeat": "heartbeat",
}

SEVERITY_NAMES = {0: "DEBUG", 1: "INFO", 2: "WARN", 3: "ERROR", 4: "FATAL"}

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)


def normalize_type_name(schema_name: str) -> str:
    """``sensor_msgs/msg/JointState`` (rosbag2 naming) -> ``sensor_msgs/JointState``."""
    parts = schema_name.split("/")
    if len(parts) == 3 and parts[1] == "msg":
        return f"{parts[0]}/{parts[2]}"
    return schema_name


def ns_to_datetime(ns: int | None) -> dt.datetime | None:
    if ns is None:
        return None
    return _EPOCH + dt.timedelta(microseconds=int(ns) // 1000)


def stamp_to_datetime(stamp: dict | None) -> dt.datetime | None:
    """``builtin_interfaces/Time`` dict -> aware UTC datetime (microsecond precision)."""
    if not stamp:
        return None
    sec = int(stamp.get("sec", 0) or 0)
    nanosec = int(stamp.get("nanosec", 0) or 0)
    return _EPOCH + dt.timedelta(seconds=sec, microseconds=nanosec // 1000)


def _get(d: Any, *path: str, default: Any = None) -> Any:
    for key in path:
        if not isinstance(d, dict) or key not in d:
            return default
        d = d[key]
    return d


def _float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def _floats(v: Any) -> list[float] | None:
    if v is None:
        return None
    return [float(x) for x in v]


def topic_parts(ros_topic: str) -> list[str]:
    return [p for p in ros_topic.split("/") if p]


def _robot_from_topic(ros_topic: str) -> str | None:
    parts = topic_parts(ros_topic)
    return parts[0] if parts else None


def _arm_from_topic(ros_topic: str) -> str | None:
    """Arm segment(s) between robot id and the leaf, e.g. ``PSM1`` or ``PSM1/jaw``."""
    parts = topic_parts(ros_topic)
    if len(parts) < 3:
        return None
    return "/".join(parts[1:-1])


# ---------------------------------------------------------------------------
# Per-type row builders
# ---------------------------------------------------------------------------


def joint_state_row(msg: dict, ros_topic: str, log_time_ns: int, bag_file: str) -> dict:
    return {
        "robot_id": _robot_from_topic(ros_topic),
        "arm": _arm_from_topic(ros_topic),
        "ts": stamp_to_datetime(_get(msg, "header", "stamp")),
        "recv_ts": ns_to_datetime(log_time_ns),
        "frame_id": _get(msg, "header", "frame_id"),
        "joint_names": list(msg.get("name")) if msg.get("name") is not None else None,
        "position": _floats(msg.get("position")),
        "velocity": _floats(msg.get("velocity")),
        "effort": _floats(msg.get("effort")),
        "source": "bag",
        "bag_file": bag_file,
    }


def tool_pose_row(msg: dict, ros_topic: str, log_time_ns: int, bag_file: str) -> dict:
    arm = msg.get("arm") or _arm_from_topic(ros_topic)
    return {
        "robot_id": _robot_from_topic(ros_topic),
        "arm": arm,
        "ts": stamp_to_datetime(_get(msg, "header", "stamp")),
        "recv_ts": ns_to_datetime(log_time_ns),
        "x": _float(_get(msg, "position", "x")),
        "y": _float(_get(msg, "position", "y")),
        "z": _float(_get(msg, "position", "z")),
        "qx": _float(_get(msg, "orientation", "x")),
        "qy": _float(_get(msg, "orientation", "y")),
        "qz": _float(_get(msg, "orientation", "z")),
        "qw": _float(_get(msg, "orientation", "w")),
        "jaw_angle": _float(msg.get("jaw_angle")),
        "speed": _float(msg.get("speed")),
        "source": "bag",
        "bag_file": bag_file,
    }


def event_row(msg: dict, ros_topic: str, log_time_ns: int, bag_file: str) -> dict:
    severity = msg.get("severity")
    severity = int(severity) if severity is not None else None
    return {
        "robot_id": _robot_from_topic(ros_topic),
        "arm": msg.get("arm", "") if msg.get("arm") is not None else "",
        "ts": stamp_to_datetime(_get(msg, "header", "stamp")),
        "recv_ts": ns_to_datetime(log_time_ns),
        "severity": severity,
        "severity_name": SEVERITY_NAMES.get(severity) if severity is not None else None,
        "code": msg.get("code"),
        "message": msg.get("message"),
        "source": "bag",
        "bag_file": bag_file,
    }


def heartbeat_row(msg: dict, ros_topic: str, log_time_ns: int, bag_file: str) -> dict:
    return {
        "robot_id": msg.get("robot_id") or _robot_from_topic(ros_topic),
        "ts": stamp_to_datetime(_get(msg, "header", "stamp")),
        "recv_ts": ns_to_datetime(log_time_ns),
        "uptime_s": _float(msg.get("uptime_s")),
        "state": msg.get("state"),
        "active_arms": list(msg.get("active_arms")) if msg.get("active_arms") is not None else None,
        "software_version": msg.get("software_version"),
        "source": "bag",
        "bag_file": bag_file,
    }


ROW_BUILDERS = {
    "joint_states": joint_state_row,
    "tool_pose": tool_pose_row,
    "events": event_row,
    "heartbeat": heartbeat_row,
}


# ---------------------------------------------------------------------------
# Bag decoding
# ---------------------------------------------------------------------------


class _CodecCache:
    """One :class:`CdrCodec` per MCAP schema id, built from the embedded definition."""

    def __init__(self, fallback: CdrCodec) -> None:
        self.fallback = fallback
        self._by_schema: dict[int, tuple[CdrCodec, str] | None] = {}

    def for_schema(self, schema, channel) -> tuple[CdrCodec, str] | None:
        key = schema.id if schema is not None else -1
        if key in self._by_schema:
            return self._by_schema[key]
        result = self._resolve(schema, channel)
        self._by_schema[key] = result
        return result

    def _resolve(self, schema, channel) -> tuple[CdrCodec, str] | None:
        if schema is None:
            log.warning("channel %s has no schema; skipping", channel.topic)
            return None
        type_name = normalize_type_name(schema.name)
        if type_name not in TYPE_TO_TABLE:
            return None  # not a telemetry type we ingest
        from_text = getattr(MsgRegistry, "from_ros2msg_schema", None)
        if schema.encoding == "ros2msg" and schema.data and from_text is not None:
            try:
                text = (
                    schema.data.decode("utf-8")
                    if isinstance(schema.data, (bytes, bytearray))
                    else str(schema.data)
                )
                registry = from_text(type_name, text)
                # Types the embedded definition does not carry resolve against the repo registry.
                merge = getattr(registry, "merge", None)
                if merge is not None:
                    registry = merge(self.fallback.registry, prefer_self=True)
                return CdrCodec(registry), type_name
            except Exception as exc:  # malformed embedded definition -> fall back to repo registry
                log.warning(
                    "embedded ros2msg schema for %s unusable (%s); using repo registry",
                    type_name,
                    exc,
                )
        elif schema.encoding != "ros2msg":
            log.warning(
                "schema %s has encoding %r (expected ros2msg); using repo registry",
                type_name,
                schema.encoding,
            )
        if type_name in self.fallback.registry:
            return self.fallback, type_name
        log.warning("no definition available for %s; skipping channel %s", type_name, channel.topic)
        return None


def iter_rows(path_or_bytes, codec: CdrCodec, bag_file: str) -> Iterable[tuple[str, dict]]:
    """Yield ``(table_short_name, row_dict)`` for every decodable message in the bag."""
    cache = _CodecCache(codec)
    n_bad = 0
    for schema, channel, message in read_messages(path_or_bytes):
        if channel.message_encoding not in ("cdr", ""):
            continue
        resolved = cache.for_schema(schema, channel)
        if resolved is None:
            continue
        chan_codec, type_name = resolved
        try:
            msg = chan_codec.decode(type_name, message.data)
        except Exception as exc:
            n_bad += 1
            if n_bad <= 5:
                log.warning("failed to decode %s on %s: %s", type_name, channel.topic, exc)
            continue
        table = TYPE_TO_TABLE[type_name]
        yield table, ROW_BUILDERS[table](msg, channel.topic, message.log_time_ns, bag_file)
    if n_bad:
        log.warning("%s: %d undecodable messages skipped", bag_file, n_bad)


def decode_bag(path_or_bytes, codec: CdrCodec, bag_file: str) -> dict[str, list[dict]]:
    """Decode a whole bag into ``{"tool_pose": [...], "joint_states": [...], "events": [...], "heartbeat": [...]}``."""
    out: dict[str, list[dict]] = {t: [] for t in TYPE_TO_TABLE.values()}
    for table, row in iter_rows(path_or_bytes, codec, bag_file):
        out[table].append(row)
    log.info("%s: decoded %s", bag_file, {k: len(v) for k, v in out.items()})
    return out
