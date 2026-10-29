"""Decode rosbag2-style MCAP bags into raw-table rows (pure Python, spec §3.2 / §5.2).

Each bag is decoded with the message definitions **embedded in the bag** (the
``ros2msg`` schema that ``ros2 bag record`` writes), so bags recorded with older
message versions still decode; the repository's ``contracts/msgs`` registry is only a
fallback for schemas a bag does not carry. Columns the embedded definition lacks come
out as ``None``.

The ingest modules (``rtdaas.mcap_lite``, ``rtdaas.contracts``) are imported lazily so
this module can be imported, and the rest of the local pipeline used, without them.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from rtdaas.config import DEFAULT_MSGS_DIR

log = logging.getLogger(__name__)

NS = 1_000_000_000
SEVERITY_NAMES = {0: "DEBUG", 1: "INFO", 2: "WARN", 3: "ERROR", 4: "FATAL"}

#: message type -> raw table short name
TYPE_TABLES: dict[str, str] = {
    "sensor_msgs/JointState": "joint_states",
    "rtdaas_msgs/ToolPose": "tool_pose",
    "rtdaas_msgs/SystemEvent": "events",
    "rtdaas_msgs/Heartbeat": "heartbeat",
}
#: raw table short name -> Iceberg/SQLite table name
RAW_TABLES: dict[str, str] = {
    "tool_pose": "telemetry.tool_pose_raw",
    "joint_states": "telemetry.joint_states_raw",
    "events": "telemetry.events_raw",
    "heartbeat": "telemetry.heartbeat_raw",
}
RAW_COLUMNS: dict[str, list[str]] = {
    "tool_pose": [
        "robot_id",
        "arm",
        "ts",
        "recv_ts",
        "x",
        "y",
        "z",
        "qx",
        "qy",
        "qz",
        "qw",
        "jaw_angle",
        "speed",
        "source",
        "bag_file",
    ],
    "joint_states": [
        "robot_id",
        "arm",
        "ts",
        "recv_ts",
        "frame_id",
        "joint_names",
        "position",
        "velocity",
        "effort",
        "source",
        "bag_file",
    ],
    "events": [
        "robot_id",
        "arm",
        "ts",
        "recv_ts",
        "severity",
        "severity_name",
        "code",
        "message",
        "source",
        "bag_file",
    ],
    "heartbeat": [
        "robot_id",
        "ts",
        "recv_ts",
        "uptime_s",
        "state",
        "active_arms",
        "software_version",
        "source",
        "bag_file",
    ],
}
RAW_KEYS: dict[str, list[str]] = {
    "tool_pose": ["robot_id", "arm", "ts"],
    "joint_states": ["robot_id", "arm", "ts"],
    "events": ["robot_id", "arm", "ts", "code"],
    "heartbeat": ["robot_id", "ts"],
}


def ns_to_datetime(t_ns: int) -> datetime:
    return datetime.fromtimestamp(t_ns / NS, tz=UTC)


def stamp_to_datetime(stamp: dict | None) -> datetime | None:
    if not stamp:
        return None
    return ns_to_datetime(int(stamp.get("sec", 0)) * NS + int(stamp.get("nanosec", 0)))


def topic_parts(ros_topic: str) -> tuple[str | None, str | None]:
    """``/dv-001/PSM1/measured_js`` -> ``("dv-001", "PSM1")``; ``/dv-001/rtdaas/events`` -> ``("dv-001", None)``."""
    parts = [p for p in ros_topic.split("/") if p]
    robot = parts[0] if parts else None
    arm = parts[1] if len(parts) > 2 and parts[1] != "rtdaas" else None
    return robot, arm


def _get(msg: dict, *path: str, default: Any = None) -> Any:
    cur: Any = msg
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def row_from_message(
    type_name: str, ros_topic: str, msg: dict, log_time_ns: int, bag_file: str
) -> tuple[str, dict] | None:
    """Map one decoded message to ``(table_short_name, row)`` or ``None`` for unknown types."""
    table = TYPE_TABLES.get(type_name)
    if table is None:
        return None
    robot, arm = topic_parts(ros_topic)
    ts = stamp_to_datetime(_get(msg, "header", "stamp"))
    recv_ts = ns_to_datetime(log_time_ns)
    base = {"robot_id": robot, "ts": ts, "recv_ts": recv_ts, "source": "bag", "bag_file": bag_file}
    if table == "tool_pose":
        row = {
            **base,
            "arm": _get(msg, "arm") or arm,
            "x": _get(msg, "position", "x"),
            "y": _get(msg, "position", "y"),
            "z": _get(msg, "position", "z"),
            "qx": _get(msg, "orientation", "x"),
            "qy": _get(msg, "orientation", "y"),
            "qz": _get(msg, "orientation", "z"),
            "qw": _get(msg, "orientation", "w"),
            "jaw_angle": _get(msg, "jaw_angle"),
            "speed": _get(msg, "speed"),
        }
    elif table == "joint_states":
        row = {
            **base,
            "arm": arm,
            "frame_id": _get(msg, "header", "frame_id"),
            "joint_names": list(_get(msg, "name", default=[]) or []),
            "position": [float(v) for v in (_get(msg, "position", default=[]) or [])],
            "velocity": [float(v) for v in (_get(msg, "velocity", default=[]) or [])],
            "effort": [float(v) for v in (_get(msg, "effort", default=[]) or [])],
        }
    elif table == "events":
        sev = _get(msg, "severity")
        row = {
            **base,
            "arm": _get(msg, "arm", default="") if _get(msg, "arm") is not None else (arm or ""),
            "severity": int(sev) if sev is not None else None,
            "severity_name": SEVERITY_NAMES.get(int(sev)) if sev is not None else None,
            "code": _get(msg, "code"),
            "message": _get(msg, "message"),
        }
    else:  # heartbeat
        row = {
            **base,
            "robot_id": _get(msg, "robot_id") or robot,
            "uptime_s": _get(msg, "uptime_s"),
            "state": _get(msg, "state"),
            "active_arms": list(_get(msg, "active_arms", default=[]) or []),
            "software_version": _get(msg, "software_version"),
        }
    return table, {c: row.get(c) for c in RAW_COLUMNS[table]}


def decode_bag(
    path: str | Path, msgs_dir: str | Path | None = DEFAULT_MSGS_DIR
) -> dict[str, list[dict]]:
    """Decode one MCAP bag into ``{table_short_name: [row, ...]}``.

    Prefers each channel's embedded ``ros2msg`` schema; falls back to the registry loaded
    from ``msgs_dir`` when a schema is missing or uses another encoding.
    """
    from rtdaas.contracts.cdr import CdrCodec
    from rtdaas.contracts.msgspec import MsgRegistry
    from rtdaas.mcap_lite import read_messages

    path = Path(path)
    bag_file = path.name
    repo_registry: MsgRegistry | None = None

    def fallback_registry() -> MsgRegistry:
        nonlocal repo_registry
        if repo_registry is None:
            repo_registry = MsgRegistry.load_dir(msgs_dir or DEFAULT_MSGS_DIR)
        return repo_registry

    codecs: dict[int | None, CdrCodec] = {}
    rows: dict[str, list[dict]] = {t: [] for t in RAW_TABLES}
    skipped: dict[str, int] = {}
    for schema, channel, message in read_messages(path):
        type_name = schema.name if schema is not None else None
        if type_name is None or channel.message_encoding != "cdr":
            skipped[channel.topic] = skipped.get(channel.topic, 0) + 1
            continue
        key = schema.id
        codec = codecs.get(key)
        if codec is None:
            registry: MsgRegistry | None = None
            from_schema = getattr(MsgRegistry, "from_ros2msg_schema", None)
            if schema.encoding == "ros2msg" and from_schema is not None:
                try:
                    registry = from_schema(type_name, schema.data.decode("utf-8"))
                    merge = getattr(registry, "merge", None)
                    if merge is not None:
                        try:
                            registry = merge(fallback_registry())
                        except Exception:  # noqa: BLE001 - repo registry may be absent in odd layouts
                            pass
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "%s: embedded schema for %s unusable (%s); using repo registry",
                        bag_file,
                        type_name,
                        exc,
                    )
                    registry = None
            if registry is None:
                registry = fallback_registry()
            codec = CdrCodec(registry)
            codecs[key] = codec
        try:
            msg = codec.decode(type_name, message.data)
        except Exception as exc:  # noqa: BLE001 - one bad message must not kill the bag
            log.warning(
                "%s: failed to decode %s on %s: %s", bag_file, type_name, channel.topic, exc
            )
            skipped[channel.topic] = skipped.get(channel.topic, 0) + 1
            continue
        mapped = row_from_message(type_name, channel.topic, msg, message.log_time_ns, bag_file)
        if mapped is None:
            skipped[channel.topic] = skipped.get(channel.topic, 0) + 1
            continue
        table, row = mapped
        rows[table].append(row)
    if skipped:
        log.info("%s: skipped messages per topic: %s", bag_file, skipped)
    log.info("%s: decoded %s", bag_file, {t: len(r) for t, r in rows.items()})
    return rows


def find_bags(bags_dir: str | Path) -> list[Path]:
    return sorted(Path(bags_dir).rglob("*.mcap"))


def bag_frames(
    paths: Iterable[str | Path], msgs_dir: str | Path | None = DEFAULT_MSGS_DIR
) -> dict[str, pd.DataFrame]:
    """Decode several bags into one deduplicated pandas frame per raw table."""
    collected: dict[str, list[dict]] = {t: [] for t in RAW_TABLES}
    for p in paths:
        for table, rows in decode_bag(p, msgs_dir).items():
            collected[table].extend(rows)
    frames: dict[str, pd.DataFrame] = {}
    for table, rows in collected.items():
        df = pd.DataFrame(rows, columns=RAW_COLUMNS[table])
        for col in ("ts", "recv_ts"):
            df[col] = pd.to_datetime(df[col], utc=True)
        if len(df):
            df = df.drop_duplicates(subset=RAW_KEYS[table], keep="last").sort_values(
                ["robot_id", "ts"], kind="stable"
            )
        frames[table] = df.reset_index(drop=True)
    return frames
