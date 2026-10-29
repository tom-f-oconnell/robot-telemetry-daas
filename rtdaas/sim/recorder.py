"""Record simulated telemetry into a rosbag2-style MCAP file.

The output mimics what ``ros2 bag record -s mcap`` produces: profile ``ros2``, one
Schema per message type (encoding ``ros2msg``, data = concatenated ``.msg``
definitions), one Channel per ROS topic (``message_encoding = "cdr"``) and CDR
payloads — so the very same batch job that ingests real bags ingests simulated ones.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from rtdaas.contracts.cdr import CdrCodec
from rtdaas.contracts.msgspec import MsgRegistry
from rtdaas.mcap_lite.writer import McapWriter


class McapRecorder:
    def __init__(self, out_path: str | Path, registry: MsgRegistry, topics: dict[str, str] | None = None) -> None:
        """``topics`` maps Kafka topic -> message type; its schemas are registered up front."""
        self.path = Path(out_path)
        self.registry = registry
        self.codec = CdrCodec(registry)
        self._writer = McapWriter(self.path, profile="ros2", library="rtdaas.sim.recorder 0.1")
        self._schema_ids: dict[str, int] = {}
        self._channel_ids: dict[str, int] = {}
        self.message_count = 0
        for type_name in sorted(set((topics or {}).values())):
            self._schema_id(type_name)

    def _schema_id(self, type_name: str) -> int:
        sid = self._schema_ids.get(type_name)
        if sid is None:
            sid = self._writer.add_schema(type_name, "ros2msg", self.registry.ros2msg_schema_text(type_name))
            self._schema_ids[type_name] = sid
        return sid

    def _channel_id(self, ros_topic: str, type_name: str) -> int:
        cid = self._channel_ids.get(ros_topic)
        if cid is None:
            cid = self._writer.add_channel(
                ros_topic,
                "cdr",
                self._schema_id(type_name),
                metadata={"offered_qos_profiles": "", "topic_type_hash": ""},
            )
            self._channel_ids[ros_topic] = cid
        return cid

    def record(self, ros_topic: str, type_name: str, msg: dict[str, Any], log_time_ns: int) -> None:
        cid = self._channel_id(ros_topic, type_name)
        self._writer.write_message(cid, log_time_ns, log_time_ns, self.codec.encode(type_name, msg))
        self.message_count += 1

    def close(self) -> None:
        self._writer.close()

    def __enter__(self) -> McapRecorder:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
