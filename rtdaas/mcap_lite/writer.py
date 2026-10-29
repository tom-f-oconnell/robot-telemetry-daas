"""Minimal MCAP writer (unchunked, uncompressed, CRCs omitted).

Produces files that are valid per the MCAP v1 specification and readable by the
official ``mcap`` libraries and ``mcap info``::

    magic
    Header
    Schema* / Channel* / Message*      (data section, written as they arrive)
    DataEnd
    Schema*  Channel*  Statistics      (summary section, grouped by opcode)
    SummaryOffset*                     (one per summary group)
    Footer
    magic

CRC fields are written as ``0``, which the spec defines as "not computed".
"""

from __future__ import annotations

import io
import logging
from pathlib import Path
from typing import BinaryIO

from rtdaas.mcap_lite.records import (
    MAGIC,
    OP_CHANNEL,
    OP_SCHEMA,
    OP_STATISTICS,
    McapChannel,
    McapFooter,
    McapHeader,
    McapMessage,
    McapSchema,
    McapStatistics,
    McapSummaryOffset,
    pack_data_end,
)

log = logging.getLogger(__name__)


class McapWriter:
    def __init__(
        self,
        path: str | Path | BinaryIO,
        profile: str = "ros2",
        library: str = "rtdaas.mcap_lite 0.1",
    ) -> None:
        if isinstance(path, (str, Path)):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            self._fh: BinaryIO = open(path, "wb")
            self._owns = True
        else:
            self._fh = path
            self._owns = False
        self._pos = 0
        self._schemas: dict[int, McapSchema] = {}
        self._channels: dict[int, McapChannel] = {}
        self._schema_ids_by_key: dict[tuple[str, str, bytes], int] = {}
        self._channel_ids_by_topic: dict[str, int] = {}
        self._counts: dict[int, int] = {}
        self._sequence: dict[int, int] = {}
        self._message_count = 0
        self._start: int | None = None
        self._end: int | None = None
        self._closed = False
        self._write(MAGIC)
        self._write(McapHeader(profile=profile, library=library).pack())

    # -- plumbing ---------------------------------------------------------------------------
    def _write(self, data: bytes) -> None:
        self._fh.write(data)
        self._pos += len(data)

    @property
    def offset(self) -> int:
        return self._pos

    # -- API --------------------------------------------------------------------------------
    def add_schema(self, name: str, encoding: str, data: bytes | str) -> int:
        payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        key = (name, encoding, payload)
        if key in self._schema_ids_by_key:
            return self._schema_ids_by_key[key]
        sid = len(self._schemas) + 1
        schema = McapSchema(id=sid, name=name, encoding=encoding, data=payload)
        self._schemas[sid] = schema
        self._schema_ids_by_key[key] = sid
        self._write(schema.pack())
        return sid

    def add_channel(
        self,
        topic: str,
        message_encoding: str,
        schema_id: int,
        metadata: dict[str, str] | None = None,
    ) -> int:
        if topic in self._channel_ids_by_topic:
            return self._channel_ids_by_topic[topic]
        if schema_id != 0 and schema_id not in self._schemas:
            raise ValueError(f"unknown schema id {schema_id}")
        cid = len(self._channels)  # channel ids start at 0 like rosbag2
        channel = McapChannel(
            id=cid,
            topic=topic,
            message_encoding=message_encoding,
            schema_id=schema_id,
            metadata=dict(metadata or {}),
        )
        self._channels[cid] = channel
        self._channel_ids_by_topic[topic] = cid
        self._counts[cid] = 0
        self._sequence[cid] = 0
        self._write(channel.pack())
        return cid

    def write_message(
        self,
        channel_id: int,
        log_time_ns: int,
        publish_time_ns: int,
        data: bytes,
        sequence: int | None = None,
    ) -> None:
        if channel_id not in self._channels:
            raise ValueError(f"unknown channel id {channel_id}")
        if sequence is None:
            sequence = self._sequence[channel_id]
        self._sequence[channel_id] = sequence + 1
        self._write(
            McapMessage(channel_id, sequence, log_time_ns, publish_time_ns, bytes(data)).pack()
        )
        self._counts[channel_id] += 1
        self._message_count += 1
        if self._start is None or log_time_ns < self._start:
            self._start = log_time_ns
        if self._end is None or log_time_ns > self._end:
            self._end = log_time_ns

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._write(pack_data_end(0))

        summary_start = self._pos
        groups: list[McapSummaryOffset] = []

        if self._schemas:
            start = self._pos
            for sid in sorted(self._schemas):
                self._write(self._schemas[sid].pack())
            groups.append(McapSummaryOffset(OP_SCHEMA, start, self._pos - start))
        if self._channels:
            start = self._pos
            for cid in sorted(self._channels):
                self._write(self._channels[cid].pack())
            groups.append(McapSummaryOffset(OP_CHANNEL, start, self._pos - start))

        start = self._pos
        stats = McapStatistics(
            message_count=self._message_count,
            schema_count=len(self._schemas),
            channel_count=len(self._channels),
            attachment_count=0,
            metadata_count=0,
            chunk_count=0,
            message_start_time=self._start or 0,
            message_end_time=self._end or 0,
            channel_message_counts=dict(self._counts),
        )
        self._write(stats.pack())
        groups.append(McapSummaryOffset(OP_STATISTICS, start, self._pos - start))

        summary_offset_start = self._pos
        for g in groups:
            self._write(g.pack())

        self._write(
            McapFooter(
                summary_start=summary_start,
                summary_offset_start=summary_offset_start,
                summary_crc=0,
            ).pack()
        )
        self._write(MAGIC)
        self._fh.flush()
        if self._owns:
            self._fh.close()

    def __enter__(self) -> McapWriter:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def message_count(self) -> int:
        return self._message_count


def write_to_bytes(build) -> bytes:
    """Helper for tests: ``build(writer)`` populates the file; returns its bytes."""
    buf = io.BytesIO()
    w = McapWriter(buf)
    build(w)
    w.close()
    return buf.getvalue()
