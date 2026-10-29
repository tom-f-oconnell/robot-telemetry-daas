"""Sequential MCAP reader.

Streams the data section record by record and yields ``(schema, channel, message)``
tuples. Chunked files (what ``ros2 bag record`` writes) are supported: chunk
records are unwrapped, and decompressed when ``zstandard`` / ``lz4`` are
installed. Index and attachment records are skipped. The summary section is only
used by :func:`read_summary`.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

from rtdaas.mcap_lite.records import (
    FOOTER_RECORD_LEN,
    MAGIC,
    OP_CHANNEL,
    OP_CHUNK,
    OP_DATA_END,
    OP_FOOTER,
    OP_HEADER,
    OP_MESSAGE,
    OP_SCHEMA,
    OP_STATISTICS,
    McapChannel,
    McapChunk,
    McapFooter,
    McapFormatError,
    McapHeader,
    McapMessage,
    McapSchema,
    McapStatistics,
    iter_records,
)


def _load(source: str | Path | bytes | bytearray | memoryview) -> bytes:
    if isinstance(source, (bytes, bytearray, memoryview)):
        return bytes(source)
    return Path(source).read_bytes()


def _check_magic(data: bytes) -> None:
    if len(data) < 2 * len(MAGIC) or data[: len(MAGIC)] != MAGIC:
        raise McapFormatError("not an MCAP file (bad leading magic)")
    if data[-len(MAGIC) :] != MAGIC:
        raise McapFormatError("truncated MCAP file (bad trailing magic)")


def read_header(source) -> McapHeader:
    data = _load(source)
    _check_magic(data)
    for _, opcode, body in iter_records(data, len(MAGIC)):
        if opcode != OP_HEADER:
            raise McapFormatError("first record is not a Header")
        return McapHeader.unpack(body)
    raise McapFormatError("empty MCAP file")


def _iter_data_records(data: bytes) -> Iterator[tuple[int, bytes]]:
    """Yield ``(opcode, body)`` for the data section, unwrapping chunks."""
    for _, opcode, body in iter_records(data, len(MAGIC), len(data) - len(MAGIC)):
        if opcode == OP_DATA_END:
            return
        if opcode == OP_FOOTER:  # file without DataEnd — tolerate
            return
        if opcode == OP_CHUNK:
            chunk = McapChunk.unpack(body)
            inner = chunk.decompressed()
            for _, op2, body2 in iter_records(inner):
                yield op2, body2
            continue
        yield opcode, body


def read_messages(source) -> Iterator[tuple[McapSchema | None, McapChannel, McapMessage]]:
    data = _load(source)
    _check_magic(data)
    schemas: dict[int, McapSchema] = {}
    channels: dict[int, McapChannel] = {}
    for opcode, body in _iter_data_records(data):
        if opcode == OP_SCHEMA:
            s = McapSchema.unpack(body)
            schemas[s.id] = s
        elif opcode == OP_CHANNEL:
            c = McapChannel.unpack(body)
            channels[c.id] = c
        elif opcode == OP_MESSAGE:
            m = McapMessage.unpack(body)
            channel = channels.get(m.channel_id)
            if channel is None:
                raise McapFormatError(f"message references unknown channel {m.channel_id}")
            schema = schemas.get(channel.schema_id) if channel.schema_id else None
            yield schema, channel, m


def read_footer(data: bytes) -> McapFooter:
    _check_magic(data)
    start = len(data) - len(MAGIC) - FOOTER_RECORD_LEN
    if start < len(MAGIC) or data[start] != OP_FOOTER:
        raise McapFormatError("footer record not found")
    return McapFooter.unpack(data[start + 9 : start + FOOTER_RECORD_LEN])


def read_summary(source) -> dict[str, Any]:
    """Schemas, channels and statistics, from the summary section when present else by scanning."""
    data = _load(source)
    footer = read_footer(data)
    schemas: dict[int, McapSchema] = {}
    channels: dict[int, McapChannel] = {}
    stats: McapStatistics | None = None
    if footer.summary_start:
        end = footer.summary_offset_start or (len(data) - len(MAGIC) - FOOTER_RECORD_LEN)
        for _, opcode, body in iter_records(data, footer.summary_start, end):
            if opcode == OP_SCHEMA:
                s = McapSchema.unpack(body)
                schemas[s.id] = s
            elif opcode == OP_CHANNEL:
                c = McapChannel.unpack(body)
                channels[c.id] = c
            elif opcode == OP_STATISTICS:
                stats = McapStatistics.unpack(body)
    if stats is None:
        counts: dict[int, int] = {}
        start_ns: int | None = None
        end_ns: int | None = None
        for schema, channel, msg in read_messages(data):
            if schema is not None:
                schemas[schema.id] = schema
            channels[channel.id] = channel
            counts[channel.id] = counts.get(channel.id, 0) + 1
            start_ns = msg.log_time_ns if start_ns is None else min(start_ns, msg.log_time_ns)
            end_ns = msg.log_time_ns if end_ns is None else max(end_ns, msg.log_time_ns)
        stats = McapStatistics(
            message_count=sum(counts.values()),
            schema_count=len(schemas),
            channel_count=len(channels),
            attachment_count=0,
            metadata_count=0,
            chunk_count=0,
            message_start_time=start_ns or 0,
            message_end_time=end_ns or 0,
            channel_message_counts=counts,
        )
    header = read_header(data)
    return {
        "profile": header.profile,
        "library": header.library,
        "schemas": {sid: {"name": s.name, "encoding": s.encoding, "data": s.data} for sid, s in sorted(schemas.items())},
        "channels": {
            cid: {"topic": c.topic, "message_encoding": c.message_encoding, "schema_id": c.schema_id, "metadata": c.metadata}
            for cid, c in sorted(channels.items())
        },
        "message_count": stats.message_count,
        "channel_message_counts": dict(stats.channel_message_counts),
        "start_ns": stats.message_start_time,
        "end_ns": stats.message_end_time,
        "footer": {"summary_start": footer.summary_start, "summary_offset_start": footer.summary_offset_start},
    }


def iter_record_boundaries(source) -> Iterator[tuple[int, int, int]]:
    """``(offset, opcode, length)`` for every top-level record — used by structural tests."""
    data = _load(source)
    _check_magic(data)
    for offset, opcode, body in iter_records(data, len(MAGIC), len(data) - len(MAGIC)):
        yield offset, opcode, len(body)
