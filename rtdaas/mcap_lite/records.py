"""MCAP v1 record definitions and low-level (de)serialisation helpers.

Reference: https://mcap.dev/spec. Only what the platform needs is implemented:
Header, Footer, Schema, Channel, Message, Chunk (read only), DataEnd, Statistics
and SummaryOffset. Every record is ``opcode (u8) | length (u64 LE) | body``.
Strings are ``u32 length | UTF-8``; maps are ``u32 byte_length | entries``.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass, field

MAGIC = b"\x89MCAP0\r\n"

OP_HEADER = 0x01
OP_FOOTER = 0x02
OP_SCHEMA = 0x03
OP_CHANNEL = 0x04
OP_MESSAGE = 0x05
OP_CHUNK = 0x06
OP_MESSAGE_INDEX = 0x07
OP_CHUNK_INDEX = 0x08
OP_ATTACHMENT = 0x09
OP_ATTACHMENT_INDEX = 0x0A
OP_STATISTICS = 0x0B
OP_METADATA = 0x0C
OP_METADATA_INDEX = 0x0D
OP_SUMMARY_OFFSET = 0x0E
OP_DATA_END = 0x0F

OPCODE_NAMES = {
    OP_HEADER: "Header",
    OP_FOOTER: "Footer",
    OP_SCHEMA: "Schema",
    OP_CHANNEL: "Channel",
    OP_MESSAGE: "Message",
    OP_CHUNK: "Chunk",
    OP_MESSAGE_INDEX: "MessageIndex",
    OP_CHUNK_INDEX: "ChunkIndex",
    OP_ATTACHMENT: "Attachment",
    OP_ATTACHMENT_INDEX: "AttachmentIndex",
    OP_STATISTICS: "Statistics",
    OP_METADATA: "Metadata",
    OP_METADATA_INDEX: "MetadataIndex",
    OP_SUMMARY_OFFSET: "SummaryOffset",
    OP_DATA_END: "DataEnd",
}

FOOTER_BODY_LEN = 8 + 8 + 4
FOOTER_RECORD_LEN = 1 + 8 + FOOTER_BODY_LEN


class McapFormatError(ValueError):
    pass


# --------------------------------------------------------------------------- primitives


def pack_string(value: str) -> bytes:
    data = value.encode("utf-8")
    return struct.pack("<I", len(data)) + data


def pack_record(opcode: int, body: bytes) -> bytes:
    return struct.pack("<BQ", opcode, len(body)) + body


def pack_str_map(entries: dict[str, str]) -> bytes:
    body = b"".join(pack_string(k) + pack_string(v) for k, v in entries.items())
    return struct.pack("<I", len(body)) + body


class Cursor:
    """Sequential reader over a bytes object."""

    __slots__ = ("data", "pos")

    def __init__(self, data: bytes, pos: int = 0) -> None:
        self.data = data
        self.pos = pos

    def remaining(self) -> int:
        return len(self.data) - self.pos

    def take(self, n: int) -> bytes:
        end = self.pos + n
        if end > len(self.data):
            raise McapFormatError(f"truncated record: needed {n} bytes at offset {self.pos}")
        out = self.data[self.pos : end]
        self.pos = end
        return out

    def u8(self) -> int:
        return self.take(1)[0]

    def u16(self) -> int:
        return struct.unpack("<H", self.take(2))[0]

    def u32(self) -> int:
        return struct.unpack("<I", self.take(4))[0]

    def u64(self) -> int:
        return struct.unpack("<Q", self.take(8))[0]

    def string(self) -> str:
        n = self.u32()
        return self.take(n).decode("utf-8")

    def str_map(self) -> dict[str, str]:
        n = self.u32()
        end = self.pos + n
        out: dict[str, str] = {}
        while self.pos < end:
            k = self.string()
            v = self.string()
            out[k] = v
        if self.pos != end:
            raise McapFormatError("malformed string map")
        return out


# --------------------------------------------------------------------------- records


@dataclass
class McapHeader:
    profile: str
    library: str

    def pack(self) -> bytes:
        return pack_record(OP_HEADER, pack_string(self.profile) + pack_string(self.library))

    @classmethod
    def unpack(cls, body: bytes) -> McapHeader:
        c = Cursor(body)
        return cls(profile=c.string(), library=c.string())


@dataclass
class McapFooter:
    summary_start: int
    summary_offset_start: int
    summary_crc: int = 0

    def pack(self) -> bytes:
        return pack_record(OP_FOOTER, struct.pack("<QQI", self.summary_start, self.summary_offset_start, self.summary_crc))

    @classmethod
    def unpack(cls, body: bytes) -> McapFooter:
        a, b, c = struct.unpack("<QQI", body[:FOOTER_BODY_LEN])
        return cls(a, b, c)


@dataclass
class McapSchema:
    id: int
    name: str
    encoding: str
    data: bytes

    def pack(self) -> bytes:
        body = struct.pack("<H", self.id) + pack_string(self.name) + pack_string(self.encoding)
        body += struct.pack("<I", len(self.data)) + self.data
        return pack_record(OP_SCHEMA, body)

    @classmethod
    def unpack(cls, body: bytes) -> McapSchema:
        c = Cursor(body)
        sid = c.u16()
        name = c.string()
        encoding = c.string()
        n = c.u32()
        return cls(id=sid, name=name, encoding=encoding, data=c.take(n))


@dataclass
class McapChannel:
    id: int
    topic: str
    message_encoding: str
    schema_id: int
    metadata: dict[str, str] = field(default_factory=dict)

    def pack(self) -> bytes:
        body = struct.pack("<HH", self.id, self.schema_id) + pack_string(self.topic) + pack_string(self.message_encoding)
        body += pack_str_map(self.metadata)
        return pack_record(OP_CHANNEL, body)

    @classmethod
    def unpack(cls, body: bytes) -> McapChannel:
        c = Cursor(body)
        cid = c.u16()
        sid = c.u16()
        topic = c.string()
        enc = c.string()
        meta = c.str_map()
        return cls(id=cid, topic=topic, message_encoding=enc, schema_id=sid, metadata=meta)


@dataclass
class McapMessage:
    channel_id: int
    sequence: int
    log_time_ns: int
    publish_time_ns: int
    data: bytes

    def pack(self) -> bytes:
        return pack_record(
            OP_MESSAGE,
            struct.pack("<HIQQ", self.channel_id, self.sequence, self.log_time_ns, self.publish_time_ns) + self.data,
        )

    @classmethod
    def unpack(cls, body: bytes) -> McapMessage:
        channel_id, sequence, log_time, publish_time = struct.unpack("<HIQQ", body[:22])
        return cls(channel_id, sequence, log_time, publish_time, body[22:])


@dataclass
class McapChunk:
    message_start_time: int
    message_end_time: int
    uncompressed_size: int
    uncompressed_crc: int
    compression: str
    records: bytes  # still compressed if compression != ""

    @classmethod
    def unpack(cls, body: bytes) -> McapChunk:
        c = Cursor(body)
        start = c.u64()
        end = c.u64()
        usize = c.u64()
        ucrc = c.u32()
        comp = c.string()
        n = c.u64()
        return cls(start, end, usize, ucrc, comp, c.take(n))

    def decompressed(self) -> bytes:
        if self.compression == "":
            return self.records
        if self.compression == "zstd":
            try:
                import zstandard
            except ImportError as exc:  # pragma: no cover
                raise McapFormatError("chunk uses zstd compression; install `zstandard`") from exc
            return zstandard.ZstdDecompressor().decompressobj().decompress(self.records)
        if self.compression == "lz4":
            try:
                import lz4.frame
            except ImportError as exc:  # pragma: no cover
                raise McapFormatError("chunk uses lz4 compression; install `lz4`") from exc
            return lz4.frame.decompress(self.records)
        raise McapFormatError(f"unsupported chunk compression {self.compression!r}")


@dataclass
class McapStatistics:
    message_count: int
    schema_count: int
    channel_count: int
    attachment_count: int
    metadata_count: int
    chunk_count: int
    message_start_time: int
    message_end_time: int
    channel_message_counts: dict[int, int]

    def pack(self) -> bytes:
        counts = b"".join(struct.pack("<HQ", cid, n) for cid, n in sorted(self.channel_message_counts.items()))
        body = struct.pack(
            "<QHIIIIQQ",
            self.message_count,
            self.schema_count,
            self.channel_count,
            self.attachment_count,
            self.metadata_count,
            self.chunk_count,
            self.message_start_time,
            self.message_end_time,
        )
        body += struct.pack("<I", len(counts)) + counts
        return pack_record(OP_STATISTICS, body)

    @classmethod
    def unpack(cls, body: bytes) -> McapStatistics:
        c = Cursor(body)
        (mc, sc, cc, ac, mdc, chc, start, end) = struct.unpack("<QHIIIIQQ", c.take(8 + 2 + 4 * 4 + 8 + 8))
        n = c.u32()
        end_pos = c.pos + n
        counts: dict[int, int] = {}
        while c.pos < end_pos:
            cid = c.u16()
            counts[cid] = c.u64()
        return cls(mc, sc, cc, ac, mdc, chc, start, end, counts)


@dataclass
class McapSummaryOffset:
    group_opcode: int
    group_start: int
    group_length: int

    def pack(self) -> bytes:
        return pack_record(OP_SUMMARY_OFFSET, struct.pack("<BQQ", self.group_opcode, self.group_start, self.group_length))

    @classmethod
    def unpack(cls, body: bytes) -> McapSummaryOffset:
        return cls(*struct.unpack("<BQQ", body[:17]))


def pack_data_end(crc: int = 0) -> bytes:
    return pack_record(OP_DATA_END, struct.pack("<I", crc))


def iter_records(data: bytes, start: int = 0, end: int | None = None):
    """Yield ``(offset, opcode, body)`` for consecutive records in ``data[start:end]``."""
    end = len(data) if end is None else end
    pos = start
    while pos < end:
        if end - pos < 9:
            raise McapFormatError(f"truncated record header at offset {pos}")
        opcode, length = struct.unpack("<BQ", data[pos : pos + 9])
        body_start = pos + 9
        body_end = body_start + length
        if body_end > end:
            raise McapFormatError(f"record {OPCODE_NAMES.get(opcode, opcode)} at {pos} overruns the stream")
        yield pos, opcode, data[body_start:body_end]
        pos = body_end


def describe(opcode: int) -> str:
    return OPCODE_NAMES.get(opcode, f"0x{opcode:02x}")
