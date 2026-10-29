"""Dependency-free MCAP v1 writer/reader used for recorded bags (spec §4.2)."""

from rtdaas.mcap_lite.reader import iter_record_boundaries, read_header, read_messages, read_summary
from rtdaas.mcap_lite.records import McapChannel, McapFormatError, McapHeader, McapMessage, McapSchema, McapStatistics
from rtdaas.mcap_lite.writer import McapWriter, write_to_bytes

__all__ = [
    "McapChannel",
    "McapFormatError",
    "McapHeader",
    "McapMessage",
    "McapSchema",
    "McapStatistics",
    "McapWriter",
    "iter_record_boundaries",
    "read_header",
    "read_messages",
    "read_summary",
    "write_to_bytes",
]
