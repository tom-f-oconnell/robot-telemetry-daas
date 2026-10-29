from __future__ import annotations

import struct

import pytest

from rtdaas.mcap_lite import McapWriter, read_messages, read_summary
from rtdaas.mcap_lite.reader import iter_record_boundaries, read_header

MAGIC = b"\x89MCAP0\r\n"


def _write(path, n=5):
    with McapWriter(path, profile="ros2", library="test") as w:
        sid = w.add_schema("std_msgs/String", "ros2msg", b"string data\n")
        cid = w.add_channel("/chatter", "cdr", sid, metadata={"k": "v"})
        for i in range(n):
            w.write_message(cid, log_time_ns=1_000 + i, publish_time_ns=1_000 + i, data=bytes([i]) * 4)
    return path


def test_round_trip(tmp_path):
    path = _write(tmp_path / "t.mcap", n=7)
    msgs = list(read_messages(path))
    assert len(msgs) == 7
    schema, channel, message = msgs[0]
    assert schema.name == "std_msgs/String" and schema.encoding == "ros2msg" and schema.data == b"string data\n"
    assert channel.topic == "/chatter" and channel.message_encoding == "cdr" and channel.metadata == {"k": "v"}
    assert message.data == b"\x00" * 4 and message.log_time_ns == 1_000
    assert [m.sequence for _, _, m in msgs] == list(range(7))
    assert [m.log_time_ns for _, _, m in msgs] == list(range(1_000, 1_007))


def test_file_layout(tmp_path):
    path = _write(tmp_path / "t.mcap", n=3)
    data = path.read_bytes()
    assert data.startswith(MAGIC) and data.endswith(MAGIC)
    # the footer record is the last record before the closing magic: opcode 0x02, len 20
    footer_start = len(data) - len(MAGIC) - (1 + 8 + 20)
    assert data[footer_start] == 0x02
    assert struct.unpack("<Q", data[footer_start + 1 : footer_start + 9])[0] == 20
    header = read_header(path)
    assert header.profile == "ros2" and header.library == "test"
    opcodes = [op for _, op, _ in iter_record_boundaries(path)]
    assert opcodes[0] == 0x01  # header
    assert 0x0F in opcodes  # data end
    assert 0x0B in opcodes  # statistics in the summary section
    assert opcodes[-1] == 0x02  # footer


def test_summary(tmp_path):
    path = _write(tmp_path / "t.mcap", n=4)
    s = read_summary(path)
    assert s["message_count"] == 4
    assert s["start_ns"] == 1_000 and s["end_ns"] == 1_003
    assert len(s["schemas"]) == 1 and len(s["channels"]) == 1


def test_reads_from_bytes(tmp_path):
    path = _write(tmp_path / "t.mcap", n=2)
    msgs = list(read_messages(path.read_bytes()))
    assert len(msgs) == 2


def test_rejects_non_mcap(tmp_path):
    bad = tmp_path / "bad.mcap"
    bad.write_bytes(b"not an mcap file")
    with pytest.raises(Exception):
        list(read_messages(bad))
