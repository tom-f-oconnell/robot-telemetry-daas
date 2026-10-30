"""decode_bag is the unit boundary between MCAP bags and Spark rows — pure Python."""

from __future__ import annotations

import datetime as dt

import pytest

from rtdaas.contracts.cdr import CdrCodec
from rtdaas.sim.cli import main as sim_main
from rtdaas.spark.decode import decode_bag, iter_rows


@pytest.fixture(scope="module")
def bag(tmp_path_factory):
    out = tmp_path_factory.mktemp("bags")
    assert (
        sim_main(
            [
                "run",
                "--robots",
                "1",
                "--duration",
                "4",
                "--rate",
                "25",
                "--seed",
                "2",
                "--mcap-out",
                str(out),
                "--no-realtime",
                "--start-time",
                "2026-10-28T10:00:00Z",
            ]
        )
        == 0
    )
    return next(out.rglob("*.mcap"))


def test_decode_bag_tables(bag, msg_registry):
    codec = CdrCodec(msg_registry)
    tables = decode_bag(bag, codec, bag.name)
    assert set(tables) == {"tool_pose", "joint_states", "events", "heartbeat"}
    assert len(tables["tool_pose"]) == 3 * 100 and len(tables["joint_states"]) == 3 * 100
    assert 3 <= len(tables["heartbeat"]) <= 6
    tp = tables["tool_pose"][0]
    assert (
        tp["robot_id"] == "dv_001"
        and tp["arm"] in {"PSM1", "PSM2", "ECM"}
        and tp["source"] == "bag"
    )
    assert (
        tp["bag_file"] == bag.name
        and isinstance(tp["ts"], dt.datetime)
        and tp["ts"].tzinfo is not None
    )
    assert tp["ts"] == dt.datetime(2026, 10, 28, 10, 0, tzinfo=dt.UTC)
    assert {"x", "y", "z", "qx", "qy", "qz", "qw", "jaw_angle", "speed", "recv_ts"} <= set(tp)
    js = tables["joint_states"][0]
    assert len(js["joint_names"]) == 6 and len(js["position"]) == 6
    ev = tables["events"][0]
    assert ev["code"] == "SESSION_START" and ev["severity_name"] == "INFO" and ev["severity"] == 1
    hb = tables["heartbeat"][0]
    assert hb["robot_id"] == "dv_001" and isinstance(hb["active_arms"], list) and hb["state"]


def test_decode_from_bytes_and_iter_rows(bag, msg_registry):
    codec = CdrCodec(msg_registry)
    from_bytes = decode_bag(bag.read_bytes(), codec, "x.mcap")
    assert from_bytes["tool_pose"][0]["bag_file"] == "x.mcap"
    kinds = {k for k, _ in iter_rows(bag, codec, bag.name)}
    assert kinds == {"tool_pose", "joint_states", "events", "heartbeat"}


def test_decode_uses_embedded_schema(bag):
    """A codec built from an unrelated registry still decodes, because the bag embeds its schemas."""
    from rtdaas.contracts.msgspec import MsgRegistry

    codec = CdrCodec(MsgRegistry({}))  # knows no types at all
    tables = decode_bag(bag, codec, bag.name)
    assert len(tables["tool_pose"]) == 300
