from __future__ import annotations

from pathlib import Path

from rtdaas.landing import bag_key, find_bags, is_closed, land
from rtdaas.landing.core import CONFLICT, DRY_RUN, INVALID, OPEN, PRESENT, UPLOADED
from rtdaas.sim.cli import main as sim_main
from rtdaas.sim.recorder import McapRecorder
from rtdaas.sim.robot import TOPIC_TYPES, RobotSim, SimConfig


class FakeStore:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def size(self, key: str) -> int | None:
        return len(self.objects[key]) if key in self.objects else None

    def put(self, path: Path, key: str) -> None:
        self.objects[key] = path.read_bytes()


def _sim_bags(root: Path, start: str = "2026-10-28T23:59:00Z") -> list[Path]:
    rc = sim_main(
        ["run", "--robots", "2", "--duration", "2", "--rate", "10", "--no-realtime"]
        + ["--start-time", start, "--mcap-out", str(root)]
    )
    assert rc == 0
    return find_bags(root)


def test_keys_come_from_the_bag_not_its_path(tmp_path):
    bags = _sim_bags(tmp_path / "sim")
    flat = tmp_path / "ros2bag" / "dv_002_0.mcap"  # ros2 bag record -o dv_002 layout
    flat.parent.mkdir()
    flat.write_bytes(bags[1].read_bytes())
    assert bag_key(bags[0]) == "bags/dv_001/2026/10/28/dv_001-20261028T235900.mcap"
    assert bag_key(flat, prefix="/raw/bags/") == "raw/bags/dv_002/2026/10/28/dv_002_0.mcap"


def test_open_bags_wait_and_landing_is_idempotent(tmp_path):
    bags = _sim_bags(tmp_path)
    partial = tmp_path / "dv_003" / "recording.mcap"  # still being written: no footer yet
    partial.parent.mkdir()
    partial.write_bytes(bags[0].read_bytes()[:-200])
    assert all(is_closed(b) for b in bags) and not is_closed(partial)

    store = FakeStore()
    assert [r.status for r in land(find_bags(tmp_path), store, dry_run=True)] == [
        DRY_RUN,
        DRY_RUN,
        OPEN,
    ]
    assert store.objects == {}
    first = land(find_bags(tmp_path), store)
    assert [r.status for r in first] == [UPLOADED, UPLOADED, OPEN]
    assert store.objects[first[0].key] == bags[0].read_bytes()
    assert [r.status for r in land(find_bags(tmp_path), store)] == [PRESENT, PRESENT, OPEN]


def test_a_different_bag_at_the_same_key_is_a_conflict_not_an_overwrite(tmp_path):
    bags = _sim_bags(tmp_path)
    store = FakeStore()
    store.objects[bag_key(bags[0])] = b"an earlier recording"
    results = land(bags, store)
    assert [r.status for r in results] == [CONFLICT, UPLOADED]
    assert store.objects[results[0].key] == b"an earlier recording"


def test_bags_mixing_robots_or_unreadable_are_invalid(tmp_path, msg_registry):
    mixed = tmp_path / "mixed.mcap"
    rec = McapRecorder(mixed, msg_registry, TOPIC_TYPES)
    t0 = 1_793_000_000 * 10**9
    for rid in ("dv_001", "dv_002"):
        sim = RobotSim(SimConfig(robot_id=rid, rate_hz=10.0))
        for kafka_topic, ros_topic, msg in sim.begin(t0):
            rec.record(ros_topic, TOPIC_TYPES[kafka_topic], msg, t0)
    rec.close()
    junk = tmp_path / "junk.mcap"
    junk.write_bytes(b"\x89MCAP0\r\n" + b"\0" * 64 + b"\x89MCAP0\r\n")
    results = {r.path.name: r for r in land(find_bags(tmp_path), FakeStore())}
    assert results["mixed.mcap"].status == INVALID
    assert "dv_001, dv_002" in results["mixed.mcap"].detail
    assert results["junk.mcap"].status == INVALID
