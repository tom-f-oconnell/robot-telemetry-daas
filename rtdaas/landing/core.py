"""Land closed MCAP bags under ``<prefix>/<robot_id>/<yyyy>/<mm>/<dd>/<file>``.

That is the layout ``telemetry_bag_ingest``'s sensor and ``batch_mcap`` read. The robot id and
the day come from the bag itself (the topic namespace and the first message's log time), not from
where it was recorded: the simulator writes ``<root>/<robot>/<yyyy>/<mm>/<dd>/<session>.mcap``,
while ``ros2 bag record -o <name>`` writes ``<name>/<name>_0.mcap``.

A bag is *closed* once the file both starts and ends with the MCAP magic: writers append the
footer and the trailing magic last, so a bag that is still being recorded is skipped and picked
up on a later pass. Landing is idempotent: a key already present with the same size is left
alone, and one present with a different size is reported as a conflict rather than overwritten
(``ros2 bag record -o dv_001`` reuses file names across recordings).
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from rtdaas.local.bags import ns_to_datetime, topic_parts
from rtdaas.mcap_lite.reader import read_summary
from rtdaas.mcap_lite.records import MAGIC, McapFormatError

log = logging.getLogger(__name__)

# Landing.status values
UPLOADED = "uploaded"
PRESENT = "present"  # already in the store with the same size
OPEN = "open"  # still being written
INVALID = "invalid"  # unreadable, empty, or not exactly one robot namespace
CONFLICT = "conflict"  # the key exists with a different size
DRY_RUN = "dry-run"
FAILED_STATUSES = frozenset({INVALID, CONFLICT})


class Store(Protocol):
    """Where bags land: an S3 bucket in practice (:class:`rtdaas.landing.s3.S3Store`)."""

    def size(self, key: str) -> int | None:
        """Size of the object at ``key``, or ``None`` if there is none."""

    def put(self, path: Path, key: str) -> None:
        """Upload ``path`` to ``key``."""


@dataclass(frozen=True)
class Landing:
    path: Path
    status: str
    key: str = ""
    detail: str = ""


def is_closed(path: str | Path) -> bool:
    """True if the file starts and ends with the MCAP magic (the writer has finished it)."""
    p = Path(path)
    try:
        if p.stat().st_size < 2 * len(MAGIC):
            return False
        with p.open("rb") as f:
            head = f.read(len(MAGIC))
            f.seek(-len(MAGIC), 2)
            return head == MAGIC and f.read() == MAGIC
    except OSError:
        return False


def bag_key(path: str | Path, prefix: str = "bags") -> str:
    """``<prefix>/<robot_id>/<yyyy>/<mm>/<dd>/<file name>`` for a closed bag."""
    p = Path(path)
    summary = read_summary(p)
    robots = {topic_parts(c["topic"])[0] for c in summary["channels"].values()} - {None}
    if len(robots) != 1:
        found = ", ".join(sorted(robots)) or "none"
        raise ValueError(f"expected one robot namespace in the bag's topics, found {found}")
    if not summary["message_count"]:
        raise ValueError("bag has no messages")
    day = ns_to_datetime(summary["start_ns"])
    return f"{prefix.strip('/')}/{robots.pop()}/{day:%Y}/{day:%m}/{day:%d}/{p.name}"


def find_bags(root: str | Path) -> list[Path]:
    return sorted(Path(root).rglob("*.mcap"))


def land(
    paths: Iterable[Path], store: Store, prefix: str = "bags", dry_run: bool = False
) -> list[Landing]:
    """Land every closed bag in ``paths``; one :class:`Landing` per path."""
    out: list[Landing] = []
    for path in paths:
        if not is_closed(path):
            out.append(Landing(path, OPEN))
            continue
        try:
            key = bag_key(path, prefix)
        except (McapFormatError, ValueError, OSError) as exc:
            out.append(Landing(path, INVALID, detail=str(exc)))
            continue
        size = path.stat().st_size
        existing = store.size(key)
        if existing == size:
            out.append(Landing(path, PRESENT, key))
        elif existing is not None:
            out.append(Landing(path, CONFLICT, key, f"{existing} bytes in the store, {size} here"))
        elif dry_run:
            out.append(Landing(path, DRY_RUN, key))
        else:
            store.put(path, key)
            out.append(Landing(path, UPLOADED, key))
    for item in out:
        level = logging.WARNING if item.status in FAILED_STATUSES else logging.INFO
        if item.status != OPEN:
            log.log(level, "%s %s %s %s", item.status, item.path, item.key, item.detail)
    return out
