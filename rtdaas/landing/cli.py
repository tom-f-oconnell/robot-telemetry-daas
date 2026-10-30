"""``rtdaas-land``: upload closed MCAP bags to the raw bucket (see :mod:`rtdaas.landing.core`).

    rtdaas-land --root data/bags                      # one pass, then exit
    rtdaas-land --root /data/bags --watch --interval 15
    rtdaas-land --root data/bags --dry-run            # print the keys, upload nothing

Exit codes: 0 every closed bag landed (or was already there), 1 runtime error, 2 at least one
bag was invalid or conflicted with a different object at its key (one pass only; ``--watch``
logs those and keeps going).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections import Counter
from pathlib import Path

from rtdaas.config import configure_logging, get_settings
from rtdaas.landing.core import FAILED_STATUSES, OPEN, find_bags, land

log = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="rtdaas-land", description="Upload closed MCAP bags to the raw bucket."
    )
    p.add_argument("--root", required=True, help="directory searched recursively for *.mcap")
    p.add_argument("--bucket", default=None, help="default: S3_BUCKET_RAW")
    p.add_argument("--prefix", default="bags")
    p.add_argument("--watch", action="store_true", help="keep scanning every --interval s")
    p.add_argument("--interval", type=float, default=15.0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--stale-after",
        type=float,
        default=300.0,
        help="warn once about an open bag unchanged for this many seconds (its recorder died)",
    )
    p.add_argument("--log-level", default=None)
    return p


def _store_errors() -> tuple[type[BaseException], ...]:
    try:
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError:  # pragma: no cover - reported when the store is created
        return (OSError,)
    return (OSError, BotoCoreError, ClientError)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    settings = get_settings()
    try:
        from rtdaas.landing.s3 import S3Store

        store = S3Store(args.bucket or settings.s3_bucket_raw, settings)
    except ImportError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    errors = _store_errors()
    # (size, mtime) per path already settled, so --watch does not re-read landed bags
    settled: dict[Path, tuple[int, float]] = {}
    warned_stale: set[Path] = set()
    while True:
        try:
            pending = []
            for path in find_bags(args.root):
                st = path.stat()
                if settled.get(path) != (st.st_size, st.st_mtime):
                    pending.append(path)
            results = land(pending, store, args.prefix, args.dry_run)
        except errors as exc:
            if not args.watch:
                print(f"error: {exc}", file=sys.stderr)
                return 1
            log.warning("landing pass failed, retrying in %.0f s: %s", args.interval, exc)
            time.sleep(args.interval)
            continue
        now = time.time()
        for r in results:
            st = r.path.stat()
            if r.status != OPEN:
                settled[r.path] = (st.st_size, st.st_mtime)
            elif now - st.st_mtime > args.stale_after and r.path not in warned_stale:
                warned_stale.add(r.path)
                log.warning(
                    "%s is still open and unchanged for %.0f s: its recorder probably died "
                    "without closing it, so it will not be landed",
                    r.path,
                    now - st.st_mtime,
                )
        counts = Counter(r.status for r in results)
        if results or not args.watch:
            log.info("landing pass: %s", dict(counts) or "nothing to do")
        if not args.watch:
            return 2 if any(counts[s] for s in FAILED_STATUSES) else 0
        time.sleep(args.interval)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
