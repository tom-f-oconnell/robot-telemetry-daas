"""``rtdaas-local`` command line: run the serial pipeline into a SQLite mirror.

Exit codes: 0 success (quality warnings allowed), 1 usage/runtime error,
2 blocking quality failures when ``--fail-on-blocking`` is given.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from rtdaas.config import DEFAULT_MSGS_DIR, configure_logging

log = logging.getLogger(__name__)


def _cmd_pipeline(args: argparse.Namespace) -> int:
    from rtdaas.local.pipeline import run

    if args.bags is None and args.jigsaws is None:
        sys.stderr.write("nothing to do: pass --bags DIR and/or --jigsaws DIR\n")
        return 1
    suites = [Path(s) for s in args.suite] if args.suite else None
    counts = run(
        bags_dir=args.bags,
        jigsaws_dir=args.jigsaws,
        sqlite_path=args.out,
        msgs_dir=args.msgs,
        suites=suites,
        pipeline_version=args.pipeline_version,
        partition_value=args.partition,
        with_video=not args.no_video,
    )
    json.dump(counts, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")
    if args.fail_on_blocking and counts.get("quality", {}).get("blocking_failures"):
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    from rtdaas import __version__

    p = argparse.ArgumentParser(prog="rtdaas-local", description=__doc__)
    p.add_argument("--log-level", default=None)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser(
        "pipeline", help="bags/JIGSAWS -> SQLite mirror (+ quality suites + catalog)"
    )
    r.add_argument("--bags", default=None, help="directory of MCAP bags (searched recursively)")
    r.add_argument("--jigsaws", default=None, help="JIGSAWS-layout dataset root")
    r.add_argument("--out", required=True, help="SQLite file to write")
    r.add_argument(
        "--msgs", default=str(DEFAULT_MSGS_DIR), help="ROS message definitions directory"
    )
    r.add_argument(
        "--suite",
        action="append",
        help="quality suite YAML (repeatable; default: quality/checks/*.yml)",
    )
    r.add_argument(
        "--partition", default=None, help="YYYY-MM-DD partition filter for quality checks"
    )
    r.add_argument("--pipeline-version", default=__version__)
    r.add_argument("--no-video", action="store_true", help="skip video frame features")
    r.add_argument(
        "--fail-on-blocking", action="store_true", help="exit 2 if a blocking quality check fails"
    )
    r.set_defaults(func=_cmd_pipeline)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args.log_level)
    try:
        return int(args.func(args))
    except Exception as exc:  # noqa: BLE001 - CLI boundary
        log.error("%s", exc)
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
