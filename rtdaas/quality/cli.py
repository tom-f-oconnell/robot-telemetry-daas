"""``rtdaas-quality`` command line: run a check suite against the SQLite mirror (or Spark).

Exit codes: 0 all good (warnings allowed), 1 usage/runtime error, 2 blocking failures.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, datetime

from rtdaas.config import configure_logging

log = logging.getLogger(__name__)


def _cmd_run(args: argparse.Namespace) -> int:
    from rtdaas.quality.engine import (
        has_blocking_failures,
        load_suite,
        new_run_id,
        run_suite,
        summary,
        to_frame,
    )

    if args.backend == "sqlite":
        from rtdaas.quality.adapters import SqliteAdapter

        adapter = SqliteAdapter(args.db)
    elif args.backend == "spark":
        from rtdaas.quality.adapters import SparkAdapter
        from rtdaas.spark.common import build_spark  # requires the spark extra

        adapter = SparkAdapter(build_spark("rtdaas-quality"))
    else:
        raise ValueError(f"unknown backend {args.backend!r}")

    run_id = args.run_id or new_run_id()
    run_ts = datetime.now(UTC)
    results = []
    for suite_path in args.suite:
        results.extend(
            run_suite(load_suite(suite_path), adapter, args.partition, run_id, now=run_ts)
        )

    n_fail = sum(r.status == "fail" for r in results)
    n_err = sum(r.status == "error" for r in results)
    sys.stdout.write(f"run {run_id}: {len(results)} checks, {n_fail} failed, {n_err} errors\n")
    text = summary(results)
    if text:
        sys.stdout.write(text + "\n")

    if args.out_table:
        frame = to_frame(results, run_id, run_ts)
        if args.backend == "sqlite":
            from rtdaas.local.sqlite_store import SqliteStore

            SqliteStore(args.db).write("dq.check_results", frame, mode="append")
        else:
            from rtdaas.spark.common import write_iceberg

            write_iceberg(adapter.spark.createDataFrame(frame), "dq.check_results", mode="append")
        sys.stdout.write("results written to dq.check_results\n")

    return 2 if has_blocking_failures(results) else 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rtdaas-quality", description=__doc__)
    p.add_argument("--log-level", default=None)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run one or more suites")
    r.add_argument("--suite", action="append", required=True, help="suite YAML (repeatable)")
    r.add_argument("--backend", choices=["sqlite", "spark"], default="sqlite")
    r.add_argument(
        "--db", default="./data/rtdaas.sqlite", help="sqlite mirror path (sqlite backend)"
    )
    r.add_argument("--partition", default=None, help="YYYY-MM-DD partition filter")
    r.add_argument("--run-id", default=None)
    r.add_argument("--out-table", action="store_true", help="append results to dq.check_results")
    r.set_defaults(func=_cmd_run)
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
