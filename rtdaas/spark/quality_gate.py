"""Quality gate: run a YAML check suite against the Iceberg tables and record the results (spec §5.4).

The same :mod:`rtdaas.quality.engine` that runs on pandas frames in unit tests and on the
SQLite mirror locally runs here through :class:`rtdaas.quality.adapters.SparkAdapter`.
Results are appended to ``lake.dq.check_results``; the process exits with status 2 when any
``blocking`` check failed (or errored) so the Airflow task fails and the publish step is
skipped.

Run::

    spark-submit rtdaas/spark/quality_gate.py --suite quality/checks/telemetry.yml --partition 2026-10-29
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from rtdaas.config import Settings, configure_logging, get_settings
from rtdaas.quality.engine import (
    CheckResult,
    has_blocking_failures,
    load_suite,
    new_run_id,
    run_suite,
    summary,
    to_frame,
)
from rtdaas.spark import ddl
from rtdaas.spark.common import build_spark, ensure_tables, now_utc, write_iceberg

log = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_BLOCKED = 2


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--suite", required=True, action="append", help="suite YAML (repeatable)")
    p.add_argument(
        "--partition", default=None, help="YYYY-MM-DD applied to each table's partition_column"
    )
    p.add_argument("--run-id", default=None)
    p.add_argument(
        "--no-record", action="store_true", help="do not append results to dq.check_results"
    )
    p.add_argument(
        "--allow-blocking",
        action="store_true",
        help="exit 0 even on blocking failures (reporting runs)",
    )
    return p.parse_args(argv)


def run(
    spark,
    settings: Settings,
    suites: list[Path],
    partition: str | None,
    run_id: str | None = None,
    record: bool = True,
) -> tuple[str, list[CheckResult]]:
    from rtdaas.quality.adapters import SparkAdapter

    ensure_tables(spark)
    run_id = run_id or new_run_id()
    adapter = SparkAdapter(spark, catalog=ddl.CATALOG)
    results: list[CheckResult] = []
    for path in suites:
        suite = load_suite(path)
        res = run_suite(suite, adapter, partition_value=partition, run_id=run_id)
        log.info(
            "suite %s: %d checks, %d non-passing",
            suite.get("suite"),
            len(res),
            sum(r.status != "pass" for r in res),
        )
        results.extend(res)

    if record and results:
        pdf = to_frame(results, run_id, now_utc())
        df = spark.createDataFrame(pdf, schema=ddl.struct_type(ddl.CHECK_RESULTS.name))
        write_iceberg(df, ddl.CHECK_RESULTS.name, mode="append")
        log.info("recorded %d check results under run_id=%s", len(results), run_id)
    return run_id, results


def main(argv=None) -> int:
    args = parse_args(argv)
    configure_logging()
    settings = get_settings()
    suites = [Path(s) for s in args.suite]
    spark = build_spark("rtdaas-quality-gate", settings)
    run_id, results = run(
        spark, settings, suites, args.partition, args.run_id, record=not args.no_record
    )

    report = summary(results)
    if report:
        log.warning("non-passing checks (run_id=%s):\n%s", run_id, report)
    blocked = has_blocking_failures(results)
    log.info(
        "quality gate run_id=%s partition=%s blocking_failures=%s", run_id, args.partition, blocked
    )
    if blocked and not args.allow_blocking:
        return EXIT_BLOCKED
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
