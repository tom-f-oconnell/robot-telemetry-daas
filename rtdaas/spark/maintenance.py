"""Iceberg table maintenance: compact small files and expire old snapshots.

Streaming appends every 30 s produce many small files; this job (scheduled by ``dq_daily``)
runs Iceberg's ``rewrite_data_files`` on the raw and KPI tables and expires snapshots older
than ``--older-than-days`` so the metadata stays small and time travel keeps a bounded window.

    spark-submit rtdaas/spark/maintenance.py [--older-than-days 7] [--table telemetry.tool_pose_raw ...]
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys

from rtdaas.config import configure_logging, get_settings
from rtdaas.spark import ddl
from rtdaas.spark.common import build_spark

log = logging.getLogger(__name__)

DEFAULT_TABLES = (*(t.name for t in ddl.RAW_TABLES.values()), ddl.KPI_1M.name)


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--older-than-days", type=int, default=7, help="expire snapshots older than this")
    p.add_argument("--table", action="append", default=None, help="table to maintain (repeatable); default: raw + kpi tables")
    p.add_argument("--target-file-size-mb", type=int, default=128)
    return p.parse_args(argv)


def maintain(spark, table: str, older_than_days: int, target_mb: int) -> dict:
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=older_than_days)).strftime("%Y-%m-%d %H:%M:%S")
    full = f"{ddl.CATALOG}.{table}"
    rewritten = spark.sql(
        f"CALL {ddl.CATALOG}.system.rewrite_data_files(table => '{table}', "
        f"options => map('target-file-size-bytes', '{target_mb * 1024 * 1024}', 'min-input-files', '5'))"
    ).collect()
    expired = spark.sql(
        f"CALL {ddl.CATALOG}.system.expire_snapshots(table => '{table}', older_than => TIMESTAMP '{cutoff}', retain_last => 5)"
    ).collect()
    orphans = spark.sql(f"CALL {ddl.CATALOG}.system.remove_orphan_files(table => '{table}')").collect()
    result = {
        "table": full,
        "rewritten_data_files": int(rewritten[0][0]) if rewritten else 0,
        "added_data_files": int(rewritten[0][1]) if rewritten and len(rewritten[0]) > 1 else 0,
        "expired_snapshots": len(expired),
        "orphan_files_removed": len(orphans),
    }
    log.info("maintenance %s", result)
    return result


def main(argv=None) -> int:
    args = parse_args(argv)
    configure_logging()
    settings = get_settings()
    spark = build_spark("rtdaas-maintenance", settings)
    tables = args.table or list(DEFAULT_TABLES)
    for table in tables:
        maintain(spark, table, args.older_than_days, args.target_file_size_mb)
    return 0


if __name__ == "__main__":
    sys.exit(main())
