"""Publish the curated marts (``marts.sql``) to Iceberg and refresh the data catalog (spec §5.5, §6).

Flow::

    lake.* ──Spark SQL (marts.sql)──▶ lake.marts.<table>
                                        day-partitioned marts: that day's partition replaced;
                                        unpartitioned ones: the table replaced
    rtdaas/serving/catalog.yml + row counts / max timestamps per table ──▶ lake.dq.catalog

The marts are one more Iceberg namespace, so every engine that reads the lake (Spark, DuckDB in
the API, PyIceberg, Trino, ...) reads them too: no separate warehouse, no credentials. (ADR 0012:
the original design copied them into Snowflake, with a DuckDB file as the stand-in.)

Run::

    spark-submit rtdaas/spark/publish_marts.py --date 2026-10-29 [--marts SCHEMA.TABLE,...]
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml

import rtdaas
from rtdaas.config import Settings, configure_logging, get_settings
from rtdaas.spark import ddl
from rtdaas.spark.common import build_spark, ensure_tables, now_utc, write_iceberg

log = logging.getLogger(__name__)

MARTS_SQL = Path(__file__).with_name("marts.sql")
CATALOG_YML = Path(rtdaas.__file__).parent / "serving" / "catalog.yml"
# horizontal whitespace only after the name: \s would let the optional partition column match
# the first word of the next line (e.g. SELECT) when a header has none
_MART_HEADER = re.compile(
    r"^--[ \t]*@mart[ \t]+([A-Za-z_]+)\.([A-Za-z_]+)(?:[ \t]+([A-Za-z_]+))?[ \t]*$", re.M
)

MARTS_NAMESPACE = "marts"


@dataclass(frozen=True)
class Mart:
    schema: str
    table: str
    partition_column: str | None
    sql: str

    @property
    def qualified(self) -> str:
        """``SCHEMA.TABLE`` as written in ``marts.sql`` (and accepted by ``--marts``)."""
        return f"{self.schema}.{self.table}"

    @property
    def iceberg_table(self) -> str:
        """``marts.<table>``: a namespace of its own, since e.g. ``DQ.CHECK_RESULTS`` would
        otherwise collide with its source, ``dq.check_results``."""
        return f"{MARTS_NAMESPACE}.{self.table.lower()}"


def load_marts(path: Path = MARTS_SQL, date: str = "", catalog: str = ddl.CATALOG) -> list[Mart]:
    """Split ``marts.sql`` into one :class:`Mart` per ``-- @mart`` block with placeholders substituted."""
    text = path.read_text(encoding="utf-8")
    marts: list[Mart] = []
    matches = list(_MART_HEADER.finditer(text))
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[m.end() : end]
        body = "\n".join(
            line for line in body.splitlines() if not line.strip().startswith("--")
        ).strip()
        body = body.format(date=date, catalog=catalog)
        marts.append(Mart(m.group(1).upper(), m.group(2).upper(), m.group(3), body))
    return marts


def build_mart(spark, mart: Mart):
    """The mart's rows (small: one row per robot/day, gesture, ...), in one partition."""
    return spark.sql(mart.sql).coalesce(1)


# ---------------------------------------------------------------------------
# Iceberg
# ---------------------------------------------------------------------------


def write_mart_iceberg(spark, mart: Mart, df) -> str:
    """Write ``df`` to ``lake.marts.<table>``; idempotent per day for day-partitioned marts.

    The query for a day-partitioned mart only returns rows of ``--date``, so a dynamic partition
    overwrite replaces exactly that day; a mart without a partition column is replaced whole.
    """
    from pyspark.sql import functions as F

    table = f"{ddl.CATALOG}.{mart.iceberg_table}"
    writer = df.writeTo(table)
    if not mart.partition_column:
        writer.createOrReplace()
    elif spark.catalog.tableExists(table):
        writer.overwritePartitions()
    else:
        writer.partitionedBy(F.col(mart.partition_column)).create()
    log.info("mart %s written to %s", mart.qualified, table)
    return table


# ---------------------------------------------------------------------------
# Catalog refresh
# ---------------------------------------------------------------------------


def catalog_entries(path: Path = CATALOG_YML) -> list[dict]:
    doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    default_owner = doc.get("owner_default", "data-platform@rtdaas")
    out = []
    for t in doc.get("tables") or []:
        out.append(
            {
                "name": t["name"],
                "owner": t.get("owner", default_owner),
                "description": t.get("description", ""),
                "sla_hours": float(t.get("sla_hours") or 0.0) or None,
                "freshness_column": t.get("freshness_column"),
            }
        )
    return out


def write_catalog(spark, settings: Settings | None = None, now: dt.datetime | None = None) -> int:
    """Rebuild ``lake.dq.catalog`` from ``catalog.yml`` plus live row counts and freshness."""
    from pyspark.sql import functions as F

    now = now or now_utc()
    rows = []
    for entry in catalog_entries():
        name = entry["name"]
        # the marts are not in ddl.TABLES (their schema comes from marts.sql): read them by name
        tdef = ddl.TABLES.get(name)
        row_count, last_updated = 0, None
        try:
            df = spark.table(tdef.qualified() if tdef else f"{ddl.CATALOG}.{name}")
            fresh_col = entry.get("freshness_column") or (tdef and tdef.freshness_column)
            aggs = [F.count(F.lit(1)).alias("n")]
            if fresh_col and fresh_col in df.columns:
                aggs.append(F.max(fresh_col).alias("last"))
            r = df.agg(*aggs).collect()[0]
            row_count = int(r["n"])
            last_updated = r["last"] if "last" in r.asDict() else None
        except Exception as exc:  # table missing in this environment
            log.warning("catalog: cannot read %s (%s)", name, exc)
        sla = entry["sla_hours"]
        if isinstance(last_updated, dt.date) and not isinstance(last_updated, dt.datetime):
            last_updated = dt.datetime.combine(last_updated, dt.time.min)  # a DATE column
        if last_updated is not None and last_updated.tzinfo is None:
            last_updated = last_updated.replace(tzinfo=dt.UTC)
        freshness_ok = True
        if sla and last_updated is not None:
            freshness_ok = (now - last_updated) <= dt.timedelta(hours=sla)
        elif sla and row_count == 0:
            freshness_ok = False
        rows.append(
            (name, entry["owner"], entry["description"], sla, row_count, last_updated, freshness_ok)
        )

    df = spark.createDataFrame(rows, schema=ddl.struct_type(ddl.CATALOG_TABLE.name))
    write_iceberg(df, ddl.CATALOG_TABLE.name, mode="replace")
    log.info("catalog refreshed: %d tables", len(rows))
    return len(rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--date", required=True, help="YYYY-MM-DD partition(s) to publish, comma-separated"
    )
    p.add_argument("--skip-catalog", action="store_true")
    p.add_argument("--marts", default=None, help="comma-separated SCHEMA.TABLE subset")
    return p.parse_args(argv)


def run(
    spark,
    settings: Settings,
    dates: list[str],
    only: set[str] | None = None,
    refresh_catalog: bool = True,
) -> dict:
    """Publish the marts for every date (a backfill passes its whole range), then the catalog."""
    ensure_tables(spark)
    tables: set[str] = set()
    for date in dates:
        for mart in load_marts(date=date):
            if not only or mart.qualified in only:
                tables.add(write_mart_iceberg(spark, mart, build_mart(spark, mart)))
    n_catalog = write_catalog(spark, settings) if refresh_catalog else 0
    return {"dates": dates, "marts": sorted(tables), "catalog_tables": n_catalog}


def main(argv=None) -> int:
    args = parse_args(argv)
    configure_logging()
    settings = get_settings()
    only = {s.strip().upper() for s in args.marts.split(",")} if args.marts else None
    dates = [d.strip() for d in args.date.split(",") if d.strip()]
    spark = build_spark(f"rtdaas-publish-marts-{dates[0]}", settings)
    result = run(spark, settings, dates, only, refresh_catalog=not args.skip_catalog)
    log.info("publish done: %s", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
