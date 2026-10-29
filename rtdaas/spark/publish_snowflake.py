"""Publish curated marts to Snowflake (or a DuckDB file) and refresh the data catalog (spec §5.5, §6).

Flow::

    lake.* ──Spark SQL (marts.sql)──▶ Parquet @ <stage>/<SCHEMA>.<TABLE>/<run_id>/
        ├─ --target snowflake : CREATE SCHEMA/TABLE IF NOT EXISTS, DELETE the date partition,
        │                       COPY INTO ... FILE_FORMAT=(TYPE=PARQUET) MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
        └─ --target duckdb    : CREATE OR REPLACE TABLE <schema>.<table> AS SELECT * FROM read_parquet(...)
                                (per-date partition replaced via DELETE + INSERT when the table exists)

    rtdaas/serving/catalog.yml + row counts / max timestamps per table ──▶ lake.dq.catalog

Credentials: ``SNOWFLAKE_*`` settings (spec §9). The stage is created with inline S3-compatible
credentials pointing at the lake bucket, which is what the MinIO-based compose stack needs;
on AWS use a storage integration + external stage instead (see docs/pipelines/publish.md).

Run::

    spark-submit rtdaas/spark/publish_snowflake.py --date 2026-10-29 --target snowflake|duckdb [--run-id X]
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import re
import sys
import uuid
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
_MART_HEADER = re.compile(r"^--\s*@mart\s+([A-Za-z_]+)\.([A-Za-z_]+)(?:\s+([A-Za-z_]+))?\s*$", re.M)

# Spark SQL simple type -> Snowflake / DuckDB type
_TYPE_MAP = {
    "string": "VARCHAR",
    "int": "INTEGER",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "long": "BIGINT",
    "double": "DOUBLE",
    "float": "FLOAT",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP_NTZ",
    "date": "DATE",
}


@dataclass(frozen=True)
class Mart:
    schema: str
    table: str
    partition_column: str | None
    sql: str

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"


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


def column_ddl(df) -> str:
    """``(col TYPE, ...)`` for CREATE TABLE from a Spark DataFrame schema."""
    parts = []
    for f in df.schema.fields:
        simple = f.dataType.simpleString()
        parts.append(f"{f.name} {_TYPE_MAP.get(simple, 'VARCHAR')}")
    return "(" + ", ".join(parts) + ")"


def stage_prefix(settings: Settings, mart: Mart, run_id: str) -> str:
    return f"{settings.snowflake_stage_uri.rstrip('/')}/{mart.qualified}/{run_id}/"


def export_mart(spark, settings: Settings, mart: Mart, run_id: str, coalesce: int = 1):
    """Run the mart query and write it as Parquet to the stage; returns the DataFrame."""
    df = spark.sql(mart.sql)
    if coalesce:
        df = df.coalesce(coalesce)
    uri = stage_prefix(settings, mart, run_id)
    df.write.mode("overwrite").parquet(uri)
    log.info("mart %s staged at %s", mart.qualified, uri)
    return df


# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------


def _s3_url(uri: str) -> str:
    return "s3://" + uri.split("://", 1)[1] if "://" in uri else uri


def load_snowflake(
    settings: Settings, marts: list[tuple[Mart, object]], run_id: str, date: str
) -> None:
    import snowflake.connector

    missing = [
        k
        for k in (
            "snowflake_account",
            "snowflake_user",
            "snowflake_password",
            "snowflake_warehouse",
        )
        if not getattr(settings, k)
    ]
    if missing:
        raise SystemExit(
            f"Snowflake target requires settings: {', '.join(m.upper() for m in missing)}"
        )

    conn = snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        role=settings.snowflake_role,
    )
    try:
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {settings.snowflake_database}")
        cur.execute(f"USE DATABASE {settings.snowflake_database}")
        for mart, df in marts:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {mart.schema}")
            cur.execute(f"CREATE TABLE IF NOT EXISTS {mart.qualified} {column_ddl(df)}")
            stage = f"{mart.schema}.RTDAAS_STAGE_{mart.table}"
            url = _s3_url(stage_prefix(settings, mart, run_id))
            # Inline credentials suit MinIO / dev. Production on AWS: STORAGE_INTEGRATION instead.
            cur.execute(
                f"CREATE OR REPLACE STAGE {stage} URL='{url}' "
                f"CREDENTIALS=(AWS_KEY_ID='{settings.s3_access_key}' AWS_SECRET_KEY='{settings.s3_secret_key}') "
                f"FILE_FORMAT=(TYPE=PARQUET)"
            )
            if mart.partition_column:
                cur.execute(
                    f"DELETE FROM {mart.qualified} WHERE {mart.partition_column} = %s", (date,)
                )
            else:
                cur.execute(f"TRUNCATE TABLE IF EXISTS {mart.qualified}")
            cur.execute(
                f"COPY INTO {mart.qualified} FROM @{stage} FILE_FORMAT=(TYPE=PARQUET) "
                f"MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE ON_ERROR=ABORT_STATEMENT"
            )
            log.info("Snowflake: loaded %s for %s", mart.qualified, date)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# DuckDB fallback
# ---------------------------------------------------------------------------


def _duckdb_type(simple: str) -> str:
    return {"timestamp": "TIMESTAMP"}.get(simple, _TYPE_MAP.get(simple, "VARCHAR"))


def load_duckdb(
    settings: Settings, marts: list[tuple[Mart, object]], run_id: str, date: str
) -> None:
    import duckdb

    db_path = Path(settings.serving_duckdb_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        for mart, df in marts:
            uri = stage_prefix(settings, mart, run_id)
            if uri.startswith("s3a://") or uri.startswith("s3://"):
                con.execute("INSTALL httpfs; LOAD httpfs;")
                endpoint = settings.s3_endpoint.split("://", 1)[-1]
                con.execute(f"SET s3_endpoint='{endpoint}'")
                con.execute(f"SET s3_access_key_id='{settings.s3_access_key}'")
                con.execute(f"SET s3_secret_access_key='{settings.s3_secret_key}'")
                con.execute("SET s3_url_style='path'")
                con.execute(
                    f"SET s3_use_ssl={'true' if settings.s3_endpoint.startswith('https') else 'false'}"
                )
                glob = "s3://" + uri.split("://", 1)[1] + "*.parquet"
            else:
                glob = uri.rstrip("/") + "/*.parquet"
            cols = ", ".join(
                f"{f.name} {_duckdb_type(f.dataType.simpleString())}" for f in df.schema.fields
            )
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {mart.schema}")
            con.execute(f"CREATE TABLE IF NOT EXISTS {mart.qualified} ({cols})")
            if mart.partition_column:
                con.execute(
                    f"DELETE FROM {mart.qualified} WHERE {mart.partition_column} = ?", [date]
                )
            else:
                con.execute(f"DELETE FROM {mart.qualified}")
            con.execute(
                f"INSERT INTO {mart.qualified} BY NAME SELECT * FROM read_parquet('{glob}')"
            )
            log.info("DuckDB: loaded %s for %s into %s", mart.qualified, date, db_path)
    finally:
        con.close()


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
        tdef = ddl.TABLES.get(name)
        row_count, last_updated = 0, None
        if tdef is not None:
            try:
                df = spark.table(tdef.qualified())
                fresh_col = entry.get("freshness_column") or tdef.freshness_column
                aggs = [F.count(F.lit(1)).alias("n")]
                if fresh_col and fresh_col in df.columns:
                    aggs.append(F.max(fresh_col).alias("last"))
                r = df.agg(*aggs).collect()[0]
                row_count = int(r["n"])
                last_updated = r["last"] if "last" in r.asDict() else None
            except Exception as exc:  # table missing in this environment
                log.warning("catalog: cannot read %s (%s)", name, exc)
        sla = entry["sla_hours"]
        if last_updated is not None and getattr(last_updated, "tzinfo", None) is None:
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
    p.add_argument("--date", required=True, help="YYYY-MM-DD partition to publish")
    p.add_argument(
        "--target",
        choices=("snowflake", "duckdb"),
        default=os.environ.get("RTDAAS_PUBLISH_TARGET", "duckdb"),
    )
    p.add_argument("--run-id", default=None)
    p.add_argument("--skip-catalog", action="store_true")
    p.add_argument("--marts", default=None, help="comma-separated SCHEMA.TABLE subset")
    return p.parse_args(argv)


def run(
    spark,
    settings: Settings,
    date: str,
    target: str,
    run_id: str | None = None,
    only: set[str] | None = None,
    refresh_catalog: bool = True,
) -> dict:
    ensure_tables(spark)
    run_id = run_id or uuid.uuid4().hex[:12]
    marts = [m for m in load_marts(date=date) if not only or m.qualified in only]
    staged = [(m, export_mart(spark, settings, m, run_id)) for m in marts]
    if target == "snowflake":
        load_snowflake(settings, staged, run_id, date)
    else:
        load_duckdb(settings, staged, run_id, date)
    n_catalog = write_catalog(spark, settings) if refresh_catalog else 0
    return {"run_id": run_id, "marts": [m.qualified for m in marts], "catalog_tables": n_catalog}


def main(argv=None) -> int:
    args = parse_args(argv)
    configure_logging()
    settings = get_settings()
    only = {s.strip().upper() for s in args.marts.split(",")} if args.marts else None
    spark = build_spark(f"rtdaas-publish-{args.date}", settings)
    result = run(
        spark,
        settings,
        args.date,
        args.target,
        args.run_id,
        only,
        refresh_catalog=not args.skip_catalog,
    )
    log.info("publish done: %s", result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
