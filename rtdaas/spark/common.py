"""Shared Spark helpers: session construction, Iceberg writes, Elasticsearch bulk indexing.

``pyspark`` / ``elasticsearch`` are imported inside functions so that this module can be
imported by code paths that only need the pure-Python helpers (e.g. ``json_safe``).
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
import os
import uuid
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

from rtdaas.config import Settings, get_settings
from rtdaas.spark import ddl

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import Column, DataFrame, SparkSession

log = logging.getLogger(__name__)

ICEBERG_VERSION = "1.6.1"
SPARK_VERSION = "3.5.3"
HADOOP_AWS_VERSION = "3.3.4"
AWS_SDK_VERSION = "2.20.160"

SPARK_PACKAGES = (
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION}",
    f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}",
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
    f"org.apache.spark:spark-avro_2.12:{SPARK_VERSION}",
    f"org.apache.hadoop:hadoop-aws:{HADOOP_AWS_VERSION}",
    f"software.amazon.awssdk:bundle:{AWS_SDK_VERSION}",
)

ES_INDEX_EVENTS = "rtdaas-events"
ES_INDEX_KPI = "rtdaas-kpi-1m"
ES_INDEX_SESSIONS = "rtdaas-sessions"
ES_INDEX_GESTURES = "rtdaas-gestures"

_KW = {"type": "keyword"}
_DATE = {"type": "date"}
_DBL = {"type": "double"}
_LONG = {"type": "long"}

ES_MAPPINGS: dict[str, dict] = {
    ES_INDEX_EVENTS: {
        "properties": {
            "@timestamp": _DATE,
            "ts": _DATE,
            "recv_ts": _DATE,
            "robot_id": _KW,
            "arm": _KW,
            "severity": {"type": "integer"},
            "severity_name": _KW,
            "code": _KW,
            "message": {"type": "text"},
            "source": _KW,
            "bag_file": _KW,
        }
    },
    ES_INDEX_KPI: {
        "properties": {
            "@timestamp": _DATE,
            "window_start": _DATE,
            "window_end": _DATE,
            "computed_at": _DATE,
            "robot_id": _KW,
            "arm": _KW,
            "computed_by": _KW,
            "n_samples": _LONG,
            "mean_speed_mps": _DBL,
            "max_speed_mps": _DBL,
            "path_length_m": _DBL,
            "idle_fraction": _DBL,
            "jaw_cycles": _LONG,
            "warn_events": _LONG,
            "error_events": _LONG,
            "heartbeat_gap_s": _DBL,
        }
    },
    ES_INDEX_SESSIONS: {
        "properties": {
            "@timestamp": _DATE,
            "start_ts": _DATE,
            "end_ts": _DATE,
            "processed_at": _DATE,
            "session_id": _KW,
            "robot_id": _KW,
            "arm": _KW,
            "bag_file": _KW,
            "pipeline_version": _KW,
            "duration_s": _DBL,
            "n_samples": _LONG,
            "path_length_m": _DBL,
            "mean_speed_mps": _DBL,
            "idle_fraction": _DBL,
            "jaw_cycles": _LONG,
            "warn_events": _LONG,
            "error_events": _LONG,
            "instrument_swaps": _LONG,
        }
    },
    ES_INDEX_GESTURES: {
        "properties": {
            "trial_id": _KW,
            "task": _KW,
            "subject": _KW,
            "skill_level": _KW,
            "gesture": _KW,
            "start_frame": _LONG,
            "end_frame": _LONG,
            "duration_s": _DBL,
            "path_length_psml": _DBL,
            "path_length_psmr": _DBL,
            "mean_speed_psml": _DBL,
            "mean_speed_psmr": _DBL,
            "gripper_cycles": _LONG,
            "motion_energy_mean": _DBL,
            "n_frames": _LONG,
        }
    },
}


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


def spark_conf(settings: Settings) -> dict[str, str]:
    """All Spark configuration for the Iceberg REST catalog + S3A access (spec §4.8)."""
    cat = f"spark.sql.catalog.{ddl.CATALOG}"
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        cat: "org.apache.iceberg.spark.SparkCatalog",
        f"{cat}.type": "rest",
        f"{cat}.uri": settings.iceberg_rest_uri,
        f"{cat}.warehouse": settings.iceberg_warehouse,
        f"{cat}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"{cat}.s3.endpoint": settings.s3_endpoint,
        f"{cat}.s3.path-style-access": "true",
        f"{cat}.s3.access-key-id": settings.s3_access_key,
        f"{cat}.s3.secret-access-key": settings.s3_secret_key,
        f"{cat}.client.region": "us-east-1",
        "spark.hadoop.fs.s3a.endpoint": settings.s3_endpoint,
        "spark.hadoop.fs.s3a.access.key": settings.s3_access_key,
        "spark.hadoop.fs.s3a.secret.key": settings.s3_secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.shuffle.partitions": os.environ.get("RTDAAS_SHUFFLE_PARTITIONS", "24"),
        "spark.sql.adaptive.enabled": "true",
    }


def build_spark(app_name: str, settings: Settings | None = None) -> SparkSession:
    """Create (or get) the SparkSession configured for the `lake` Iceberg catalog."""
    from pyspark.sql import SparkSession

    settings = settings or get_settings()
    # S3FileIO's default credential chain also reads these; set them before the JVM starts.
    os.environ.setdefault("AWS_ACCESS_KEY_ID", settings.s3_access_key)
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", settings.s3_secret_key)
    os.environ.setdefault("AWS_REGION", "us-east-1")

    builder = SparkSession.builder.appName(app_name)
    if os.environ.get("RTDAAS_SPARK_PACKAGES", "1") != "0":
        # The rtdaas-spark image bakes these jars in and sets RTDAAS_SPARK_PACKAGES=0.
        builder = builder.config("spark.jars.packages", ",".join(SPARK_PACKAGES))
    for k, v in spark_conf(settings).items():
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    log.info(
        "SparkSession %s ready (catalog=%s, uri=%s)",
        app_name,
        ddl.CATALOG,
        settings.iceberg_rest_uri,
    )
    return spark


def ensure_tables(spark: SparkSession) -> None:
    """Create namespaces and every spec §3.2 table if missing (idempotent)."""
    for stmt in ddl.all_ddl():
        spark.sql(stmt)
    log.info("ensured %d Iceberg tables", len(ddl.TABLES))


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------


def conform(df: DataFrame, table: str) -> DataFrame:
    """Select the table's columns in order, adding missing ones as typed NULLs."""
    from pyspark.sql import functions as F

    tdef = ddl.get(table)
    present = set(df.columns)
    cols = []
    for c in tdef.columns:
        if c.name in present:
            cols.append(F.col(c.name).cast(c.type).alias(c.name))
        else:
            cols.append(F.lit(None).cast(c.type).alias(c.name))
    return df.select(*cols)


def write_iceberg(df: DataFrame, table: str, mode: str = "append") -> None:
    """Append or dynamically overwrite partitions of ``lake.<table>``."""
    writer = conform(df, table).writeTo(f"{ddl.CATALOG}.{table}")
    if mode == "append":
        writer.append()
    elif mode in ("overwrite_partitions", "overwritePartitions"):
        writer.overwritePartitions()
    elif mode == "replace":
        writer.createOrReplace()
    else:
        raise ValueError(f"unknown write mode {mode!r}")


def merge_iceberg(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    keys: list[str],
    matched_condition: str | None = None,
) -> None:
    """Upsert ``df`` into ``lake.<table>`` with ``MERGE INTO`` on ``keys``.

    The source is de-duplicated on ``keys`` first (Iceberg rejects a merge where one
    target row matches several source rows). ``matched_condition`` (SQL over aliases
    ``t`` / ``s``) restricts which matched rows are updated, e.g. ``"t.computed_by <> 'batch'"``.
    """
    src = conform(df, table).dropDuplicates(keys)
    view = f"rtdaas_src_{uuid.uuid4().hex}"
    src.createOrReplaceTempView(view)
    try:
        on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
        matched = "WHEN MATCHED"
        if matched_condition:
            matched += f" AND ({matched_condition})"
        spark.sql(
            f"MERGE INTO {ddl.CATALOG}.{table} t USING {view} s ON {on} "
            f"{matched} THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView(view)


def empty_df(spark: SparkSession, table: str) -> DataFrame:
    return spark.createDataFrame([], ddl.struct_type(table))


# ---------------------------------------------------------------------------
# Column helpers
# ---------------------------------------------------------------------------


def strip_wire_header(col: str = "value") -> Column:
    """Drop the 5-byte Confluent wire header (magic byte + int32 schema id) from a binary column."""
    from pyspark.sql import functions as F

    return F.expr(f"substring({col}, 6, length({col}) - 5)")


def _timestamp_micros(micros: Column) -> Column:
    from pyspark.sql import functions as F

    if hasattr(F, "timestamp_micros"):  # Spark >= 3.5
        return F.timestamp_micros(micros)
    return (micros.cast("double") / F.lit(1_000_000.0)).cast("timestamp")  # pragma: no cover


def ros_time_to_ts(sec_col: Column, nanosec_col: Column) -> Column:
    """``builtin_interfaces/Time`` (sec, nanosec) -> timestamp with microsecond precision."""
    from pyspark.sql import functions as F

    micros = sec_col.cast("long") * F.lit(1_000_000) + F.floor(
        nanosec_col.cast("long") / F.lit(1000)
    ).cast("long")
    return _timestamp_micros(micros)


def ns_to_ts(ns_col: Column) -> Column:
    """UNIX nanoseconds -> timestamp (microsecond precision)."""
    from pyspark.sql import functions as F

    return _timestamp_micros(F.floor(ns_col.cast("long") / F.lit(1000)).cast("long"))


def envelope_schema_json(topic: str, settings: Settings | None = None) -> str:
    """Fetch the latest Kafka record (envelope) schema for a topic from the registry."""
    from rtdaas.contracts.registry import RegistryClient, subject_for_topic

    settings = settings or get_settings()
    client = RegistryClient(settings.schema_registry_url)
    schema_id, version, schema = client.latest(subject_for_topic(topic))
    log.info("schema for %s: id=%s version=%s", topic, schema_id, version)
    return json.dumps(schema)


# ---------------------------------------------------------------------------
# Elasticsearch
# ---------------------------------------------------------------------------


def json_safe(value: Any) -> Any:
    """Convert datetimes / numpy / NaN into JSON-serialisable values (recursively)."""
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.UTC)
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, float):
        return None if (math.isnan(value) or math.isinf(value)) else value
    if hasattr(value, "item"):  # numpy scalar
        try:
            return json_safe(value.item())
        except Exception:  # pragma: no cover
            return str(value)
    if hasattr(value, "asDict"):  # pyspark Row
        return json_safe(value.asDict(recursive=True))
    return value


def es_client(settings: Settings | None = None):
    from elasticsearch import Elasticsearch

    settings = settings or get_settings()
    return Elasticsearch(settings.es_url, request_timeout=30)


def es_ensure_index(
    index: str, mappings: dict | None = None, settings: Settings | None = None
) -> None:
    """Create ``index`` with mappings if it does not exist."""
    es = es_client(settings)
    if not es.indices.exists(index=index):
        es.indices.create(index=index, mappings=mappings or ES_MAPPINGS.get(index, {}))
        log.info("created ES index %s", index)


def es_bulk(
    rows: Iterable[dict],
    index: str,
    id_fn: Callable[[dict], str],
    settings: Settings | None = None,
    ts_field: str | None = None,
) -> int:
    """Bulk-index ``rows`` (dicts) into ``index``; returns the number indexed."""
    from elasticsearch import helpers

    settings = settings or get_settings()
    es = es_client(settings)

    def actions():
        for row in rows:
            doc = json_safe(dict(row))
            if ts_field and ts_field in doc and "@timestamp" not in doc:
                doc["@timestamp"] = doc[ts_field]
            yield {"_op_type": "index", "_index": index, "_id": id_fn(row), "_source": doc}

    ok, errors = helpers.bulk(
        es, actions(), chunk_size=1000, raise_on_error=False, stats_only=False
    )
    if errors:
        log.warning("ES bulk to %s: %d errors (first: %s)", index, len(errors), errors[0])
    log.info("ES bulk to %s: %d docs", index, ok)
    return ok


def rows_as_dicts(df: DataFrame) -> list[dict]:
    """Collect a (small) DataFrame to a list of plain dicts."""
    return [r.asDict(recursive=True) for r in df.collect()]


def now_utc() -> dt.datetime:
    return dt.datetime.now(tz=dt.UTC)
