"""Batch job: MCAP bags for one day -> raw tables, sessions and exact 1-minute KPIs (spec §5.2).

Flow::

    s3a://rtdaas-raw/bags/<robot>/<yyyy>/<mm>/<dd>/*.mcap
      -> sc.binaryFiles -> mapPartitions(decode_bag)        (pure Python, tested)
      -> 4 raw DataFrames -> MERGE INTO telemetry.*_raw      (bag rows win over stream rows)
      -> sessions spine (split_sessions on the driver per robot)
      -> tool_pose joined to spine -> applyInPandas(session_metrics) -> telemetry.sessions
      -> applyInPandas(kpi_windows) -> telemetry.kpi_1m (computed_by='batch', batch wins)
      -> Elasticsearch rtdaas-sessions

Assumption: the message definitions directory (``contracts/msgs``) is available on the
executors at the path derived from the installed ``rtdaas`` package (the ``rtdaas-spark``
image copies the whole repository to ``/opt/rtdaas``), or at ``$RTDAAS_MSGS_DIR``. Bags
are decoded primarily with the definitions embedded in the bag; the repo registry is
the fallback.

Run::

    spark-submit rtdaas/spark/batch_mcap.py --date 2026-10-29 [--robot dv_001] [--input s3a://rtdaas-raw/bags]
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import posixpath
import sys
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING

import pandas as pd

from rtdaas.config import DEFAULT_MSGS_DIR, Settings, configure_logging, get_settings
from rtdaas.spark import ddl
from rtdaas.spark.common import (
    ES_INDEX_EVENTS,
    ES_INDEX_KPI,
    ES_INDEX_SESSIONS,
    build_spark,
    ensure_tables,
    es_bulk,
    es_ensure_index,
    es_event_id,
    es_kpi_id,
    merge_iceberg,
    rows_as_dicts,
)
from rtdaas.spark.decode import decode_bag

if TYPE_CHECKING:  # pyspark is imported lazily at runtime (see build_spark)
    from pyspark.sql import DataFrame

log = logging.getLogger(__name__)

RAW_TABLE_NAMES = {short: tdef.name for short, tdef in ddl.RAW_TABLES.items()}


# ---------------------------------------------------------------------------
# Decoding (runs on executors)
# ---------------------------------------------------------------------------


def _msgs_dir() -> str:
    return os.environ.get("RTDAAS_MSGS_DIR", str(DEFAULT_MSGS_DIR))


def decode_partition(files: Iterable[tuple[str, bytes]]) -> Iterator[tuple[str, tuple]]:
    """``(path, bytes)`` pairs -> ``(table_short_name, row_tuple)`` pairs.

    Builds the fallback codec once per partition; row dicts are converted to tuples in
    the table's column order so ``createDataFrame`` can use an explicit schema.
    """
    from rtdaas.contracts.cdr import CdrCodec
    from rtdaas.contracts.msgspec import MsgRegistry

    codec = CdrCodec(MsgRegistry.load_dir(_msgs_dir()))
    for path, content in files:
        bag_file = posixpath.basename(path)
        try:
            tables = decode_bag(bytes(content), codec, bag_file)
        except Exception as exc:  # one bad bag must not fail the day's job
            log.error("failed to decode %s: %s", path, exc)
            continue
        for short, rows in tables.items():
            tdef = ddl.RAW_TABLES[short]
            for row in rows:
                yield short, ddl.to_tuple(tdef, row)


def bag_glob(input_uri: str, date: dt.date, robot: str | None) -> str:
    return f"{input_uri.rstrip('/')}/{robot or '*'}/{date:%Y}/{date:%m}/{date:%d}/*.mcap"


def load_raw(spark, pattern: str, min_partitions: int = 8) -> dict[str, DataFrame]:
    """Decode every bag matching ``pattern`` into one DataFrame per raw table."""
    decoded = spark.sparkContext.binaryFiles(pattern, minPartitions=min_partitions).mapPartitions(
        decode_partition
    )
    decoded = decoded.persist()
    frames = {}
    for short, tdef in ddl.RAW_TABLES.items():
        rows = decoded.filter(lambda kv, s=short: kv[0] == s).map(lambda kv: kv[1])
        frames[short] = spark.createDataFrame(rows, schema=ddl.struct_type(tdef.name))
    return frames


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------


def _to_utc_pdf(pdf: pd.DataFrame, cols: Iterable[str] = ("ts",)) -> pd.DataFrame:
    for c in cols:
        if c in pdf and len(pdf) and getattr(pdf[c].dt, "tz", None) is None:
            pdf[c] = pdf[c].dt.tz_localize("UTC")
    return pdf


def build_session_spine(spark, pose, events, heartbeat):
    """One row per (robot_id, session): ``robot_id, session_id, start_ts, end_ts``.

    Session boundaries come from :func:`rtdaas.metrics.kinematics.split_sessions`, run on
    the driver per robot (events + heartbeat are small).
    """
    from pyspark.sql import functions as F

    from rtdaas.metrics.kinematics import session_id, split_sessions

    spans = pose.groupBy("robot_id").agg(F.min("ts").alias("t0"), F.max("ts").alias("t1")).collect()
    ev_pdf = _to_utc_pdf(events.select("robot_id", "ts", "severity", "code").toPandas())
    hb_pdf = _to_utc_pdf(heartbeat.select("robot_id", "ts").toPandas())

    rows = []
    for r in spans:
        robot = r["robot_id"]
        span = (pd.Timestamp(r["t0"]).tz_localize("UTC"), pd.Timestamp(r["t1"]).tz_localize("UTC"))
        ev = ev_pdf[ev_pdf["robot_id"] == robot]
        hb = hb_pdf[hb_pdf["robot_id"] == robot]
        for start, end in split_sessions(ev, hb if len(hb) else None, span):
            rows.append(
                (robot, session_id(robot, start), start.to_pydatetime(), end.to_pydatetime())
            )
    log.info("session spine: %d sessions across %d robots", len(rows), len(spans))
    schema = "robot_id string, session_id string, start_ts timestamp, end_ts timestamp"
    return spark.createDataFrame(rows, schema=schema)


def _session_metrics_udf(key, pdf: pd.DataFrame) -> pd.DataFrame:
    from rtdaas.metrics.kinematics import session_metrics

    session, robot, arm = key
    pdf = _to_utc_pdf(pdf)
    m = session_metrics(pdf[["ts", "x", "y", "z", "jaw_angle"]], events=None)
    pdf_sorted = pdf.sort_values("ts")
    return pd.DataFrame(
        [
            {
                "session_id": session,
                "robot_id": robot,
                "arm": arm,
                "start_ts": pdf_sorted["ts"].iloc[0],
                "end_ts": pdf_sorted["ts"].iloc[-1],
                "duration_s": float(m["duration_s"]),
                "n_samples": int(m["n_samples"]),
                "path_length_m": float(m["path_length_m"]),
                "mean_speed_mps": float(m["mean_speed_mps"]),
                "idle_fraction": float(m["idle_fraction"]),
                "jaw_cycles": int(m["jaw_cycles"]),
                "bag_file": pdf_sorted["bag_file"].dropna().iloc[0]
                if pdf_sorted["bag_file"].notna().any()
                else None,
            }
        ]
    )


SESSION_UDF_SCHEMA = (
    "session_id string, robot_id string, arm string, start_ts timestamp, end_ts timestamp, "
    "duration_s double, n_samples bigint, path_length_m double, mean_speed_mps double, "
    "idle_fraction double, jaw_cycles bigint, bag_file string"
)


def _kpi_udf(key, pdf: pd.DataFrame) -> pd.DataFrame:
    from rtdaas.metrics.kinematics import kpi_windows

    session, robot, arm = key
    pdf = _to_utc_pdf(pdf)
    out = kpi_windows(
        pdf[["ts", "x", "y", "z", "jaw_angle"]], events=None, heartbeat=None, window="1min"
    )
    out.insert(0, "arm", arm)
    out.insert(0, "robot_id", robot)
    return out[
        [
            "robot_id",
            "arm",
            "window_start",
            "window_end",
            "n_samples",
            "mean_speed_mps",
            "max_speed_mps",
            "path_length_m",
            "idle_fraction",
            "jaw_cycles",
        ]
    ]


KPI_UDF_SCHEMA = (
    "robot_id string, arm string, window_start timestamp, window_end timestamp, n_samples bigint, "
    "mean_speed_mps double, max_speed_mps double, path_length_m double, idle_fraction double, jaw_cycles bigint"
)


def _event_counts(events, spine, by_arm: bool):
    """Event counts per session (and arm when ``by_arm``); robot-wide events have ``arm = ''``."""
    from pyspark.sql import functions as F

    ev = events.join(
        spine,
        (events.robot_id == spine.robot_id) & events.ts.between(spine.start_ts, spine.end_ts),
        "inner",
    ).select(spine.session_id, events.arm, events.severity, events.code)
    ev = ev.where(F.col("arm") != "") if by_arm else ev.where(F.col("arm") == "")
    keys = ["session_id", "arm"] if by_arm else ["session_id"]
    return ev.groupBy(*keys).agg(
        F.sum(F.when(F.col("severity") == 2, 1).otherwise(0)).cast("bigint").alias("warn_events"),
        F.sum(F.when(F.col("severity") >= 3, 1).otherwise(0)).cast("bigint").alias("error_events"),
        F.sum(F.when(F.col("code") == "INSTRUMENT_SWAP", 1).otherwise(0))
        .cast("bigint")
        .alias("instrument_swaps"),
    )


def build_sessions(spark, pose_in_session, events, spine, settings: Settings):
    """``telemetry.sessions`` rows for the day (pose metrics via pandas UDF + event counts)."""
    from pyspark.sql import functions as F

    base = pose_in_session.groupBy("session_id", "robot_id", "arm").applyInPandas(
        _session_metrics_udf, schema=SESSION_UDF_SCHEMA
    )
    arm_ev = _event_counts(events, spine, by_arm=True)
    sys_ev = _event_counts(events, spine, by_arm=False).select(
        "session_id",
        F.col("warn_events").alias("s_warn"),
        F.col("error_events").alias("s_err"),
        F.col("instrument_swaps").alias("s_swaps"),
    )
    out = base.join(arm_ev, ["session_id", "arm"], "left").join(sys_ev, ["session_id"], "left")
    return out.select(
        *[c for c in base.columns if c != "bag_file"],
        (F.coalesce("warn_events", F.lit(0)) + F.coalesce("s_warn", F.lit(0)))
        .cast("bigint")
        .alias("warn_events"),
        (F.coalesce("error_events", F.lit(0)) + F.coalesce("s_err", F.lit(0)))
        .cast("bigint")
        .alias("error_events"),
        (F.coalesce("instrument_swaps", F.lit(0)) + F.coalesce("s_swaps", F.lit(0)))
        .cast("bigint")
        .alias("instrument_swaps"),
        "bag_file",
        F.lit(settings.pipeline_version).alias("pipeline_version"),
        F.current_timestamp().alias("processed_at"),
    )


def build_kpis(pose_in_session, events, heartbeat):
    """Exact 1-minute KPIs per robot x arm (``computed_by = 'batch'``)."""
    from pyspark.sql import functions as F

    kpi = pose_in_session.groupBy("session_id", "robot_id", "arm").applyInPandas(
        _kpi_udf, schema=KPI_UDF_SCHEMA
    )
    win = F.date_trunc("minute", F.col("ts")).alias("window_start")

    arm_ev = (
        events.where(F.col("arm") != "")
        .groupBy("robot_id", "arm", win)
        .agg(
            F.sum(F.when(F.col("severity") == 2, 1).otherwise(0)).cast("bigint").alias("a_warn"),
            F.sum(F.when(F.col("severity") >= 3, 1).otherwise(0)).cast("bigint").alias("a_err"),
        )
    )
    sys_ev = (
        events.where(F.col("arm") == "")
        .groupBy("robot_id", win)
        .agg(
            F.sum(F.when(F.col("severity") == 2, 1).otherwise(0)).cast("bigint").alias("s_warn"),
            F.sum(F.when(F.col("severity") >= 3, 1).otherwise(0)).cast("bigint").alias("s_err"),
        )
    )
    hb = (
        heartbeat.groupBy("robot_id", win)
        .agg(F.count("ts").alias("n_hb"), F.min("ts").alias("hb_min"), F.max("ts").alias("hb_max"))
        .select(
            "robot_id",
            "window_start",
            F.when(
                F.col("n_hb") > 1,
                (F.col("hb_max").cast("double") - F.col("hb_min").cast("double"))
                / (F.col("n_hb") - 1),
            ).alias("heartbeat_gap_s"),
        )
    )
    out = (
        kpi.join(arm_ev, ["robot_id", "arm", "window_start"], "left")
        .join(sys_ev, ["robot_id", "window_start"], "left")
        .join(hb, ["robot_id", "window_start"], "left")
    )
    return out.select(
        "robot_id",
        "arm",
        "window_start",
        "window_end",
        "n_samples",
        "mean_speed_mps",
        "max_speed_mps",
        "path_length_m",
        "idle_fraction",
        "jaw_cycles",
        (F.coalesce("a_warn", F.lit(0)) + F.coalesce("s_warn", F.lit(0)))
        .cast("bigint")
        .alias("warn_events"),
        (F.coalesce("a_err", F.lit(0)) + F.coalesce("s_err", F.lit(0)))
        .cast("bigint")
        .alias("error_events"),
        "heartbeat_gap_s",
        F.lit("batch").alias("computed_by"),
        F.current_timestamp().alias("computed_at"),
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--date", required=True, help="YYYY-MM-DD (UTC day of the bag landing path)")
    p.add_argument("--robot", default=None, help="restrict to one robot id")
    p.add_argument("--input", default=None, help="bags base URI (default: s3a://<raw bucket>/bags)")
    p.add_argument(
        "--skip-es", action="store_true", help="do not index sessions into Elasticsearch"
    )
    return p.parse_args(argv)


def run(
    spark,
    settings: Settings,
    date: dt.date,
    robot: str | None,
    input_uri: str,
    skip_es: bool = False,
) -> dict:
    ensure_tables(spark)
    pattern = bag_glob(input_uri, date, robot)
    log.info("reading bags: %s", pattern)
    frames = load_raw(spark, pattern)
    for df in frames.values():
        df.persist()

    counts = {}
    for short, df in frames.items():
        n = df.count()
        counts[short] = n
        # the stream appends to the same tables: bound the merge to the bags' days
        merge_iceberg(
            spark, df, RAW_TABLE_NAMES[short], ddl.RAW_MERGE_KEYS[short], time_column="ts"
        )
    log.info("raw rows merged: %s", counts)

    pose, events, hb = frames["tool_pose"], frames["events"], frames["heartbeat"]
    # The API's event search and the dashboards read Elasticsearch, where until now only the
    # stream wrote events: bag-only events were invisible there. Same ids as the stream (see
    # es_event_id), so an event both paths saw ends up as one document.
    if not skip_es and counts["events"]:
        es_ensure_index(ES_INDEX_EVENTS, settings=settings)
        es_bulk(
            rows_as_dicts(events),
            ES_INDEX_EVENTS,
            id_fn=es_event_id,
            settings=settings,
            ts_field="ts",
        )
    if counts["tool_pose"] == 0:
        log.warning("no tool_pose rows for %s; skipping sessions/KPIs", date)
        return {**counts, "sessions": 0, "kpi_1m": 0}

    spine = build_session_spine(spark, pose, events, hb).persist()
    pose_in_session = (
        pose.join(
            spine,
            (pose.robot_id == spine.robot_id) & pose.ts.between(spine.start_ts, spine.end_ts),
            "inner",
        )
        .select(spine.session_id, pose["*"])
        .persist()
    )

    sessions = build_sessions(spark, pose_in_session, events, spine, settings).persist()
    n_sessions = sessions.count()
    merge_iceberg(spark, sessions, "telemetry.sessions", ["session_id", "arm"])

    kpis = build_kpis(pose_in_session, events, hb)
    merge_iceberg(
        spark,
        kpis,
        "telemetry.kpi_1m",
        ["robot_id", "arm", "window_start"],
        time_column="window_start",
    )
    n_kpi = kpis.count()
    log.info("sessions=%d kpi windows=%d", n_sessions, n_kpi)

    if not skip_es and n_kpi:
        # exact windows replace the stream's approximations in the dashboards too, as in Iceberg
        # (there the MERGE also keeps a later stream write from undoing them; ES has no such
        # guard, which only matters for data that arrives on the stream after its day's batch)
        es_ensure_index(ES_INDEX_KPI, settings=settings)
        es_bulk(
            rows_as_dicts(kpis),
            ES_INDEX_KPI,
            id_fn=es_kpi_id,
            settings=settings,
            ts_field="window_start",
        )
    if not skip_es and n_sessions:
        es_ensure_index(ES_INDEX_SESSIONS, settings=settings)
        es_bulk(
            rows_as_dicts(sessions),
            ES_INDEX_SESSIONS,
            id_fn=lambda r: f"{r['session_id']}|{r['arm']}",
            settings=settings,
            ts_field="start_ts",
        )
    return {**counts, "sessions": n_sessions, "kpi_1m": n_kpi}


def main(argv=None) -> int:
    args = parse_args(argv)
    configure_logging()
    settings = get_settings()
    date = dt.date.fromisoformat(args.date)
    spark = build_spark(f"rtdaas-batch-mcap-{date}", settings)
    result = run(
        spark, settings, date, args.robot, args.input or settings.raw_bags_uri, args.skip_es
    )
    log.info("batch_mcap %s done: %s", date, result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
