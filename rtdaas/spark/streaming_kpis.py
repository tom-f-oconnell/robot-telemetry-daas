"""Structured Streaming job: Kafka telemetry -> raw Iceberg tables + 1-minute KPIs (spec §5.1).

Two streaming queries share one Kafka source definition:

* ``raw``  — appends every ``tool_pose`` / ``events`` / ``heartbeat`` record to the raw
  Iceberg tables (``source = 'stream'``) and indexes events into Elasticsearch.
* ``kpi``  — a single stateful aggregation over the union of the three parsed streams,
  keyed by ``(robot_id, arm, 1-minute window)``. Robot-level signals (heartbeat, events
  with ``arm = ''``) are aggregated under the pseudo-arm ``"*"`` and folded into each
  arm's row inside ``foreachBatch`` (a tiny join on the micro-batch), which avoids any
  stream-stream join.

Approximations in the streaming KPIs (the batch job recomputes all of them exactly):

* ``path_length_m`` ≈ ``avg(speed) * (max(ts) - min(ts))`` — the stream carries the
  adapter's instantaneous speed but no ordering guarantee inside the aggregation, so
  the exact sum of step distances is left to the batch job.
* ``jaw_cycles`` = 0 — a threshold crossing needs ordered samples.
* ``heartbeat_gap_s`` ≈ ``(max(hb_ts) - min(hb_ts)) / (n_hb - 1)`` — mean gap, not max gap.

Stream rows are written with ``computed_by = 'stream'`` and never overwrite rows the
batch job already wrote (``computed_by = 'batch'``).

Run::

    spark-submit rtdaas/spark/streaming_kpis.py [--trigger "30 seconds"] [--starting-offsets latest]
"""

from __future__ import annotations

import argparse
import logging
import sys

from rtdaas.config import Settings, configure_logging, get_settings
from rtdaas.spark.common import (
    ES_INDEX_EVENTS,
    ES_INDEX_KPI,
    build_spark,
    ensure_tables,
    envelope_schema_json,
    es_bulk,
    es_ensure_index,
    merge_iceberg,
    ns_to_ts,
    ros_time_to_ts,
    rows_as_dicts,
    strip_wire_header,
    write_iceberg,
)

log = logging.getLogger(__name__)

TOPIC_TOOL_POSE = "rtdaas.telemetry.tool_pose"
TOPIC_EVENTS = "rtdaas.telemetry.events"
TOPIC_HEARTBEAT = "rtdaas.telemetry.heartbeat"
TOPICS = (TOPIC_TOOL_POSE, TOPIC_EVENTS, TOPIC_HEARTBEAT)

ROBOT_WIDE = "*"
IDLE_SPEED_MPS = 0.005
WATERMARK = "30 seconds"
WINDOW = "1 minute"


# ---------------------------------------------------------------------------
# Source + parsing
# ---------------------------------------------------------------------------


def read_kafka(spark, settings: Settings, starting_offsets: str):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
        .option("subscribe", ",".join(TOPICS))
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "500000")
        .load()
    )


def _decode(df, topic: str, schema_json: str):
    """Filter one topic and decode the Confluent-framed Avro envelope into column ``r``."""
    from pyspark.sql import functions as F
    from pyspark.sql.avro.functions import from_avro

    return (
        df.where(F.col("topic") == topic)
        .select(
            from_avro(strip_wire_header("value"), schema_json, {"mode": "PERMISSIVE"}).alias("r")
        )
        .where(F.col("r").isNotNull())
    )


def build_pose_stream(df, schema_json: str):
    """Kafka rows for ``tool_pose`` -> ``telemetry.tool_pose_raw`` columns."""
    from pyspark.sql import functions as F

    r = _decode(df, TOPIC_TOOL_POSE, schema_json)
    return r.select(
        F.col("r.robot_id").alias("robot_id"),
        F.coalesce(F.col("r.msg.arm"), F.element_at(F.split(F.col("r.ros_topic"), "/"), 3)).alias(
            "arm"
        ),
        ros_time_to_ts(F.col("r.msg.header.stamp.sec"), F.col("r.msg.header.stamp.nanosec")).alias(
            "ts"
        ),
        ns_to_ts(F.col("r.recv_time_ns")).alias("recv_ts"),
        F.col("r.msg.position.x").alias("x"),
        F.col("r.msg.position.y").alias("y"),
        F.col("r.msg.position.z").alias("z"),
        F.col("r.msg.orientation.x").alias("qx"),
        F.col("r.msg.orientation.y").alias("qy"),
        F.col("r.msg.orientation.z").alias("qz"),
        F.col("r.msg.orientation.w").alias("qw"),
        F.col("r.msg.jaw_angle").alias("jaw_angle"),
        F.col("r.msg.speed").alias("speed"),
        F.lit("stream").alias("source"),
        F.lit(None).cast("string").alias("bag_file"),
    ).where(F.col("robot_id").isNotNull() & F.col("ts").isNotNull())


def build_event_stream(df, schema_json: str):
    """Kafka rows for ``events`` -> ``telemetry.events_raw`` columns."""
    from pyspark.sql import functions as F

    r = _decode(df, TOPIC_EVENTS, schema_json)
    severity = F.col("r.msg.severity").cast("int")
    names = F.create_map(
        F.lit(0),
        F.lit("DEBUG"),
        F.lit(1),
        F.lit("INFO"),
        F.lit(2),
        F.lit("WARN"),
        F.lit(3),
        F.lit("ERROR"),
        F.lit(4),
        F.lit("FATAL"),
    )
    return r.select(
        F.col("r.robot_id").alias("robot_id"),
        F.coalesce(F.col("r.msg.arm"), F.lit("")).alias("arm"),
        ros_time_to_ts(F.col("r.msg.header.stamp.sec"), F.col("r.msg.header.stamp.nanosec")).alias(
            "ts"
        ),
        ns_to_ts(F.col("r.recv_time_ns")).alias("recv_ts"),
        severity.alias("severity"),
        F.element_at(names, severity).alias("severity_name"),
        F.col("r.msg.code").alias("code"),
        F.col("r.msg.message").alias("message"),
        F.lit("stream").alias("source"),
        F.lit(None).cast("string").alias("bag_file"),
    ).where(F.col("robot_id").isNotNull() & F.col("ts").isNotNull())


def build_heartbeat_stream(df, schema_json: str):
    """Kafka rows for ``heartbeat`` -> ``telemetry.heartbeat_raw`` columns."""
    from pyspark.sql import functions as F

    r = _decode(df, TOPIC_HEARTBEAT, schema_json)
    return r.select(
        F.coalesce(F.col("r.msg.robot_id"), F.col("r.robot_id")).alias("robot_id"),
        ros_time_to_ts(F.col("r.msg.header.stamp.sec"), F.col("r.msg.header.stamp.nanosec")).alias(
            "ts"
        ),
        ns_to_ts(F.col("r.recv_time_ns")).alias("recv_ts"),
        F.col("r.msg.uptime_s").alias("uptime_s"),
        F.col("r.msg.state").alias("state"),
        F.col("r.msg.active_arms").alias("active_arms"),
        F.col("r.msg.software_version").alias("software_version"),
        F.lit("stream").alias("source"),
        F.lit(None).cast("string").alias("bag_file"),
    ).where(F.col("robot_id").isNotNull() & F.col("ts").isNotNull())


# ---------------------------------------------------------------------------
# KPI aggregation
# ---------------------------------------------------------------------------


def unify(pose, events, heartbeat):
    """Union the three parsed streams into one narrow frame for a single aggregation.

    Columns: robot_id, arm, ts, speed, is_pose, is_warn, is_error, hb_ts.
    Robot-level rows (heartbeat, events with empty arm) use ``arm = "*"``.
    """
    from pyspark.sql import functions as F

    p = pose.select(
        "robot_id",
        "arm",
        "ts",
        F.col("speed").cast("double").alias("speed"),
        F.lit(1).alias("is_pose"),
        F.lit(0).alias("is_warn"),
        F.lit(0).alias("is_error"),
        F.lit(None).cast("timestamp").alias("hb_ts"),
    )
    e = events.select(
        "robot_id",
        F.when(F.col("arm") == "", F.lit(ROBOT_WIDE)).otherwise(F.col("arm")).alias("arm"),
        "ts",
        F.lit(None).cast("double").alias("speed"),
        F.lit(0).alias("is_pose"),
        F.when(F.col("severity") == 2, 1).otherwise(0).alias("is_warn"),
        F.when(F.col("severity") >= 3, 1).otherwise(0).alias("is_error"),
        F.lit(None).cast("timestamp").alias("hb_ts"),
    )
    h = heartbeat.select(
        "robot_id",
        F.lit(ROBOT_WIDE).alias("arm"),
        "ts",
        F.lit(None).cast("double").alias("speed"),
        F.lit(0).alias("is_pose"),
        F.lit(0).alias("is_warn"),
        F.lit(0).alias("is_error"),
        F.col("ts").alias("hb_ts"),
    )
    return p.unionByName(e).unionByName(h)


def kpi_aggregate(unified, watermark: str = WATERMARK, window: str = WINDOW):
    """1-minute tumbling-window aggregation per (robot_id, arm). Works on static frames too."""
    from pyspark.sql import functions as F

    df = unified.withWatermark("ts", watermark) if unified.isStreaming else unified
    agg = df.groupBy(F.window("ts", window).alias("w"), "robot_id", "arm").agg(
        F.sum("is_pose").cast("bigint").alias("n_samples"),
        F.avg("speed").alias("mean_speed_mps"),
        F.max("speed").alias("max_speed_mps"),
        F.min(F.when(F.col("is_pose") == 1, F.col("ts"))).alias("pose_min_ts"),
        F.max(F.when(F.col("is_pose") == 1, F.col("ts"))).alias("pose_max_ts"),
        F.avg(
            F.when(
                F.col("is_pose") == 1, F.when(F.col("speed") < IDLE_SPEED_MPS, 1.0).otherwise(0.0)
            )
        ).alias("idle_fraction"),
        F.sum("is_warn").cast("bigint").alias("warn_events"),
        F.sum("is_error").cast("bigint").alias("error_events"),
        F.count("hb_ts").alias("n_hb"),
        F.min("hb_ts").alias("hb_min"),
        F.max("hb_ts").alias("hb_max"),
    )
    span_s = F.col("pose_max_ts").cast("double") - F.col("pose_min_ts").cast("double")
    hb_gap = F.when(
        F.col("n_hb") > 1,
        (F.col("hb_max").cast("double") - F.col("hb_min").cast("double")) / (F.col("n_hb") - 1),
    )
    return agg.select(
        "robot_id",
        "arm",
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "n_samples",
        F.coalesce("mean_speed_mps", F.lit(0.0)).alias("mean_speed_mps"),
        F.coalesce("max_speed_mps", F.lit(0.0)).alias("max_speed_mps"),
        F.coalesce(F.col("mean_speed_mps") * span_s, F.lit(0.0)).alias("path_length_m"),
        F.coalesce("idle_fraction", F.lit(0.0)).alias("idle_fraction"),
        F.lit(0).cast("bigint").alias("jaw_cycles"),
        "warn_events",
        "error_events",
        hb_gap.alias("heartbeat_gap_s"),
    )


def fold_robot_wide(kpi):
    """Fold ``arm = "*"`` rows (robot-level counts, heartbeat gap) into each arm's row.

    Operates on a static (micro-batch) frame. Rows for ``"*"`` are dropped afterwards.
    """
    from pyspark.sql import functions as F

    wide = kpi.where(F.col("arm") == ROBOT_WIDE).select(
        "robot_id",
        "window_start",
        F.col("warn_events").alias("w_warn"),
        F.col("error_events").alias("w_err"),
        F.col("heartbeat_gap_s").alias("w_gap"),
    )
    arms = kpi.where(F.col("arm") != ROBOT_WIDE)
    joined = arms.join(wide, ["robot_id", "window_start"], "left")
    return joined.select(
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
        (F.col("warn_events") + F.coalesce("w_warn", F.lit(0))).cast("bigint").alias("warn_events"),
        (F.col("error_events") + F.coalesce("w_err", F.lit(0)))
        .cast("bigint")
        .alias("error_events"),
        F.coalesce("heartbeat_gap_s", "w_gap").alias("heartbeat_gap_s"),
    )


# ---------------------------------------------------------------------------
# Sinks (foreachBatch)
# ---------------------------------------------------------------------------


def make_raw_sink(spark, settings: Settings, schemas: dict[str, str]):
    def sink_raw(batch_df, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        batch_df = batch_df.persist()
        try:
            pose = build_pose_stream(batch_df, schemas[TOPIC_TOOL_POSE])
            events = build_event_stream(batch_df, schemas[TOPIC_EVENTS]).persist()
            hb = build_heartbeat_stream(batch_df, schemas[TOPIC_HEARTBEAT])
            write_iceberg(pose, "telemetry.tool_pose_raw")
            write_iceberg(events, "telemetry.events_raw")
            write_iceberg(hb, "telemetry.heartbeat_raw")
            ev_rows = rows_as_dicts(events)  # low volume
            if ev_rows:
                es_bulk(
                    ev_rows,
                    ES_INDEX_EVENTS,
                    id_fn=lambda r: f"{r['robot_id']}|{r['ts'].isoformat()}|{r['code']}",
                    settings=settings,
                    ts_field="ts",
                )
            log.info("raw batch %d: events=%d", batch_id, len(ev_rows))
            events.unpersist()
        finally:
            batch_df.unpersist()

    return sink_raw


def make_kpi_sink(spark, settings: Settings):
    from pyspark.sql import functions as F

    def sink_kpi(batch_df, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        rows = (
            fold_robot_wide(batch_df)
            .withColumn("computed_by", F.lit("stream"))
            .withColumn("computed_at", F.current_timestamp())
        ).persist()
        try:
            merge_iceberg(
                spark,
                rows,
                "telemetry.kpi_1m",
                ["robot_id", "arm", "window_start"],
                matched_condition="t.computed_by <> 'batch'",
                time_column="window_start",  # batch merges the same table
            )
            dicts = rows_as_dicts(rows)
            es_bulk(
                dicts,
                ES_INDEX_KPI,
                id_fn=lambda r: f"{r['robot_id']}|{r['arm']}|{r['window_start'].isoformat()}",
                settings=settings,
                ts_field="window_start",
            )
            log.info("kpi batch %d: %d windows", batch_id, len(dicts))
        finally:
            rows.unpersist()

    return sink_kpi


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--trigger", default="30 seconds", help="processing-time trigger interval")
    p.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"])
    p.add_argument("--checkpoint", default=None, help="override checkpoint base URI")
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    configure_logging()
    settings = get_settings()
    spark = build_spark("rtdaas-streaming-kpis", settings)
    ensure_tables(spark)
    for index in (ES_INDEX_EVENTS, ES_INDEX_KPI):
        es_ensure_index(index, settings=settings)

    schemas = {t: envelope_schema_json(t, settings) for t in TOPICS}
    checkpoint = (args.checkpoint or f"{settings.checkpoint_uri}/streaming_kpis").rstrip("/")

    source = read_kafka(spark, settings, args.starting_offsets)

    raw_query = (
        source.writeStream.queryName("rtdaas_raw")
        .foreachBatch(make_raw_sink(spark, settings, schemas))
        .option("checkpointLocation", f"{checkpoint}/raw")
        .trigger(processingTime=args.trigger)
        .start()
    )

    unified = unify(
        build_pose_stream(source, schemas[TOPIC_TOOL_POSE]),
        build_event_stream(source, schemas[TOPIC_EVENTS]),
        build_heartbeat_stream(source, schemas[TOPIC_HEARTBEAT]),
    )
    kpi_query = (
        kpi_aggregate(unified)
        .writeStream.queryName("rtdaas_kpi")
        .outputMode("update")
        .foreachBatch(make_kpi_sink(spark, settings))
        .option("checkpointLocation", f"{checkpoint}/kpi")
        .trigger(processingTime=args.trigger)
        .start()
    )
    log.info("streaming queries started: %s, %s", raw_query.id, kpi_query.id)
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
