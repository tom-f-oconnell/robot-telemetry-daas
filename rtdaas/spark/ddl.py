"""Table definitions for the Iceberg lakehouse (spec §3.2) in one place.

Each table is described once as a list of ``(column, spark_sql_type)`` pairs plus a
partition spec. From that single description we derive:

* the Iceberg ``CREATE TABLE`` DDL executed by :func:`rtdaas.spark.common.ensure_tables`,
* PySpark ``StructType`` schemas (:func:`struct_type`) for ``createDataFrame`` and
  ``applyInPandas``,
* ordered tuple conversion for row dicts produced by the pure-Python decoders
  (:func:`to_tuple`).

This module must stay importable without ``pyspark`` (only :func:`struct_type`
imports it, lazily).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

CATALOG = "lake"

ARM_STRUCT_FIELDS = (
    "x",
    "y",
    "z",
    "r11",
    "r12",
    "r13",
    "r21",
    "r22",
    "r23",
    "r31",
    "r32",
    "r33",
    "vx",
    "vy",
    "vz",
    "wx",
    "wy",
    "wz",
    "gripper",
)
ARM_STRUCT_TYPE = "struct<" + ",".join(f"{f}:double" for f in ARM_STRUCT_FIELDS) + ">"


@dataclass(frozen=True)
class Column:
    name: str
    type: str  # Spark SQL type string, e.g. "string", "array<double>", "struct<x:double>"


@dataclass(frozen=True)
class TableDef:
    name: str  # "<namespace>.<table>"
    columns: tuple[Column, ...]
    partition_by: tuple[str, ...] = ()
    comment: str = ""
    freshness_column: str | None = None  # timestamp column used for freshness / catalog

    @property
    def namespace(self) -> str:
        return self.name.split(".", 1)[0]

    @property
    def short_name(self) -> str:
        return self.name.split(".", 1)[1]

    @property
    def column_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def qualified(self, catalog: str = CATALOG) -> str:
        return f"{catalog}.{self.name}"

    def ddl(self, catalog: str = CATALOG) -> str:
        cols = ",\n  ".join(f"{c.name} {c.type}" for c in self.columns)
        sql = f"CREATE TABLE IF NOT EXISTS {self.qualified(catalog)} (\n  {cols}\n) USING iceberg"
        if self.partition_by:
            sql += "\nPARTITIONED BY (" + ", ".join(self.partition_by) + ")"
        if self.comment:
            escaped = self.comment.replace("'", "''")
            sql += f"\nCOMMENT '{escaped}'"
        sql += (
            "\nTBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd',"
            " 'write.distribution-mode'='hash')"
        )
        return sql


def _cols(*pairs: tuple[str, str]) -> tuple[Column, ...]:
    return tuple(Column(n, t) for n, t in pairs)


_RAW_TAIL = (("source", "string"), ("bag_file", "string"))

TABLES: dict[str, TableDef] = {}


def _register(t: TableDef) -> TableDef:
    TABLES[t.name] = t
    return t


JOINT_STATES_RAW = _register(
    TableDef(
        "telemetry.joint_states_raw",
        _cols(
            ("robot_id", "string"),
            ("arm", "string"),
            ("ts", "timestamp"),
            ("recv_ts", "timestamp"),
            ("frame_id", "string"),
            ("joint_names", "array<string>"),
            ("position", "array<double>"),
            ("velocity", "array<double>"),
            ("effort", "array<double>"),
            *_RAW_TAIL,
        ),
        partition_by=("days(ts)", "robot_id"),
        comment="sensor_msgs/JointState samples per arm (dVRK measured_js).",
        freshness_column="ts",
    )
)

TOOL_POSE_RAW = _register(
    TableDef(
        "telemetry.tool_pose_raw",
        _cols(
            ("robot_id", "string"),
            ("arm", "string"),
            ("ts", "timestamp"),
            ("recv_ts", "timestamp"),
            ("x", "double"),
            ("y", "double"),
            ("z", "double"),
            ("qx", "double"),
            ("qy", "double"),
            ("qz", "double"),
            ("qw", "double"),
            ("jaw_angle", "double"),
            ("speed", "double"),
            *_RAW_TAIL,
        ),
        partition_by=("days(ts)", "robot_id"),
        comment="rtdaas_msgs/ToolPose: instrument tip pose, jaw angle and speed per arm.",
        freshness_column="ts",
    )
)

EVENTS_RAW = _register(
    TableDef(
        "telemetry.events_raw",
        _cols(
            ("robot_id", "string"),
            ("arm", "string"),
            ("ts", "timestamp"),
            ("recv_ts", "timestamp"),
            ("severity", "int"),
            ("severity_name", "string"),
            ("code", "string"),
            ("message", "string"),
            *_RAW_TAIL,
        ),
        partition_by=("days(ts)",),
        comment="rtdaas_msgs/SystemEvent: discrete system events.",
        freshness_column="ts",
    )
)

HEARTBEAT_RAW = _register(
    TableDef(
        "telemetry.heartbeat_raw",
        _cols(
            ("robot_id", "string"),
            ("ts", "timestamp"),
            ("recv_ts", "timestamp"),
            ("uptime_s", "double"),
            ("state", "string"),
            ("active_arms", "array<string>"),
            ("software_version", "string"),
            *_RAW_TAIL,
        ),
        partition_by=("days(ts)",),
        comment="rtdaas_msgs/Heartbeat: 1 Hz liveness per robot.",
        freshness_column="ts",
    )
)

KPI_1M = _register(
    TableDef(
        "telemetry.kpi_1m",
        _cols(
            ("robot_id", "string"),
            ("arm", "string"),
            ("window_start", "timestamp"),
            ("window_end", "timestamp"),
            ("n_samples", "bigint"),
            ("mean_speed_mps", "double"),
            ("max_speed_mps", "double"),
            ("path_length_m", "double"),
            ("idle_fraction", "double"),
            ("jaw_cycles", "bigint"),
            ("warn_events", "bigint"),
            ("error_events", "bigint"),
            ("heartbeat_gap_s", "double"),
            ("computed_by", "string"),
            ("computed_at", "timestamp"),
        ),
        partition_by=("days(window_start)",),
        comment="1-minute KPIs per robot x arm; computed_by=stream (approximate) or batch (exact).",
        freshness_column="window_start",
    )
)

SESSIONS = _register(
    TableDef(
        "telemetry.sessions",
        _cols(
            ("session_id", "string"),
            ("robot_id", "string"),
            ("arm", "string"),
            ("start_ts", "timestamp"),
            ("end_ts", "timestamp"),
            ("duration_s", "double"),
            ("n_samples", "bigint"),
            ("path_length_m", "double"),
            ("mean_speed_mps", "double"),
            ("idle_fraction", "double"),
            ("jaw_cycles", "bigint"),
            ("warn_events", "bigint"),
            ("error_events", "bigint"),
            ("instrument_swaps", "bigint"),
            ("bag_file", "string"),
            ("pipeline_version", "string"),
            ("processed_at", "timestamp"),
        ),
        partition_by=("days(start_ts)",),
        comment="Economy-of-motion metrics per session x arm.",
        freshness_column="start_ts",
    )
)

TRIALS = _register(
    TableDef(
        "procedures.trials",
        _cols(
            ("trial_id", "string"),
            ("task", "string"),
            ("subject", "string"),
            ("repetition", "int"),
            ("skill_level", "string"),
            ("grs_total", "int"),
            ("duration_s", "double"),
            ("n_frames", "bigint"),
            ("n_kinematic_samples", "bigint"),
            ("sample_rate_hz", "double"),
            ("video_path", "string"),
            ("kinematics_path", "string"),
            ("transcription_path", "string"),
            ("n_gestures", "bigint"),
            ("processed_at", "timestamp"),
            ("pipeline_version", "string"),
        ),
        partition_by=("task",),
        comment="JIGSAWS trial metadata.",
        freshness_column="processed_at",
    )
)

KINEMATICS = _register(
    TableDef(
        "procedures.kinematics",
        _cols(
            ("trial_id", "string"),
            ("task", "string"),
            ("frame_idx", "bigint"),
            ("t_s", "double"),
            ("mtml", ARM_STRUCT_TYPE),
            ("mtmr", ARM_STRUCT_TYPE),
            ("psml", ARM_STRUCT_TYPE),
            ("psmr", ARM_STRUCT_TYPE),
            ("gesture", "string"),
        ),
        partition_by=("task", "trial_id"),
        comment="JIGSAWS kinematic samples (30 Hz) with gesture label.",
    )
)

GESTURES = _register(
    TableDef(
        "procedures.gestures",
        _cols(
            ("trial_id", "string"),
            ("task", "string"),
            ("subject", "string"),
            ("skill_level", "string"),
            ("gesture", "string"),
            ("start_frame", "bigint"),
            ("end_frame", "bigint"),
            ("duration_s", "double"),
            ("path_length_psml", "double"),
            ("path_length_psmr", "double"),
            ("mean_speed_psml", "double"),
            ("mean_speed_psmr", "double"),
            ("gripper_cycles", "bigint"),
            ("motion_energy_mean", "double"),
            ("n_frames", "bigint"),
        ),
        partition_by=("task",),
        comment="Per-gesture segment metrics.",
    )
)

FRAME_FEATURES = _register(
    TableDef(
        "procedures.frame_features",
        _cols(
            ("trial_id", "string"),
            ("task", "string"),
            ("frame_idx", "bigint"),
            ("t_s", "double"),
            ("mean_intensity", "double"),
            ("motion_energy", "double"),
            ("sharpness", "double"),
            ("gesture", "string"),
        ),
        partition_by=("task", "trial_id"),
        comment="Per-video-frame features aligned to kinematics.",
    )
)

CHECK_RESULTS = _register(
    TableDef(
        "dq.check_results",
        _cols(
            ("run_id", "string"),
            ("suite", "string"),
            ("table_name", "string"),
            ("check_name", "string"),
            ("severity", "string"),
            ("status", "string"),
            ("observed", "double"),
            ("threshold", "string"),
            ("partition_value", "string"),
            ("run_ts", "timestamp"),
            ("message", "string"),
        ),
        partition_by=("days(run_ts)",),
        comment="Data-quality check results.",
        freshness_column="run_ts",
    )
)

CATALOG_TABLE = _register(
    TableDef(
        "dq.catalog",
        _cols(
            ("name", "string"),
            ("owner", "string"),
            ("description", "string"),
            ("sla_hours", "double"),
            ("row_count", "bigint"),
            ("last_updated", "timestamp"),
            ("freshness_ok", "boolean"),
        ),
        comment="Published data products with freshness, served by GET /api/catalog.",
        freshness_column="last_updated",
    )
)

NAMESPACES = ("telemetry", "procedures", "dq", "marts")  # marts: written by publish_marts.py

RAW_TABLES = {
    "tool_pose": TOOL_POSE_RAW,
    "joint_states": JOINT_STATES_RAW,
    "events": EVENTS_RAW,
    "heartbeat": HEARTBEAT_RAW,
}

# Merge keys used when upserting into the raw tables (events get `code` to allow two
# different events at the same stamp).
RAW_MERGE_KEYS = {
    "tool_pose": ["robot_id", "arm", "ts"],
    "joint_states": ["robot_id", "arm", "ts"],
    "events": ["robot_id", "arm", "ts", "code"],
    "heartbeat": ["robot_id", "ts"],
}


def all_ddl(catalog: str = CATALOG) -> list[str]:
    stmts = [f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}" for ns in NAMESPACES]
    stmts += [t.ddl(catalog) for t in TABLES.values()]
    return stmts


def get(name: str) -> TableDef:
    return TABLES[name]


# ---------------------------------------------------------------------------
# Type-string parsing -> PySpark types (lazy import) / struct ordering helpers
# ---------------------------------------------------------------------------


def _split_top_level(s: str, sep: str = ",") -> list[str]:
    parts, depth, cur = [], 0, []
    for ch in s:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        if ch == sep and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur).strip())
    return parts


def struct_fields_of(type_str: str) -> list[tuple[str, str]] | None:
    """Return ``[(name, type), ...]`` for a ``struct<...>`` type string, else ``None``."""
    t = type_str.strip()
    if not t.startswith("struct<") or not t.endswith(">"):
        return None
    inner = t[len("struct<") : -1]
    out = []
    for part in _split_top_level(inner):
        name, typ = part.split(":", 1)
        out.append((name.strip(), typ.strip()))
    return out


def spark_type(type_str: str):
    """Build a ``pyspark.sql.types.DataType`` from a Spark SQL type string."""
    from pyspark.sql import types as T

    t = type_str.strip().lower()
    simple = {
        "string": T.StringType(),
        "int": T.IntegerType(),
        "integer": T.IntegerType(),
        "bigint": T.LongType(),
        "long": T.LongType(),
        "double": T.DoubleType(),
        "float": T.FloatType(),
        "boolean": T.BooleanType(),
        "timestamp": T.TimestampType(),
        "date": T.DateType(),
        "binary": T.BinaryType(),
    }
    if t in simple:
        return simple[t]
    if t.startswith("array<") and t.endswith(">"):
        return T.ArrayType(spark_type(t[len("array<") : -1]))
    fields = struct_fields_of(type_str)
    if fields is not None:
        return T.StructType([T.StructField(n, spark_type(ty), True) for n, ty in fields])
    raise ValueError(f"unsupported type string: {type_str!r}")


def struct_type(table_name: str):
    """``StructType`` for a table (requires pyspark)."""
    from pyspark.sql import types as T

    t = TABLES[table_name]
    return T.StructType([T.StructField(c.name, spark_type(c.type), True) for c in t.columns])


def to_tuple(table: TableDef | str, row: dict[str, Any]) -> tuple:
    """Order a row dict into a tuple matching the table's columns (nested structs too)."""
    t = TABLES[table] if isinstance(table, str) else table
    out = []
    for c in t.columns:
        v = row.get(c.name)
        fields = struct_fields_of(c.type)
        if fields is not None and isinstance(v, dict):
            v = tuple(v.get(n) for n, _ in fields)
        out.append(v)
    return tuple(out)
