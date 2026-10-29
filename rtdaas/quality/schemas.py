"""Expected column sets for every published table (transcribed from spec §3.2).

Used by the ``schema_match`` check and by the DDL generator in ``rtdaas.spark.ddl``.
Type names are Iceberg/Spark SQL names; struct columns list their flattened children
(``psml`` -> ``psml_x`` ...) under ``STRUCT_CHILDREN`` so the SQLite mirror can be checked too.
"""

from __future__ import annotations

ARM_STRUCT = "struct<x:double,y:double,z:double,r11:double,r12:double,r13:double,r21:double,r22:double,r23:double,r31:double,r32:double,r33:double,vx:double,vy:double,vz:double,wx:double,wy:double,wz:double,gripper:double>"
ARM_STRUCT_FIELDS: tuple[str, ...] = (
    "x", "y", "z", "r11", "r12", "r13", "r21", "r22", "r23", "r31", "r32", "r33",
    "vx", "vy", "vz", "wx", "wy", "wz", "gripper",
)

SPEC_SCHEMAS: dict[str, dict[str, str]] = {
    "telemetry.joint_states_raw": {
        "robot_id": "string", "arm": "string", "ts": "timestamp", "recv_ts": "timestamp", "frame_id": "string",
        "joint_names": "array<string>", "position": "array<double>", "velocity": "array<double>",
        "effort": "array<double>", "source": "string", "bag_file": "string",
    },
    "telemetry.tool_pose_raw": {
        "robot_id": "string", "arm": "string", "ts": "timestamp", "recv_ts": "timestamp",
        "x": "double", "y": "double", "z": "double", "qx": "double", "qy": "double", "qz": "double", "qw": "double",
        "jaw_angle": "double", "speed": "double", "source": "string", "bag_file": "string",
    },
    "telemetry.events_raw": {
        "robot_id": "string", "arm": "string", "ts": "timestamp", "recv_ts": "timestamp", "severity": "int",
        "severity_name": "string", "code": "string", "message": "string", "source": "string", "bag_file": "string",
    },
    "telemetry.heartbeat_raw": {
        "robot_id": "string", "ts": "timestamp", "recv_ts": "timestamp", "uptime_s": "double", "state": "string",
        "active_arms": "array<string>", "software_version": "string", "source": "string", "bag_file": "string",
    },
    "telemetry.kpi_1m": {
        "robot_id": "string", "arm": "string", "window_start": "timestamp", "window_end": "timestamp",
        "n_samples": "long", "mean_speed_mps": "double", "max_speed_mps": "double", "path_length_m": "double",
        "idle_fraction": "double", "jaw_cycles": "long", "warn_events": "long", "error_events": "long",
        "heartbeat_gap_s": "double", "computed_by": "string", "computed_at": "timestamp",
    },
    "telemetry.sessions": {
        "session_id": "string", "robot_id": "string", "arm": "string", "start_ts": "timestamp", "end_ts": "timestamp",
        "duration_s": "double", "n_samples": "long", "path_length_m": "double", "mean_speed_mps": "double",
        "idle_fraction": "double", "jaw_cycles": "long", "warn_events": "long", "error_events": "long",
        "instrument_swaps": "long", "bag_file": "string", "pipeline_version": "string", "processed_at": "timestamp",
    },
    "procedures.trials": {
        "trial_id": "string", "task": "string", "subject": "string", "repetition": "int", "skill_level": "string",
        "grs_total": "int", "duration_s": "double", "n_frames": "long", "n_kinematic_samples": "long",
        "sample_rate_hz": "double", "video_path": "string", "kinematics_path": "string", "transcription_path": "string",
        "n_gestures": "long", "processed_at": "timestamp", "pipeline_version": "string",
    },
    "procedures.kinematics": {
        "trial_id": "string", "task": "string", "frame_idx": "long", "t_s": "double",
        "mtml": ARM_STRUCT, "mtmr": ARM_STRUCT, "psml": ARM_STRUCT, "psmr": ARM_STRUCT, "gesture": "string",
    },
    "procedures.gestures": {
        "trial_id": "string", "task": "string", "subject": "string", "skill_level": "string", "gesture": "string",
        "start_frame": "long", "end_frame": "long", "duration_s": "double", "path_length_psml": "double",
        "path_length_psmr": "double", "mean_speed_psml": "double", "mean_speed_psmr": "double",
        "gripper_cycles": "long", "motion_energy_mean": "double", "n_frames": "long",
    },
    "procedures.frame_features": {
        "trial_id": "string", "task": "string", "frame_idx": "long", "t_s": "double", "mean_intensity": "double",
        "motion_energy": "double", "sharpness": "double", "gesture": "string",
    },
    "dq.check_results": {
        "run_id": "string", "suite": "string", "table_name": "string", "check_name": "string", "severity": "string",
        "status": "string", "observed": "double", "threshold": "string", "partition_value": "string",
        "run_ts": "timestamp", "message": "string",
    },
}

STRUCT_CHILDREN: dict[str, tuple[str, ...]] = {
    "procedures.kinematics": tuple(f"{arm}_{f}" for arm in ("mtml", "mtmr", "psml", "psmr") for f in ARM_STRUCT_FIELDS),
}

PARTITIONING: dict[str, list[str]] = {
    "telemetry.joint_states_raw": ["days(ts)", "robot_id"],
    "telemetry.tool_pose_raw": ["days(ts)", "robot_id"],
    "telemetry.events_raw": ["days(ts)"],
    "telemetry.heartbeat_raw": ["days(ts)"],
    "telemetry.kpi_1m": ["days(window_start)"],
    "telemetry.sessions": ["days(start_ts)"],
    "procedures.trials": ["task"],
    "procedures.kinematics": ["task", "trial_id"],
    "procedures.gestures": ["task"],
    "procedures.frame_features": ["task", "trial_id"],
    "dq.check_results": ["days(run_ts)"],
}


def flattened_columns(table_name: str) -> dict[str, str]:
    """Spec schema with struct columns replaced by their flattened children (SQLite mirror view)."""
    schema: dict[str, str] = {}
    for col, typ in SPEC_SCHEMAS[table_name].items():
        if typ.startswith("struct<"):
            for child in ARM_STRUCT_FIELDS:
                schema[f"{col}_{child}"] = "double"
        else:
            schema[col] = typ
    return schema


def sqlite_table_name(table_name: str) -> str:
    """``telemetry.sessions`` -> ``telemetry__sessions`` (spec §3.3)."""
    return table_name.replace(".", "__")


def iceberg_table_name(sqlite_name: str) -> str:
    return sqlite_name.replace("__", ".", 1)
