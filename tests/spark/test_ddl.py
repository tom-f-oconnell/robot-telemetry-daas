"""DDL consistency with the spec (§3.2) and the quality schema registry — no Spark needed."""

from __future__ import annotations

from rtdaas.quality.schemas import SPEC_SCHEMAS
from rtdaas.spark import ddl

SPEC_COLUMNS = {
    "telemetry.tool_pose_raw": ["robot_id", "arm", "ts", "recv_ts", "x", "y", "z", "qx", "qy", "qz", "qw", "jaw_angle", "speed", "source", "bag_file"],
    "telemetry.events_raw": ["robot_id", "arm", "ts", "recv_ts", "severity", "severity_name", "code", "message", "source", "bag_file"],
    "telemetry.heartbeat_raw": ["robot_id", "ts", "recv_ts", "uptime_s", "state", "active_arms", "software_version", "source", "bag_file"],
    "telemetry.kpi_1m": ["robot_id", "arm", "window_start", "window_end", "n_samples", "mean_speed_mps", "max_speed_mps", "path_length_m", "idle_fraction", "jaw_cycles", "warn_events", "error_events", "heartbeat_gap_s", "computed_by", "computed_at"],
    "telemetry.sessions": ["session_id", "robot_id", "arm", "start_ts", "end_ts", "duration_s", "n_samples", "path_length_m", "mean_speed_mps", "idle_fraction", "jaw_cycles", "warn_events", "error_events", "instrument_swaps", "bag_file", "pipeline_version", "processed_at"],
    "procedures.trials": ["trial_id", "task", "subject", "repetition", "skill_level", "grs_total", "duration_s", "n_frames", "n_kinematic_samples", "sample_rate_hz", "video_path", "kinematics_path", "transcription_path", "n_gestures", "processed_at", "pipeline_version"],
    "procedures.gestures": ["trial_id", "task", "subject", "skill_level", "gesture", "start_frame", "end_frame", "duration_s", "path_length_psml", "path_length_psmr", "mean_speed_psml", "mean_speed_psmr", "gripper_cycles", "motion_energy_mean", "n_frames"],
    "procedures.frame_features": ["trial_id", "task", "frame_idx", "t_s", "mean_intensity", "motion_energy", "sharpness", "gesture"],
    "dq.check_results": ["run_id", "suite", "table_name", "check_name", "severity", "status", "observed", "threshold", "partition_value", "run_ts", "message"],
}


def test_every_spec_table_has_ddl():
    for name in SPEC_SCHEMAS:
        t = ddl.get(name)
        assert t.name == name


def test_column_order_matches_spec():
    for name, cols in SPEC_COLUMNS.items():
        assert ddl.get(name).column_names == cols, name
    kin = ddl.get("procedures.kinematics").column_names
    assert kin[:4] == ["trial_id", "task", "frame_idx", "t_s"] and kin[-1] == "gesture"
    assert {"mtml", "mtmr", "psml", "psmr"} <= set(kin)


def test_partitioning_and_statements():
    assert ddl.get("telemetry.tool_pose_raw").partition_by == ("days(ts)", "robot_id")
    assert ddl.get("telemetry.kpi_1m").partition_by == ("days(window_start)",)
    assert ddl.get("procedures.kinematics").partition_by == ("task", "trial_id")
    statements = ddl.all_ddl()
    assert any(s.startswith("CREATE NAMESPACE IF NOT EXISTS lake.telemetry") for s in statements)
    create = [s for s in statements if "lake.telemetry.sessions" in s][0]
    assert "USING iceberg" in create and "PARTITIONED BY (days(start_ts))" in create and "format-version'='2" in create


def test_raw_merge_keys_cover_partition_columns():
    for kind, keys in ddl.RAW_MERGE_KEYS.items():
        table = ddl.RAW_TABLES[kind]
        assert "ts" in keys and set(keys) <= set(table.column_names)
