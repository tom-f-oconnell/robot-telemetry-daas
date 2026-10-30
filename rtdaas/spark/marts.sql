-- Curated marts, written by publish_marts.py with Spark SQL over the `lake` catalog to
-- `lake.marts.<table>` (lower-cased TABLE). Statements are separated by a line containing only
-- `-- @mart <SCHEMA>.<TABLE> [partition_column]`; `{date}` is substituted with the run date
-- (YYYY-MM-DD) and `{catalog}` with the Iceberg catalog name.
--
-- Every mart carries a `day` (or run date) column; with a partition column, a re-run replaces
-- exactly that day's partition (idempotent publish, ADR 0005), without one the table is replaced.

-- @mart TELEMETRY.ROBOT_DAILY day
SELECT
  robot_id,
  CAST(start_ts AS DATE)                  AS day,
  COUNT(DISTINCT session_id)              AS sessions,
  SUM(duration_s)                         AS active_s,
  SUM(path_length_m)                      AS path_length_m,
  SUM(warn_events)                        AS warn_events,
  SUM(error_events)                       AS error_events,
  SUM(instrument_swaps)                   AS instrument_swaps,
  AVG(idle_fraction)                      AS mean_idle_fraction,
  MAX(processed_at)                       AS processed_at
FROM {catalog}.telemetry.sessions
WHERE CAST(start_ts AS DATE) = DATE '{date}'
GROUP BY robot_id, CAST(start_ts AS DATE)

-- @mart TELEMETRY.SESSION_DAILY day
SELECT
  session_id, robot_id, arm, start_ts, end_ts,
  CAST(start_ts AS DATE)                  AS day,
  duration_s, n_samples, path_length_m, mean_speed_mps, idle_fraction,
  jaw_cycles, warn_events, error_events, instrument_swaps,
  bag_file, pipeline_version, processed_at
FROM {catalog}.telemetry.sessions
WHERE CAST(start_ts AS DATE) = DATE '{date}'

-- @mart PROCEDURES.GESTURE_SKILL_SUMMARY
SELECT
  task,
  gesture,
  skill_level,
  COUNT(*)                                AS n,
  AVG(duration_s)                         AS mean_duration_s,
  AVG(path_length_psml)                   AS mean_path_length_psml,
  AVG(path_length_psmr)                   AS mean_path_length_psmr,
  AVG(mean_speed_psml)                    AS mean_speed_psml,
  AVG(mean_speed_psmr)                    AS mean_speed_psmr,
  AVG(gripper_cycles)                     AS mean_gripper_cycles,
  AVG(motion_energy_mean)                 AS mean_motion_energy,
  DATE '{date}'                           AS day
FROM {catalog}.procedures.gestures
GROUP BY task, gesture, skill_level

-- @mart DQ.CHECK_RESULTS day
SELECT
  run_id, suite, table_name, check_name, severity, status, observed, threshold,
  partition_value, run_ts, message,
  CAST(run_ts AS DATE)                    AS day
FROM {catalog}.dq.check_results
WHERE CAST(run_ts AS DATE) = DATE '{date}'
