# Quality gates

Quality is enforced, not reported: a blocking failure stops the DAG before anything is published.
All checks are YAML suites under `quality/checks/`, evaluated by `rtdaas.quality.engine` through a
`TableAdapter` (pandas for tests, SQLite for the local mirror, Spark for Iceberg) — see
[ADR 0011](../adr/0011-quality-gates-as-yaml.md).

## Suites

| Suite | Tables | Run by |
|---|---|---|
| `telemetry.yml` | `tool_pose_raw`, `events_raw`, `heartbeat_raw`, `sessions`, `kpi_1m` | `telemetry_bag_ingest`, `dq_daily`, local pipeline |
| `procedures.yml` | `trials`, `kinematics`, `gestures`, `frame_features` | `jigsaws_ingest`, `dq_daily`, local pipeline |
| `streaming.yml` | `kpi_1m` freshness | `dq_daily` (and a 5-minute Airflow sensor if desired) |

## Check types

| Type | Parameters | Pass condition |
|---|---|---|
| `not_null` | `column` | no NULLs |
| `row_count` | `min`, `max` | count within bounds |
| `value_range` | `column`, `min`, `max`, `max_violation_fraction` | out-of-range fraction ≤ threshold |
| `unique` | `columns` | no duplicate keys |
| `sample_rate` | `ts_column`, `group_by`, `expected_hz`, `tolerance_fraction` | median rate per group within tolerance |
| `schema_match` | `expected: spec` | all spec columns present (extra columns are reported, not failed) |
| `freshness` | `ts_column`, `max_age_hours` | max(ts) within the window |
| `sql` | `query` with `{table}` and optional `{partition}` placeholders, `op`, `threshold` | scalar result satisfies the comparison (the query decides its own partition filter) |

Each check has `severity: blocking | warning`. Results are rows in `dq.check_results`
(`run_id, suite, table_name, check_name, severity, status, observed, threshold, partition_value,
run_ts, message`).

## Running

```bash
# local mirror
rtdaas-quality run --suite quality/checks/telemetry.yml --backend sqlite --db data/rtdaas.sqlite --partition 2026-10-29 --out-table
# Spark / Iceberg
spark-submit rtdaas/spark/quality_gate.py --suite quality/checks/telemetry.yml --partition 2026-10-29
```

Exit code 2 means at least one blocking check failed; Airflow marks the task failed and the
`publish` task is skipped. Warnings are recorded and shown on the Quality page but do not block.

## Adding a check

1. Add the entry to the suite YAML (name, type, parameters, severity).
2. If it is a new type, implement it in `rtdaas/quality/engine.py` (one function) and add a
   pass/fail unit test with hand-built frames.
3. Run the suite against the local mirror; commit with the schema change it protects.
