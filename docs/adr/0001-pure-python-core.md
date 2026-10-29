# ADR 0001: Pure-Python core, thin Spark/Airflow wrappers

**Status:** accepted · **Date:** 2026-10-29

## Context

The platform runs the same computations in three places: Spark executors (production),
Airflow-triggered batch jobs (reprocessing/backfills), and a developer laptop (tests, demo
without a cluster). Logic duplicated across those paths drifts; logic that only exists inside
Spark DataFrame code can only be tested with a running cluster.

## Decision

All domain logic (MCAP/CDR decoding, Avro contract generation and compatibility rules, the
simulator, JIGSAWS parsing, kinematics and gesture metrics, data-quality checks) lives in
pure-Python modules that operate on dicts, numpy arrays and pandas frames. Spark jobs are thin:
they list files, parallelize, call those functions inside `mapPartitions` / `applyInPandas`,
and write Iceberg. Airflow DAGs only orchestrate `spark-submit` invocations.

`rtdaas.local.pipeline` runs the identical functions serially and writes a SQLite mirror of the
Iceberg tables so the API/UI and the quality suites can be exercised end to end on one machine.

## Consequences

- Unit tests cover the logic without a cluster; the Spark and Airflow layers are covered by
  integration tests in CI with real dependencies.
- Per-record work in Spark goes through Python, which is slower than Catalyst-native
  expressions. Aggregations stay in DataFrame code; only decoding and per-session metrics use
  Python UDF paths. For this workload (hundreds of MB of bags/day) that is far from the bottleneck.
- A behaviour change made once in the core applies to streaming approximations, batch and
  local runs alike.
