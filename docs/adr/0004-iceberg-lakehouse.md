# ADR 0004: Apache Iceberg on object storage with a REST catalog

**Status:** accepted · **Date:** 2026-10-29

## Context

Raw telemetry is append-mostly but reprocessing must be idempotent (bags re-ingested, metrics
recomputed with a new pipeline version). Plain Parquet directories cannot express "replace this
session's rows"; Hive-style overwrites are not atomic. Multiple engines (Spark, DuckDB in the API,
Snowflake via external tables) need to read the same tables.

## Decision

Tables are Iceberg v2 with hidden partitioning (`days(ts)`, `robot_id`), served by a REST catalog
(tabulario/iceberg-rest locally; the same API on Kubernetes). Writes use `MERGE INTO` for
upserts and `overwritePartitions` for whole-partition rebuilds. Streaming appends go to the same
tables with `source = 'stream'`; batch rows carry `source = 'bag'` and win on key collisions.

## Consequences

- Time travel and snapshot isolation come for free; a bad backfill is a `rollback_to_snapshot`.
- DuckDB reads the tables through the `iceberg` extension for the API's analytical endpoints,
  avoiding a Spark dependency in the serving path.
- Small files from 30-second streaming triggers need periodic `rewrite_data_files` compaction
  (scheduled in the `dq_daily` DAG's maintenance task).
