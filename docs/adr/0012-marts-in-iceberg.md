# ADR 0012: Curated marts live in Iceberg; Snowflake is dropped

**Status:** accepted · **Date:** 2026-10-30 · supersedes the Snowflake / DuckDB-file part of
[ADR 0007](0007-serving-stores.md)

## Context

ADR 0007 copied the four curated marts (`ROBOT_DAILY`, `SESSION_DAILY`, `GESTURE_SKILL_SUMMARY`,
`CHECK_RESULTS`) into Snowflake, as the warehouse an analyst or BI tool would query, with a DuckDB
file as the stand-in when no Snowflake account was configured. Snowflake was in the design
because the stack the project mirrors (a robotics data-platform role) names it; nothing in the
platform read the marts back — the API serves from Iceberg and Elasticsearch, the dashboards from
Elasticsearch — and no BI tool was deployed.

Running the stack end to end showed the Snowflake path could not have worked as written:

- the job created an *external* stage on `s3://<lake>/stage/...` with inline keys; in the compose
  and k3s deployments that bucket is MinIO, which Snowflake (a cloud service) cannot reach;
- the publish job's driver runs in the Airflow container (client mode), which had no Snowflake
  connector;
- it authenticated with a password, which Snowflake no longer accepts for service users since its
  2026 single-factor-password deprecation (trial accounts excepted).

Keeping it would have meant an account and credentials for a copy nobody reads.

## Decision

- `publish_marts.py` (was `publish_snowflake.py`) writes each mart from `marts.sql` to
  `lake.marts.<table>`: day-partitioned marts replace only the published day's partition
  (dynamic partition overwrite), unpartitioned ones are replaced whole. `--date` takes a list, and
  `telemetry_backfill` publishes every date of its range (it used to publish only the last).
- The marts get their own namespace: `DQ.CHECK_RESULTS` would otherwise collide with its source,
  `dq.check_results`.
- The Snowflake target, its settings and secrets, the Parquet export stage, the DuckDB-file target
  (`PUBLISH_DUCKDB_PATH`) and the API's fallback views over that stage (which had never matched the
  stage's paths) are removed.
- The marts are listed in `catalog.yml`, so they appear on the Catalog page with row counts and
  freshness like every other table.

## Consequences

- One copy of the data, no warehouse account or credentials; any engine that reads the Iceberg
  REST catalog reads the marts (Spark, DuckDB as in the API, PyIceberg, Trino).
- No SQL endpoint for BI tools out of the box. If one is wanted, put a query engine on the catalog
  (e.g. Trino) rather than copying the data out again; a warehouse export can come back as another
  `publish_marts` target that reads `lake.marts.*`.
- Changing a mart's columns needs a migration (or dropping the table): a partition overwrite
  requires the existing schema.
