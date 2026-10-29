# Publishing to Snowflake (or DuckDB)

`rtdaas/spark/publish_snowflake.py` builds curated marts from Iceberg with Spark SQL
(`rtdaas/spark/marts.sql`), writes them as Parquet to a stage prefix, and loads them into
Snowflake with `COPY INTO`. With `--target duckdb` the same marts land in a DuckDB file instead,
which is what the compose demo and CI use ([ADR 0007](../adr/0007-serving-stores.md)).

## Marts

| Snowflake table | Grain | Source |
|---|---|---|
| `TELEMETRY.ROBOT_DAILY` | robot × day | `telemetry.sessions` |
| `TELEMETRY.SESSION_DAILY` | robot × arm × session | `telemetry.sessions` |
| `PROCEDURES.GESTURE_SKILL_SUMMARY` | task × gesture × skill level | `procedures.gestures` |
| `DQ.CHECK_RESULTS` | check × run | `dq.check_results` |

## Load path

```
Iceberg (lake.*) ──Spark SQL──▶ Parquet @ s3://rtdaas-lake/stage/snowflake/<TABLE>/<run_id>/
                                  └──▶ Snowflake: CREATE SCHEMA/TABLE IF NOT EXISTS
                                       DELETE WHERE day = :date      (idempotent)
                                       COPY INTO <TABLE> FROM @stage/<TABLE>/<run_id>/
                                            FILE_FORMAT = (TYPE = PARQUET)
                                            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
```

Credentials come from `SNOWFLAKE_*` environment variables (Kubernetes Secret
`rtdaas-secrets`). For a real AWS deployment use a storage integration + external stage instead
of inline keys; the compose stack uses MinIO with inline keys.

## Catalog refresh

The same job rewrites `dq.catalog` from `rtdaas/serving/catalog.yml`, filling in row counts and
last-updated timestamps per table, which the API's `/api/catalog` endpoint serves.
