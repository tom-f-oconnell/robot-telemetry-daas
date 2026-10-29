# ADR 0007: Elasticsearch for search and live dashboards, Snowflake for marts, DuckDB as the fallback

**Status:** accepted · **Date:** 2026-10-29

## Context

Three access patterns: free-text/filtered search over events and gesture segments with
sub-second latency; aggregated BI over sessions and gestures; and a portfolio/demo environment
that must run without a paid warehouse.

## Decision

- Elasticsearch holds events, 1-minute KPIs, sessions and gesture segments with deterministic
  document ids (idempotent upserts) and `@timestamp` for Kibana/Grafana.
- Snowflake receives curated marts via Parquet stage + `COPY INTO`, partition-replaced per day.
- `publish_snowflake.py --target duckdb` writes the same marts to a DuckDB file so the full
  pipeline, API and tests run without Snowflake credentials.

## Consequences

- Raw tables never go to Snowflake; Iceberg is the system of record and Snowflake is a
  consumer, which keeps warehouse cost proportional to curated data.
- ES mappings are versioned in code (`es_ensure_index`) so index creation is reproducible.
