# ADR 0005: Batch results win over streaming results; every job is idempotent

**Status:** accepted · **Date:** 2026-10-29

## Context

The same telemetry arrives twice: live through Kafka and later as recorded bags. Streaming KPIs
are approximations under watermarks and late data; batch KPIs are exact. Reprocessing (new
metric definition, bug fix) must be safe to run any number of times.

## Decision

- Raw tables are keyed by `(robot_id, arm, ts)`; `MERGE INTO` makes re-ingestion a no-op or an update.
- `telemetry.kpi_1m` rows carry `computed_by ∈ {stream, batch}`. The streaming sink's MERGE
  never updates a row whose `computed_by = 'batch'`; the batch job always overwrites.
- Sessions are keyed by a deterministic `session_id` (robot + start timestamp) so a second run
  replaces rather than duplicates.
- Every job takes an explicit partition (`--date`) and can be invoked for any past date from the
  `telemetry_backfill` DAG.

## Consequences

- Dashboards may show slightly different numbers for the last hour than for yesterday; that is
  expected and documented in the catalog descriptions.
- Deleting data (GDPR-style) is a MERGE/DELETE on the raw tables plus a session recompute, not a
  partition rewrite.
