# ADR 0011: Data-quality checks are YAML suites run by one engine on pandas, SQLite or Spark

**Status:** accepted · **Date:** 2026-10-29

## Context

Great Expectations and Soda are capable but heavy; their Spark integrations pin versions and the
rule DSLs are opaque to reviewers. The platform needs a small set of check types (null, range,
uniqueness, row count, sample rate, schema match, freshness, arbitrary SQL) that run identically
in CI (fixtures), locally (SQLite) and in production (Iceberg via Spark), and whose results land
in a table the UI can show.

## Decision

Suites are YAML files under `quality/checks/`. `rtdaas.quality.engine` evaluates them through a
`TableAdapter` protocol with pandas, SQLite and Spark implementations. Every result is a row in
`dq.check_results` with `severity ∈ {blocking, warning}`; `quality_gate.py` exits 2 on blocking
failures so Airflow stops the publish step.

## Consequences

- Check definitions are reviewed like code and versioned with the schema they protect.
- The engine is ~400 lines; adding a check type is one function.
- Soda/GE can still be adopted later for anomaly-detection style checks; the results table
  schema does not depend on the engine.
