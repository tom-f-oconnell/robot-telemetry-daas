# ADR 0006: Streaming KPIs use window-local approximations

**Status:** accepted · **Date:** 2026-10-29

## Context

Exact path length and jaw-cycle counts need ordered access to consecutive samples. Structured
Streaming aggregations are commutative/associative reductions over a window; `lag()` over an
unbounded stream is not available without stateful `flatMapGroupsWithState`, which is harder to
operate and to reason about under late data.

## Decision

In the streaming job, `path_length_m ≈ mean(speed) × window span`, `idle_fraction =
mean(speed < 0.005)`, and `jaw_cycles = 0` (left to batch). The `speed` field itself is computed
upstream by the adapter node from consecutive poses, which keeps the stream-side math stateless.
Batch recomputes all KPIs exactly from ordered samples and overwrites (ADR 0005).

## Consequences

- Live dashboards are within a few percent of the batch numbers for normal motion; the catalog
  marks streaming-derived columns as "approximate until batch".
- If exact live counts become a requirement, the upgrade path is `applyInPandasWithState` keyed
  by `(robot_id, arm)` — the same `rtdaas.metrics` functions apply.
