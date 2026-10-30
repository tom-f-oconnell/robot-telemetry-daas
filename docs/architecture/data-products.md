# Data products and the catalog

Every table the platform publishes is a *data product*: it has an owner, a description, an SLA,
documented columns, a contract (for raw tables, the Kafka subject; for derived tables, the
pipeline that produces them) and a freshness status. The catalog is the first thing a new
consumer sees, before any SQL.

## Where the catalog lives

- **Source:** `rtdaas/serving/catalog.yml` — one entry per table, reviewed with the code that
  produces the table.
- **Materialized:** the `catalog` table (`name, owner, description, sla_hours, row_count,
  last_updated, freshness_ok`) is written by `publish_marts.py` (Iceberg `dq.catalog`) and by
  the local pipeline (SQLite), so the numbers reflect the current state of the lake.
- **Served:** `GET /api/catalog` and the **Catalog** page of the UI, which shows every table with
  its columns, freshness, and a link to the quality results for that table.

## Published tables

| Table | Grain | Produced by | Consumers |
|---|---|---|---|
| `telemetry.joint_states_raw` | one row per JointState message per arm | bridge → streaming; bags → batch | debugging, future joint-level analytics |
| `telemetry.tool_pose_raw` | one row per ToolPose sample | streaming, batch | trajectory replay API, session metrics |
| `telemetry.events_raw` | one row per SystemEvent | streaming, batch | log explorer (via ES), session metrics |
| `telemetry.heartbeat_raw` | 1 Hz per robot | streaming, batch | liveness, session splitting |
| `telemetry.kpi_1m` | robot × arm × minute | streaming (approx.) then batch (exact) | live dashboards, session KPI timeline |
| `telemetry.sessions` | robot × arm × session | batch | economy-of-motion reporting, `marts.session_daily` |
| `procedures.trials` | one row per JIGSAWS trial | batch_jigsaws | trial browser |
| `procedures.kinematics` | trial × 30 Hz sample | batch_jigsaws | synced video + kinematics scrubber |
| `procedures.gestures` | trial × gesture segment | batch_jigsaws | gesture search (ES), skill summaries |
| `procedures.frame_features` | trial × video frame | batch_jigsaws | video QC, gesture motion energy |
| `dq.check_results` | one row per check per run | quality_gate, dq_daily, local pipeline | Quality page, alerts |
| `dq.catalog` | one row per table | publish, local pipeline | Catalog page |

Full column definitions are in [spec §3.2](spec.md#32-iceberg-tables-catalog-lake-rest-catalog) and
in `catalog.yml`.

## Session metrics ("economy of motion")

`telemetry.sessions` is the product most consumers care about. Per robot × arm × session:

| Column | Definition |
|---|---|
| `duration_s` | last − first tool-pose timestamp |
| `path_length_m` | Σ ‖pₜ − pₜ₋₁‖ over consecutive tool-tip positions |
| `mean_speed_mps` | mean of instantaneous speed |
| `idle_fraction` | fraction of samples with speed < 0.005 m/s |
| `jaw_cycles` | upward crossings of jaw angle through 0.1 rad |
| `warn_events` / `error_events` | SystemEvents with severity 2 / ≥ 3 in the session |
| `instrument_swaps` | `INSTRUMENT_SWAP` events in the session |

A session starts at a `SESSION_START` event and ends at `SESSION_END`; without those, a
heartbeat gap longer than 60 s splits sessions, and a bag file boundary always does.

## Gesture metrics (JIGSAWS)

`procedures.gestures` aligns the transcript's gesture segments (`G1`…`G15`) to kinematics and
video frames at 30 Hz and computes per segment: duration, PSM left/right path length and mean
speed, gripper open/close cycles, and the mean video motion energy (mean absolute frame
difference) — a cheap video-derived activity measure that can be compared with the
kinematics-derived speed to catch desynchronized recordings.

## SLAs and freshness

| Product | SLA | Freshness check |
|---|---|---|
| `kpi_1m` (stream) | 2 min | `streaming.yml` → `freshness` ≤ 5 min (warning) |
| `*_raw`, `sessions` | daily partition by 06:00 UTC | `telemetry.yml` → `freshness` ≤ 26 h |
| `procedures.*` | on dataset drop | n/a |
| `marts.*` | after the daily ingest DAGs | `freshness` via `dq.catalog`, DAG SLA miss callback |

Freshness and row counts are recomputed into `dq.catalog` whenever the publish step runs, so the
Catalog page is never more stale than the last pipeline run.
