# System design

`robot-telemetry-daas` is an internal data platform for robot telemetry: it ingests ROS 2
telemetry from a fleet of (simulated) da Vinci research arms in real time and in batch, computes
procedure-level metrics, joins robot kinematics with endoscope video, enforces data contracts and
quality gates, and serves the results to engineers through a documented API and a web UI.

![System overview](../diagrams/system-overview.svg)

## The three paths

| Path | Source | Processing | Lands in |
|---|---|---|---|
| **Real-time** | dVRK arms in ROS 2 (`measured_js`, `measured_cp`, jaw) → adapter nodes → `rtdaas_kafka_bridge` | Spark Structured Streaming, 30 s triggers, 1-minute windows per robot × arm | Iceberg `telemetry.*_raw`, `telemetry.kpi_1m`; Elasticsearch `rtdaas-kpi-1m`, `rtdaas-events` |
| **Micro-batch: bags** | `ros2 bag record` MCAP files landed in `s3://rtdaas-raw/bags/<robot>/<yyyy>/<mm>/<dd>/` | Airflow `telemetry_bag_ingest` → `batch_mcap.py` → `quality_gate.py` → `publish_snowflake.py` | Iceberg raw + `telemetry.sessions` + exact `kpi_1m`; ES `rtdaas-sessions`; Snowflake `TELEMETRY.*` |
| **Micro-batch: JIGSAWS** | JIGSAWS (or synthetic) dataset: 76-variable kinematics at 30 Hz, gesture transcripts, stereo video | Airflow `jigsaws_ingest` → `batch_jigsaws.py` (OpenCV frame features on executors) | Iceberg `procedures.*`; ES `rtdaas-gestures`; Snowflake `PROCEDURES.GESTURE_SKILL_SUMMARY` |

The same tables are also produced by `rtdaas-local pipeline`, a serial pure-Python runner that
writes a SQLite mirror — that is how the API, UI and quality suites are tested without a cluster
(see [ADR 0001](../adr/0001-pure-python-core.md)).

## Design principles

1. **Contracts are generated, not written.** ROS `.msg` files are the source of truth; Avro schemas
   and Kafka envelopes are derived, committed, compatibility-checked in CI, and registered in the
   schema registry at deploy time ([data contracts](data-contracts.md)).
2. **Logic lives in pure Python; engines are thin.** Decoding, metrics and quality rules are
   unit-tested functions called from Spark, Airflow and the local runner alike.
3. **Everything is idempotent and keyed.** Raw tables merge on `(robot_id, arm, ts)`, sessions on
   `session_id`, KPIs on `(robot_id, arm, window_start)`; batch results win over streaming
   approximations ([ADR 0005](../adr/0005-batch-wins-idempotent-reprocessing.md)).
4. **Quality is a gate, not a report.** Blocking failures stop publishing; every check result is a
   row in `dq.check_results` and visible in the UI ([quality gates](../pipelines/quality.md)).
5. **Self-service.** Every published table has an owner, description, SLA and column docs in the
   catalog; the API ships its OpenAPI document; the UI needs no SQL.

## Component map

| Layer | Component | Where |
|---|---|---|
| Sources | dVRK kinematic simulation (ROS 2 Jazzy), `tool_pose_adapter`, `fleet_monitor` | `ros2/` |
| Ingest | `rtdaas_kafka_bridge` (C++, rosidl introspection → Avro → librdkafka) | `ros2/rtdaas_kafka_bridge` |
| Ingest (no ROS) | `rtdaas-sim`: fleet simulator producing identical messages to Kafka and MCAP bags | `rtdaas/sim` |
| Contracts | `.msg` → Avro generator, compatibility checker, registry client, CDR codec | `rtdaas/contracts` |
| Bag format | Minimal MCAP writer/reader | `rtdaas/mcap_lite` |
| Streaming | `streaming_kpis.py` | `rtdaas/spark` |
| Batch | `batch_mcap.py`, `batch_jigsaws.py`, `quality_gate.py`, `publish_snowflake.py` | `rtdaas/spark` |
| Orchestration | 5 Airflow DAGs | `rtdaas/airflow/dags` |
| Metrics | economy-of-motion session metrics, KPI windows, gesture metrics | `rtdaas/metrics` |
| Quality | YAML suites + engine with pandas / SQLite / Spark adapters | `rtdaas/quality`, `quality/checks` |
| Serving | Starlette API, SQLite and DuckDB/Iceberg backends, web UI | `rtdaas/serving` |
| Infra | Docker images, docker compose, Terraform, Ansible, Argo CD apps, GitHub Actions | `infra/`, `.github/workflows` |

## Sizing assumptions

- Fleet of 3–20 robots, 3 arms each, 50 Hz tool pose + 50–200 Hz joint states → 10–40k
  messages/s at the top end. Kafka topics have 12 partitions keyed by robot; one Spark streaming
  job with 2–4 executors handles this comfortably because all per-record work is Catalyst
  expressions (`from_avro`, window aggregates).
- Bags: ~1–2 GB per robot-day. `batch_mcap` parallelizes per file; a day backfills in minutes on
  the compose stack.
- JIGSAWS: 101 trials, a few GB of video; one task per trial.

## What is deliberately out of scope

- Authentication/authorization on the API (the platform sits behind the internal ingress; add
  OIDC at the ingress or Starlette middleware).
- Clinical data. Only robot kinematics and bench-top research video are handled; no PHI.
- Exactly-once Kafka semantics; the design is at-least-once with idempotent sinks.
