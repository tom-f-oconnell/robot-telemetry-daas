# Verification status

This page records what has actually been executed and where, so nobody mistakes "written" for
"tested". The repository was built in a sandbox without network access, PyPI, Docker, Spark,
Kafka or ROS; everything that could run there did, and everything else is wired to run in CI
or on the compose stack.

## Executed and passing (sandbox, Python 3.13)

| Area | How verified | Result |
|---|---|---|
| Contracts: `.msg` parsing, Avro generation, Avro binary codec, wire format, compatibility checker, registry client (mocked HTTP), CDR codec | `tests/contracts` (83 tests, golden byte vectors incl. the `std_msgs/String` CDR vector) | pass |
| Contract gate end to end | `rtdaas-contracts check` with a field added **without** a default → exit 2 with `$.msg.force_n: reader_field_missing_default`; **with** a default → exit 0; base given as a git ref (`HEAD`) → works | pass |
| MCAP writer/reader | `tests/mcap_lite` (round trip, record layout, summary/statistics, footer + magic) | pass |
| Simulator | `tests/sim`: every produced message validates against the generated Avro schemas; CLI writes bags that decode with the CDR codec; deterministic per seed | pass |
| Metrics (sessions, KPI windows, session splitting, gesture metrics) | `tests/metrics` with analytic expectations (constant-speed line, idle segments, 1-minute windows, heartbeat gaps) | pass |
| JIGSAWS parsing, synthetic dataset, OpenCV frame features | `tests/jigsaws`: layout, 76-column parsing, 1-based transcript labelling, H.264 video via ffprobe, video motion energy correlates with kinematic speed (r > 0.3) | pass |
| Quality engine | `tests/quality`: every check type pass + fail, partition filters, SQLite adapter, the three shipped suites | pass |
| Local pipeline | `tests/local`: 2 robots × 70 s bags + 3 trials → all 11 spec tables, idempotent re-run, CLIs | pass |
| `decode_bag` (the Spark unit boundary) | `tests/spark/test_decode.py`: decodes simulator bags, including with an **empty** fallback registry (embedded `ros2msg` schemas are used) | pass |
| DDL ↔ spec consistency | `tests/spark/test_ddl.py` | pass |
| Data API | `tests/serving`: every route, 400/404 paths, Range requests (206/416), OpenAPI ↔ routes parity, metrics | pass |
| Web UI | Playwright (Chromium) against a running API: all 10 pages render; trial page plays the H.264 video and the kinematics cursor follows playback (`t = 1.50 s · frame 45 · G7`); the only console error was the Plotly CDN being blocked in the sandbox (charts degrade to a notice) | pass |
| End-to-end demo without a cluster | `rtdaas-sim` → `rtdaas-jigsaws synth` → `rtdaas-local pipeline` → `rtdaas-api` | 10,800 pose rows, 6 sessions, 12 KPI windows, 3 trials, 10 gestures, 61 checks (0 blocking failures) |
| Infra YAML | 52 files under `infra/` and `.github/` parse as YAML; shell scripts pass `bash -n`; `ros2/check-msgs-in-sync.sh` passes | pass |
| ROS adapters' pure helpers | `ros2/rtdaas_adapters/test` (15 tests) | pass |

Total: **140 tests passing** in the unit tier (`make test`).

## Executed in GitHub Actions (real dependencies)

After the first push, the CI matrix ran with freshly installed dependencies on `ubuntu-latest`
(run history: github.com/tom-f-oconnell/robot-telemetry-daas/actions):

| Job | What it exercised | Result |
|---|---|---|
| `ci / unit` (Python 3.11 and 3.12) | the full unit tier above, plus `rtdaas-contracts gen && git diff --exit-code` (committed Avro is current) | pass |
| `ci / spark` (PySpark 3.5.3, Temurin 17) | `tests/spark/test_jobs.py`: Spark `StructType` for every Iceberg table, the streaming KPI aggregation (`unify → kpi_aggregate → fold_robot_wide`) on a static frame with warn-event and heartbeat-gap folding, `sc.binaryFiles → decode_bag → createDataFrame` on simulator bags | pass |
| `ci / airflow` (Airflow 2.10.3, official constraints) | `DagBag` loads all 5 DAGs with no import errors; expected task ids, tags, `max_active_runs`, `catchup=False`; `S3KeySensor` key pattern | pass |
| `ci / lint`, `ci / docs` | ruff check + format, `mkdocs build --strict` | pass |
| `integration` (3-node kind cluster, Kubernetes 1.32) | Strimzi operator + single-broker KRaft Kafka with the 4 `KafkaTopic` CRs, Apicurio registry, the object store manifest, `rtdaas-contracts register` against the real registry API, the API Deployment from `infra/k8s/manifests/rtdaas`, the simulator producing Avro/Confluent-wire records to Kafka for 60 s, `kcat` reading keyed records back | pass |
| `deploy` | after each image build, pins the new tags in `kustomization.yaml` and commits (Argo CD hand-off) | pass |
| `images` | all five images built with buildx and pushed to GHCR — including `rtdaas-bridge`, i.e. the C++ ROS 2 → Kafka bridge, the rclpy adapters and the launch package compiled with `colcon` against ROS 2 Jazzy, avro-cpp 1.11.3 and librdkafka | pass |

Getting there took four short iterations, all in the thin layers, exactly as expected: a
collection-time import of Starlette in the serving tests (now skipped without the `api`
extra), ruff findings, and two `-Werror` warnings in avro-cpp's source build under GCC 13 /
Boost 1.83 (built as C++17 with those diagnostics relaxed; only the library targets are built),
and MinIO having withdrawn its public container images (every `minio/*` tag fails to pull), which
replaced the MinIO Helm chart with a plain manifest and a frozen MinIO build.

## Executed on the compose stack (`make demo`, Linux laptop, 2026-10-30)

| Area | What ran | Result |
|---|---|---|
| Kafka producer | `rtdaas-sim --kafka`, 3 robots × 600 s at 50 Hz, Confluent-wire Avro against Apicurio | pass |
| Streaming (`streaming_kpis`) | Kafka → Iceberg `MERGE` (raw tables, `kpi_1m`) + ES bulk (`rtdaas-events`, `rtdaas-kpi-1m`), 30 s trigger | pass |
| `contracts_sync` DAG | check committed contracts → set compatibility + register → verify registry | pass |
| `jigsaws_ingest` DAG | discover (S3) → `batch_jigsaws` (6 trials, video features) → `quality_gate` (procedures) → `publish_snowflake --target duckdb` → ES gesture check | pass |
| `telemetry_bag_ingest` DAG | `S3KeySensor` → `batch_mcap` (2 bags, 884k pose rows) → `quality_gate` (telemetry) → publish → ES session check | pass |
| `telemetry_backfill` DAG | one-day range: mapped `batch_mcap` + gate, then publish | pass |
| `dq_daily` DAG | mapped gates over all three suites (telemetry correctly blocks on an empty partition), `iceberg_maintenance` (compaction 15 → 3 files, snapshot expiry, orphan scan) | pass |
| DuckDB API backend | every GET route against the live Iceberg catalog (incl. trajectory, kinematics, video with Range); catalog re-attached while running so new commits show up without a restart | pass |
| Elasticsearch client | events / gestures search through the API; KPI and session indices | pass |
| C++ bridge at runtime | CI-built `rtdaas-bridge` image, `fleet_sim.launch.py launch_dvrk:=false robot_ids:=dv_001`, dVRK-shaped `measured_cp` / `measured_js` / `jaw/measured_js` published by hand: adapters → bridge → Kafka (records decode with fastavro against the registry) → streaming → API | pass |

Reaching that took fixes across the thin layers: compose
healthchecks and bind-mount ownership, the Spark image's Python (3.10 → 3.11, matching the
Airflow driver) and an empty `UNKNOWN` package install, connector jars for the client-mode driver
in the Airflow image, the `spark_default` connection, temp views inside `foreachBatch`, the
`marts.sql` header regex, the DuckDB publish target and `pytz`, the Iceberg catalog going stale in
the API, `s3://` for Hadoop, `s3a://` video paths, and the bridge params template.

## Written, compiles, not yet executed

| Area | Why not yet | Where it runs |
|---|---|---|
| The fleet launch with its default robot ids | `dv-001` is not a valid ROS 2 namespace / topic segment (hyphen), and the bag pipeline derives `robot_id` from the topic path, so the fix is a naming decision, not a patch | ROS 2 side + `rtdaas/local/bags.py`, `rtdaas/spark/decode.py` |
| The C++ bridge against *live* dVRK topics (only hand-published messages of the same types so far) | needs the dVRK ROS 2 workspace | `ros2 launch rtdaas_launch fleet_sim.launch.py` against the compose stack |
| Snowflake publish (the DuckDB fallback is exercised) | needs Snowflake credentials | `publish_snowflake.py --target snowflake` |
| Terraform, Ansible, the Argo CD applications and Helm values for Airflow / spark-operator / ECK / kube-prometheus-stack | no VMs in CI | `make tf-local` + `make ansible` |

## What to run first after cloning

```bash
make install && make test             # should match the table above
make synth && make local-pipeline && make api
make demo                             # exercises Kafka/Spark/Airflow/ES/Iceberg for real
```

If something in the second tier fails, the fix almost always belongs in one of the thin wrappers
(`rtdaas/spark/*.py`, `rtdaas/airflow/dags/*.py`, `infra/*`), not in the tested core.
