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
| `images` | all five images built with buildx and pushed to GHCR — including `rtdaas-bridge`, i.e. the C++ ROS 2 → Kafka bridge, the rclpy adapters and the launch package compiled with `colcon` against ROS 2 Jazzy, avro-cpp 1.11.3 and librdkafka | pass |

Getting there took four short iterations, all in the thin layers, exactly as expected: a
collection-time import of Starlette in the serving tests (now skipped without the `api`
extra), ruff findings, and two `-Werror` warnings in avro-cpp's source build under GCC 13 /
Boost 1.83 (built as C++17 with those diagnostics relaxed; only the library targets are built).

## Written, compiles, not yet executed

| Area | Why not yet | Where it runs |
|---|---|---|
| Spark jobs against a real Iceberg catalog, Kafka source and Elasticsearch sink (`streaming_kpis`, `batch_mcap`, `batch_jigsaws`, `quality_gate`, `publish_snowflake`, `maintenance` end to end) | needs the service stack | compose stack (`make demo`), `integration.yml` |
| Airflow DAG *runs* (the integrity tests only load them) | needs Spark + S3 + the stack | compose stack |
| Kafka producer (`rtdaas.sim.producer`), Elasticsearch client, Snowflake/DuckDB loaders, DuckDB API backend | no services in CI | compose stack |
| The C++ bridge *at runtime* (it compiles and links; the introspection walk has not yet run against live dVRK topics) | needs ROS 2 + the dVRK simulation | `ros2 launch rtdaas_launch fleet_sim.launch.py` against the compose stack |
| Terraform, Ansible, Argo CD applications, Helm values, Kubernetes manifests | no cluster in CI | `make tf-local` + `make ansible`; `integration.yml` covers a kind cluster with Strimzi, Apicurio, MinIO, the simulator and the API |

## What to run first after cloning

```bash
make install && make test             # should match the table above
make synth && make local-pipeline && make api
make demo                             # exercises Kafka/Spark/Airflow/ES/Iceberg for real
```

If something in the second tier fails, the fix almost always belongs in one of the thin wrappers
(`rtdaas/spark/*.py`, `rtdaas/airflow/dags/*.py`, `infra/*`), not in the tested core.
