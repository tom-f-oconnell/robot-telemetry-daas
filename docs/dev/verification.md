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

## Written, compiles, not yet executed

| Area | Why not here | Where it runs |
|---|---|---|
| Spark jobs (`streaming_kpis`, `batch_mcap`, `batch_jigsaws`, `quality_gate`, `publish_snowflake`, `maintenance`) | no PySpark/Java jars available offline | CI `spark` job (`tests/spark/test_jobs.py`: struct types, KPI aggregation on a static frame, `binaryFiles → decode_bag`), then the compose stack |
| Airflow DAGs | no Airflow | CI `airflow` job (`tests/airflow/test_dag_integrity.py`: no import errors, expected task ids, sensor pattern) |
| Kafka producer (`rtdaas.sim.producer`), Elasticsearch client, Snowflake/DuckDB loaders, DuckDB API backend | no services, no drivers | compose stack (`make demo`), `integration.yml` |
| C++ Kafka bridge, rclpy adapters' ROS plumbing, launch files | no ROS 2 | `infra/docker/bridge.Dockerfile` build is the compile check; `ros2/README.md` lists the two API details written from documentation |
| Dockerfiles, docker compose, Terraform, Ansible, Argo CD applications, Helm values, Kubernetes manifests | no Docker/Terraform/cluster | `make demo`, `make tf-local` + `make ansible`, `integration.yml` (kind) |
| GitHub Actions workflows | not pushed yet | first push |

## What to run first after cloning

```bash
make install && make test             # should match the table above
make synth && make local-pipeline && make api
make demo                             # exercises Kafka/Spark/Airflow/ES/Iceberg for real
```

If something in the second tier fails, the fix almost always belongs in one of the thin wrappers
(`rtdaas/spark/*.py`, `rtdaas/airflow/dags/*.py`, `infra/*`), not in the tested core.
