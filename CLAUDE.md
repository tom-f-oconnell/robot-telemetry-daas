# CLAUDE.md — handoff for Claude Code

Last updated 2026-10-29 (end of the initial build session). Read this first; then
`docs/architecture/spec.md` (the interface/schema contract everything is built against) and
`docs/dev/verification.md` (what has actually executed, where).

## What this is

`robot-telemetry-daas` is a portfolio-grade Data-as-a-Service platform for robot telemetry,
built to showcase the stack in an Intuitive "Software Engineer (DaaS)" posting: ROS 2 →
Kafka → Spark (streaming + batch) → Iceberg / Elasticsearch / Snowflake, orchestrated by
Airflow, with generated-and-enforced data contracts, YAML quality gates, a Starlette API +
UI, and Terraform/Ansible/k3s/Argo CD infra. Data sources: a simulated dVRK (da Vinci
Research Kit) fleet, recorded MCAP bags, and the JIGSAWS surgical kinematics+video dataset.

Owner: Thomas O'Connell (github `tom-f-oconnell`). Repo: github.com/tom-f-oconnell/robot-telemetry-daas.
Images: `ghcr.io/tom-f-oconnell/rtdaas-{api,spark,airflow,sim,bridge}` (built on every push to main).

## Map

```
contracts/         .msg definitions (SOURCE OF TRUTH) → generated Avro (committed, CI-checked) → registry.yml
rtdaas/
  contracts/       .msg parser, Avro gen, pure-Python Avro codec + compat checker, registry client, CDR codec, CLI
  mcap_lite/       dependency-free MCAP v1 writer/reader (chunked + zstd/lz4 via optional libs)
  sim/             ROS-free fleet simulator → Kafka (confluent_kafka) and ros2-bag-compatible MCAP
  jigsaws/         JIGSAWS layout parser, synthetic dataset generator (H.264 video), OpenCV frame features
  metrics/         session "economy of motion" metrics, 1-min KPI windows, gesture metrics (pure pandas/numpy)
  quality/         YAML check suites + one engine; adapters for pandas / SQLite / Spark
  local/           serial pure-Python pipeline → SQLite mirror of every Iceberg table (dev/test/demo)
  spark/           thin PySpark jobs: streaming_kpis, batch_mcap, batch_jigsaws, quality_gate, publish_snowflake, maintenance
  airflow/dags/    5 DAGs (telemetry_bag_ingest, telemetry_backfill, jigsaws_ingest, dq_daily, contracts_sync)
  serving/         Starlette API (hand-maintained openapi.yaml + parity test), sqlite/duckdb backends, vanilla-JS UI
ros2/              rtdaas_msgs, rtdaas_kafka_bridge (C++, rosidl introspection → Avro → librdkafka), rtdaas_adapters, rtdaas_launch
infra/             docker/, compose/, terraform/ (libvirt + aws), ansible/ (k3s + Argo CD), k8s/ (argocd apps, manifests)
quality/checks/    telemetry.yml, procedures.yml, streaming.yml
docs/              mkdocs-material site: architecture, pipelines, infra, runbooks, 11 ADRs, verification status
tests/             pytest; markers: spark, airflow, integration (unmarked = pure Python)
scripts/           demo.sh (compose orchestration), ci-annotate.sh (see CI notes), wait-for.sh
```

## The one design rule

All logic lives in the pure-Python core (`contracts`, `mcap_lite`, `sim`, `jigsaws`,
`metrics`, `quality`, `local`) and is unit-tested without a cluster. Spark jobs and Airflow
DAGs are thin orchestration around those functions (`mapPartitions(decode_bag)`,
`applyInPandas(session_metrics)`, …). `rtdaas.local.pipeline` runs the identical functions
serially into SQLite. When something breaks in Spark/Airflow/infra, the fix almost always
belongs in the thin layer; when a *number* is wrong, fix it in the core and both paths follow.
See ADR 0001.

## Working here

```bash
make install            # pip install -e .[dev,api,video]
make test               # unit tier (140 tests, ~8 s) — must stay green
make lint / make format # ruff (CI enforces check + format)
make synth && make local-pipeline && make api   # no-cluster demo → http://localhost:8080
make demo               # full compose stack (needs Docker, ~16 GB RAM) — NOT YET EXERCISED, see below
pip install -e .[spark] && make test-spark      # needs Java 17
```

Conventions (details in spec §10): UTC everywhere; CLIs exit 0/1/2 (2 = data failed a gate);
every keyed Iceberg write is a `MERGE` or partition overwrite; `from __future__ import
annotations`, logging not print; `contracts/avro` is generated — never hand-edit, run
`rtdaas-contracts gen` and commit; custom `.msg` files exist twice (`contracts/msgs/rtdaas_msgs`
and `ros2/rtdaas_msgs/msg`) and `ros2/check-msgs-in-sync.sh` enforces equality.

Changing a message: edit `.msg` → `rtdaas-contracts gen` → `rtdaas-contracts check --base
origin/main --mode BACKWARD` → also update `rtdaas/spark/ddl.py`, `rtdaas/quality/schemas.py`,
`rtdaas/serving/catalog.yml`, `rtdaas/spark/decode.py`, `rtdaas/local/bags.py` if a table column
changes. The CI `contracts` workflow enforces the first two.

## State as of this handoff

Verified in the build sandbox (no network, no Docker): everything pure-Python end to end —
simulator → bags → local pipeline → SQLite → API → UI (Playwright-checked, video+kinematics
sync works), contract gate rejects a field added without a default, 140 tests.

Verified in GitHub Actions (all green on `main`):
- `ci`: lint, unit (3.11/3.12), docs, **spark** (real PySpark 3.5: KPI aggregation, binaryFiles→decode_bag,
  struct types), **airflow** (Airflow 2.10 DagBag loads all 5 DAGs).
- `images`: all 5 images incl. `rtdaas-bridge` — the C++ bridge compiles against ROS 2 Jazzy + avro-cpp + librdkafka.
- `integration` (manual dispatch): 3-node kind cluster, Strimzi KRaft Kafka + topics, Apicurio, object store,
  `rtdaas-contracts register` against the real registry, API Deployment, simulator → Kafka for 60 s, kcat reads keys.
- `deploy`: pins image tags into `infra/k8s/manifests/rtdaas/kustomization.yaml` after each image build.

NOT yet executed anywhere (highest-value next work):
1. `make demo` — Spark jobs against a real Iceberg REST catalog + Kafka + Elasticsearch, Airflow DAG *runs*,
   publish to DuckDB/Snowflake, the DuckDB API backend. Expect a few rounds of small fixes in
   `rtdaas/spark/common.py` (jar/catalog config), `infra/compose/docker-compose.yml`, the DAG operators
   (`spark_default` connection, `SparkSubmitOperator` args), and `rtdaas/serving/backends/duckdb.py`.
2. The C++ bridge at runtime against the dVRK kinematic simulation (`ros2 launch rtdaas_launch fleet_sim.launch.py`);
   `ros2/README.md` lists the two API details written from docs rather than exercised.
3. Terraform (libvirt/AWS) + Ansible + Argo CD app-of-apps on real VMs.
4. Real JIGSAWS data (request from JHU CIRL; `--root` at it, nothing else changes).

## CI mechanics worth knowing

- Full job logs download from Azure blob storage, which some sandboxes cannot reach. `scripts/ci-annotate.sh
  "<label>" -- <cmd>` tees a command's output and, on failure, emits its tail as `::error` annotations readable via
  `gh api repos/{o}/{r}/check-runs/{job_id}/annotations`. Most failing steps are wrapped with it; wrap new ones.
  GitHub shows at most 10 annotations per step — pack output into few annotations, don't emit one per line.
- `lint-autofix` (workflow_dispatch) runs `ruff --fix` + `ruff format` and commits back; `probe-images`
  (workflow_dispatch, input: image refs) reports which container images resolve. Both saved hours; keep them.
- The `deploy` workflow and `lint-autofix` push bot commits to `main` → always `git pull --rebase origin main`
  before pushing. Bot pushes use `GITHUB_TOKEN` and therefore do not trigger other workflows.
- Commits in this session end with `Co-Authored-By: Claude …` trailers; keep doing that for Claude-authored commits.

## Gotchas discovered (don't rediscover them)

- **MinIO withdrew all public images in 2025** (`minio/*`, `quay.io/minio/*` → 401/"not found"; the Helm chart's
  default image too). Object store is a plain manifest (`infra/k8s/manifests/minio/minio.yaml`) and compose service
  using the frozen `bitnamilegacy/minio:latest` with `MINIO_DEFAULT_BUCKETS`; `amazon/aws-cli` replaces `mc` for
  bucket init/uploads. RustFS (`rustfs/rustfs`) or real S3 are the documented swaps.
- **avro-cpp 1.11.3** builds with `-Werror`; GCC 13 + Boost 1.83 trip `-Wcpp` and `-Wc++20-compat`. The bridge
  Dockerfile builds it as C++17 with those relaxed and only the `avrocpp avrocpp_s avrogencpp` targets.
- **`rtdaas.config.REPO_ROOT`** must work for pip-installed images: resolution order is `RTDAAS_ROOT` env →
  package parent (editable install) → `/opt/rtdaas` (images) → cwd, picking the first with `contracts/registry.yml`.
- pandas 3.x: `tz_localize`/string timestamps handled in `rtdaas.metrics.kinematics.to_utc`; speeds are computed
  from int64-nanosecond differences (float64 epoch-seconds lost ~0.2 µs — a real bug that was fixed).
- Synthetic JIGSAWS video must be H.264 (`imageio-ffmpeg`) — OpenCV's `mp4v` is unplayable in browsers.
- JIGSAWS transcripts are 1-based inclusive frames; `label_frames` maps to 0-based `frame_idx`.
- Streaming KPIs are approximations by design (`jaw_cycles = 0`, `path_length ≈ mean speed × span`); batch
  overwrites (`computed_by='batch'` wins in the MERGE predicate). ADRs 0005/0006.
- Quality `sql` checks don't get an automatic partition filter — the query uses `{table}` and `{partition}` placeholders.
- UI loads Plotly from a CDN and degrades to a notice when offline; tests never depend on the CDN.
- `tests/serving` skip without the `api` extra (`pytest.importorskip("starlette")`) so the spark/airflow CI jobs collect cleanly.
- Spark tests need `RTDAAS_SPARK_PACKAGES=0` (jars are baked into the image; CI runs local SparkSession without Iceberg).

## Useful commands

```bash
rtdaas-contracts gen|check|register|diff          # contracts
rtdaas-sim run --robots 2 --duration 120 --rate 50 --mcap-out data/bags --no-realtime [--kafka]
rtdaas-jigsaws synth --out data/jigsaws --trials 2
rtdaas-local pipeline --bags data/bags --jigsaws data/jigsaws --out data/rtdaas.sqlite
rtdaas-quality run --suite quality/checks/telemetry.yml --backend sqlite --db data/rtdaas.sqlite --out-table
rtdaas-api                                         # SERVING_BACKEND=sqlite|duckdb, see spec §9 for env vars
gh api repos/tom-f-oconnell/robot-telemetry-daas/actions/runs?per_page=5   # CI status without the UI
```
