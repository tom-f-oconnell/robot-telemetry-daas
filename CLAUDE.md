# CLAUDE.md — handoff for Claude Code

Last updated 2026-10-30 (after `make demo` and the first k3s run on real VMs; initial build 2026-10-29). Read this first; then
`docs/architecture/spec.md` (the interface/schema contract everything is built against) and
`docs/dev/verification.md` (what has actually executed, where).

## What this is

`robot-telemetry-daas` is a portfolio-grade Data-as-a-Service platform for robot telemetry,
built to showcase the stack in an Intuitive "Software Engineer (DaaS)" posting: ROS 2 →
Kafka → Spark (streaming + batch) → Iceberg / Elasticsearch, orchestrated by
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
  landing/         rtdaas-land: closed MCAP bags → s3://<raw>/bags/<robot>/<yyyy>/<mm>/<dd>/ (boto3, [land] extra)
  jigsaws/         JIGSAWS layout parser, synthetic dataset generator (H.264 video), importer for the real download, OpenCV frame features
  metrics/         session "economy of motion" metrics, 1-min KPI windows, gesture metrics, da Vinci arm kinematics (pure pandas/numpy)
  quality/         YAML check suites + one engine; adapters for pandas / SQLite / Spark
  local/           serial pure-Python pipeline → SQLite mirror of every Iceberg table (dev/test/demo)
  spark/           thin PySpark jobs: streaming_kpis, batch_mcap, batch_jigsaws, quality_gate, publish_marts, maintenance
  airflow/dags/    5 DAGs (telemetry_bag_ingest, telemetry_backfill, jigsaws_ingest, dq_daily, contracts_sync)
  serving/         Starlette API (hand-maintained openapi.yaml + parity test), sqlite/duckdb backends, vanilla-JS UI
ros2/              rtdaas_msgs, rtdaas_kafka_bridge (C++, rosidl introspection → Avro → librdkafka), rtdaas_adapters, rtdaas_launch
infra/             docker/, compose/, terraform/ (libvirt + aws), ansible/ (k3s + Argo CD), k8s/ (argocd apps, manifests)
quality/checks/    telemetry.yml, procedures.yml, streaming.yml
docs/              mkdocs-material site: architecture, pipelines, infra, runbooks, 11 ADRs, verification status
tests/             pytest; markers: spark, airflow, integration (unmarked = pure Python)
scripts/           demo.sh (compose orchestration), ci-annotate.sh (see CI notes), wait-for.sh, check-k8s-video.sh
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
make test               # unit tier (186 tests, ~10 s) — must stay green
make lint / make format # ruff (CI enforces check + format)
make synth && make local-pipeline && make api   # no-cluster demo → http://localhost:8080
make demo               # full compose stack (needs Docker, ~16 GB RAM); exercised 2026-10-30, see verification.md
# `.env` (git-ignored; see .env.example) sets machine-specific values, e.g. JIGSAWS_ROOT to use the real dataset
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

Verified on the compose stack (`make demo`, 2026-10-30; details in `docs/dev/verification.md`): sim → Kafka →
streaming → Iceberg + ES; all 5 DAGs run green (dq_daily's telemetry gate correctly blocks an empty day);
publish → DuckDB; every API route on the DuckDB backend against live Iceberg; the CI-built `rtdaas-bridge`
image at runtime (`launch_dvrk:=false`, default ids, dVRK-shaped topics published by hand) through to the
API. ~15 fixes, all in the thin layers. Then a clean `make demo` (empty volumes and `./data`) ran unattended;
it surfaced the batch/stream MERGE conflict below, fixed and A/B-verified.

Also verified 2026-10-30: the `integration` workflow on the new images; the bridge against the real dVRK
kinematic simulation (`infra/docker/dvrk-sim.Dockerfile`, see ros2/README.md) through to Iceberg/ES with arm
motion and fleet_monitor's CRTK mode; marts published to Iceberg (ADR 0012), multi-date and idempotent.

Verified on real VMs (libvirt + k3s, 2026-10-30; see verification.md): Terraform + Ansible from zero, all 12 Argo CD
apps Synced and Healthy, streaming on the Spark operator, all 5 DAGs in `k8s` mode, Ingress, Prometheus.

Verified with the real JIGSAWS dataset (2026-10-31; see verification.md and docs/pipelines/jigsaws.md): import, the local
pipeline, and `jigsaws_ingest` on the compose stack, with the UI's player in Chrome. Not yet on k3s.

Open (highest-value next work):
1. `envs/aws` has never run, and its Airflow S3 connection still points at MinIO. On k8s, nothing scrapes Kafka's
   metrics yet, and `rtdaas-bridge` stays at `replicas: 0` (it needs a dVRK on the ROS domain).
2. `gripper_cycles` is not meaningful on real JIGSAWS data: angles run -1.32 to 0.54 rad, so `GRIPPER_OPEN = 0.5`
   (chosen for the synthetic 0 to 1) only sees the jaw fully open, and 62 % of segments score 0.
3. A batch merge and the stream writing the *same robot on the same day* can still conflict (merges are scoped
   by day and robot); Airflow retries cover it. The introspection walk's bounded-sequence branch is unexercised.

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

From the first `make demo` (each is fixed; the notes are so the fix is not undone):

- **Client mode**: Airflow submits with `--deploy-mode client`, so the *driver runs in the Airflow container*. It
  needs the same connector jars as the cluster (`infra/docker/spark-jars.sh`, shared by both Dockerfiles) and the
  same Python minor version as the executors (3.11; the Spark image adds it from deadsnakes into `/opt/venv`
  because `apache/spark` ships jammy's 3.10). `spark.jars.packages` set from Python is too late under
  `spark-submit` — the JVM is already up. Old pip on that base silently installs pyproject packages as `UNKNOWN`.
- The Spark provider builds `--master` as `<host>:<port>`, so the `spark_default` connection's host must carry
  `spark://` (compose uses the JSON connection form). A missing connection silently means `--master yarn`.
- `foreachBatch` hands a *cloned* session: temp views must be created and queried through `df.sparkSession`.
- DuckDB's Iceberg `ATTACH` is a snapshot of the catalog: the API re-attaches after 30 s
  (`ICEBERG_REFRESH_S`). DuckDB returns `TIMESTAMPTZ` via `pytz`, which pandas 3 no longer installs.
- The REST catalog reports `s3://` locations; Hadoop needs `fs.s3.impl` = S3A for `remove_orphan_files`.
- `./data` is a host bind mount: api/sim run as `RTDAAS_UID:RTDAAS_GID`, Airflow as `RTDAAS_UID:0`
  (`demo.sh` exports `id -u`/`id -g`; `airflow-init` runs as root to hand over the logs volume).
- Unpausing a `catchup=False` scheduled DAG immediately creates a run for the last complete interval, and with
  `max_active_runs=1` a manual run queues behind it — `demo.sh` seeds *yesterday's* bags for that run. Airflow
  also won't start a manual run whose logical date is in the future.
- `iceberg-rest` ships no curl (healthcheck speaks HTTP over bash `/dev/tcp`); `compose run` of a profiled
  service needs the `--profile` flags even with `--no-deps`; Spark wants `--trigger "30 seconds"`, not `30s`.
- `str.format` templates substitute inside comments too (`bridge.yaml.template`: double the braces).
- Airflow DAG args: a Jinja arg that renders to `""` becomes a stray positional — always pass flag/value pairs.
- Clearing a task that is already `up_for_retry` after a restart kills its retry mid-run; let it retry.
- **Batch and stream write the same Iceberg tables.** A serializable `MERGE` whose `ON` has no partition
  predicate conflicts with *any* concurrent commit ("Found conflicting files that can contain records matching
  true"), so batch_mcap failed whenever the stream appended mid-merge. `merge_iceberg(..., time_column=)` bounds
  the target to the source's days (`merge_on_clause`); pass it for every table both sides write (raw tables,
  `kpi_1m`). A/B on the live catalog: unbounded 6/6 conflicted, bounded 0/6. Multiple writer *threads* in one
  driver additionally need separate Spark sessions (Iceberg "runtime file filtering" error).
- **Liveness is message arrival.** CRTK `operating_state` publishes on change, so without a timeout a dead
  dVRK kept reporting `ready` and the pose adapter kept republishing its last pose *and last speed* (phantom
  motion; the stream appended the copies). `fleet_monitor` now marks a silent arm `offline` and restores its
  previous state on recovery; going silent is not `FAULT_CLEARED` and coming back in fault is not a new `ESTOP`
  (`StateTracker`, tested without ROS). Heartbeat.state gained `"offline"` (doc-only contract change).
- **rclpy raises "Logger severity cannot be changed between calls"** when one logging call site is used with
  different severities: one call per severity. `fleet_monitor._emit` had this and would have died on the first
  WARN/ERROR event (e.g. `JOINT_LIMIT_WARN`) on a real dVRK.
- **The Iceberg catalog lives on the `iceberg-catalog` volume.** tabulario/iceberg-rest's default
  `CATALOG_URI` is `jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory` — a *file* (mode=memory is part of the
  name) in the container layer, so `make demo-down` dropped every table definition while the data stayed in
  MinIO. Compose mounts the volume at `/home/iceberg` (owned by the image user, so the volume inherits it).
  After a down/up Airflow still remembers which runs succeeded: clear a run to re-ingest it.
- **The simulator turns SIGTERM into a normal stop** (`docker stop`, `make demo-down`): Python's default
  SIGTERM exits without unwinding, which left bags without their footer — never "closed", never landed.
  `rtdaas-land` warns about open bags unchanged for `--stale-after` s.
- **The UI only draws charts when the Plotly CDN loads**, so chart code went unexercised in the offline build
  sandbox: it called `window.plot`/`restyle`/`relayout` (Plotly's API is `window.Plotly.*`) and stacked panels
  with grid `"independent"` and `yaxis: "y1"` (overlapping axes). Stacked time panels use `"coupled"`.
- **Elasticsearch gets events and KPI windows from both the stream and `batch_mcap`**, under shared ids
  (`es_event_id`, `es_kpi_id`); before, bag events never reached ES, and the DuckDB backend serves events from
  ES whenever it is reachable, so they were invisible in the API/UI.
- **The Iceberg REST catalog is SQLite: one writer at a time.** Without a busy timeout, a commit that meets the
  lock fails at once (500 "database is locked"), which concurrent jobs (stream + backfill + publish) hit and
  which killed the streaming query. `CATALOG_URI=...db?journal_mode=WAL&busy_timeout=30000` (compose and k8s).
- **Merges are scoped by day *and* robot** (`merge_on_clause(time_column, scope_column)`): the raw tables are
  partitioned by `(days(ts), robot_id)`, so a backfill of today's bags no longer conflicts with the stream
  writing other robots.
- **Marts live in Iceberg (`lake.marts.*`), not Snowflake** (ADR 0012): Snowflake could not read the
  compose/k3s MinIO stage, its connector was missing from the client-mode driver, and password logins for
  service users are gone. The DuckDB-file target and the API's stage fallback (whose paths never matched) went
  with it. Marts' `day` is a DATE: `to_iso` and the catalog handle dates.
- **Against the real dVRK**: `dvrk_system` needs a GL-capable X server (Xvfb + Mesa; `offscreen` segfaults);
  `operating_state` is latched (TRANSIENT_LOCAL) and on-change only, so fleet_monitor subscribes TRANSIENT_LOCAL;
  `measured_cp` (~100 Hz) is jittery and sometimes repeats a stamp, so the pose adapter publishes each pose once
  and skips stamps equal at µs precision (the stored key) — a fixed-rate republish made ~1.7 % duplicate keys.
- **Robot ids are ROS names**: `dv_001`, not `dv-001` (renamed fleet-wide 2026-10-30). The id is the ROS
  namespace and the bag pipeline reads `robot_id` back from the topic path, so one form must serve both;
  ROS 2 names allow no `-`.

From the first k3s run on real VMs (each is fixed; the notes are so the fix is not undone):

- **Argo CD uses annotation tracking.** With the default label, Strimzi copies the instance label from the Kafka CR to
  what it creates, and Argo CD prunes those objects as its own: the PVCs vanished and brokers stayed Pending.
- **ECK cannot run with `xpack.security.enabled: false`**: its file-based settings fail to load, and the readiness
  port never opens. Elasticsearch keeps security on with an anonymous superuser (same no-credentials access).
- **libvirt**: pin provider `~> 0.8.0` (0.9 is a rewrite). Root disks attach by `file`, not `volume_id` (with a pool
  volume, AppArmor blocks the backing image: "Permission denied"), and the provider attaches a file as raw, hence
  `qcow2-disk.xsl`. A failed create leaves a defined domain outside the state: `virsh undefine` it.
- **Don't put the VM disks on an auto-snapshotted ZFS pool**: snapshots pin what the VMs write and delete, so free
  space fell far faster than the disks grew, and deleting things freed nothing until they expired. `pool_path`.
- **`SparkKubernetesOperator` reads any templated string ending in `.yml` as a template file** (`--suite x.yml`:
  TemplateNotFound), hence `template_ext = ()` in `_common.py`. DAGs import `_common` from next to the file first, so
  a git-synced DAG and its helper change together.
- **Sizing**: the stack reserves ~28 GB and a batch job ~7 GB (Spark adds 40 % overhead), so 12 GB per VM. A Pending
  executor is a capacity problem, and a batch driver can take the slot of a Recreate rollout, leaving the API down.
- **The Spark operator runs a spark-submit JVM per submission in its own pod**: 2 Gi. When it was OOM-killed, jobs in
  flight failed with "driver pod already exist" although their checks had passed, and streaming stuck in PENDING_RERUN.
- **Probes use `/api/live`**, and backend handlers are plain `def` (thread pool). `/api/health` counts every table's
  rows, so it outgrew the probe timeout, and in `async def` a blocking DuckDB call stalled the event loop for all.
- **Trial videos stream from the bucket** when `MEDIA_ROOT` has no copy (nothing fills that volume on k8s, so the player
  404ed). Proxied, not presigned: the store's cluster address is unreachable from a browser. The `api` extra needs boto3.
- **One API replica**: a DuckDB file takes one process. The in-memory registry forgets schemas on restart, so a sidecar
  in the Apicurio pod keeps registering them. Airflow keys are fixed in values (the chart's hook Secrets regenerate on
  every sync). Task logs go to the lake bucket (worker pods are deleted, and their logs with them).

From the first run on the real JIGSAWS data (each is fixed; the notes are so the fix is not undone):

- **The pipelines read `rtdaas-jigsaws import`'s copy, not the download**: MPEG-4 AVIs become H.264 (frame for frame), one eye
  per trial, and only the 103 complete trials of the 120 kinematics files. Spark's video glob matches both eyes; last one wins.
- **Video and kinematics lengths never match** (2 to 17 frames, and `Knot_Tying_F005`'s source video is 470 short). Join on
  `frame_idx` and expect gaps at the end; don't assume equal lengths, which synthetic data has by construction.
- **`SparkAdapter.frame` selects only what the checks read** (`psml_x` from `psml.x`). `toPandas` on all of
  procedures.kinematics ran 1 GB executors out of heap, so blocking checks errored and the gate failed.
- **Switching datasets needs a purge**, which `make demo` does: the gesture `MERGE` only upserts, so old segments survive, and
  the API prefers a local video to the bucket's, so `infra/compose/data/jigsaws` would serve synthetic video for real trials.
- **`.env` is read by both make and bash**: plain `KEY=value`, no quotes, spaces or trailing comments.

From adding the da Vinci simulation to the UI (see docs/serving/api.md):

- **`rtdaas/metrics/da_vinci.py` and `static/robot-sim.js` hold the same joints** (dvrk_model's URDF). Tests check the closed
  forms against the URDF written out joint by joint; in a browser, `document.getElementById("sim").robotSim.tipPosition("PSML")`
  must match the API's `tip` (0.01 mm).
- **JIGSAWS' rotation columns are tool axes, column 2 the approach** (wrist bends stay near 10 degrees if so). Which way the jaws
  face cannot be told from the data: all four candidates fit equally. Its arms have a ~5 cm offset, removed via G4; see the docs.
- **An arm's mount cannot be up and outward at zero pitch** when its shaft leans outward, hence `MEAN_PITCH`. With the base
  aimed along the mean shaft, mounts went below the floor.
- **Telemetry's arms are in their own frames, RCM at the origin.** The ROS-free simulator's rotations are near identity, which
  no wrist can reach, so those tools are held straight (`wrist_reachable`).
- **Import three.js dynamically**: a failed static import would take down the whole single-page app, not just the box. Read
  `video.currentTime` every animation frame; `timeupdate` fires at about 4 Hz.

## Useful commands

```bash
rtdaas-contracts gen|check|register|diff          # contracts
rtdaas-sim run --robots 2 --duration 120 --rate 50 --mcap-out data/bags --no-realtime [--kafka]
rtdaas-jigsaws synth --out data/jigsaws --trials 2
rtdaas-local pipeline --bags data/bags --jigsaws data/jigsaws --out data/rtdaas.sqlite
rtdaas-quality run --suite quality/checks/telemetry.yml --backend sqlite --db data/rtdaas.sqlite --out-table
rtdaas-api                                         # SERVING_BACKEND=sqlite|duckdb, see spec §9 for env vars
rtdaas-land --root data/bags [--watch] [--dry-run]  # land closed bags in the raw bucket
gh api repos/tom-f-oconnell/robot-telemetry-daas/actions/runs?per_page=5   # CI status without the UI
```
