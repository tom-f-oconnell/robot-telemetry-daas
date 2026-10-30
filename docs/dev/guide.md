# Developer guide

## Layout

```
rtdaas/            Python package (pure-Python core + thin Spark/Airflow/Starlette layers)
contracts/         .msg sources, generated Avro, registry.yml
quality/checks/    data-quality suites (YAML)
ros2/              ROS 2 packages (msgs, C++ bridge, adapters, launch)
infra/             docker, compose, terraform, ansible, k8s
docs/              this site (mkdocs-material)
tests/             pytest; markers spark / airflow / integration / slow
```

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
make install                       # pip install -e .[dev,api,video]
make test                          # unit tier
pip install -e .[spark] && make test-spark   # needs Java 17
```

## Everyday commands

| Task | Command |
|---|---|
| regenerate contracts | `rtdaas-contracts gen` |
| check a contract change | `rtdaas-contracts check --base origin/main --mode BACKWARD` |
| synthetic data | `rtdaas-jigsaws synth --out data/jigsaws` · `rtdaas-sim run --robots 2 --duration 120 --mcap-out data/bags --no-realtime` |
| land bags in the raw bucket | `rtdaas-land --root data/bags` (`--watch`, `--dry-run`) |
| run the pipeline without Spark | `rtdaas-local pipeline --bags data/bags --jigsaws data/jigsaws --out data/rtdaas.sqlite` |
| run a quality suite | `rtdaas-quality run --suite quality/checks/telemetry.yml --backend sqlite --db data/rtdaas.sqlite` |
| serve the API/UI | `rtdaas-api` (SQLite backend by default) |
| full stack | `make demo` |
| docs | `make docs` (mkdocs serve) |

## Where to put things

- A new **metric** → `rtdaas/metrics/` as a pure function on pandas; call it from the Spark job
  with `applyInPandas` and from `rtdaas/local/pipeline.py`. Add a unit test with an analytic
  expectation (constant-speed line → known path length).
- A new **message type** → `.msg` in `contracts/msgs/<pkg>/msg/` (+ `ros2/rtdaas_msgs` if custom),
  a topic in `contracts/registry.yml`, `rtdaas-contracts gen`, a bridge config entry, a decode
  mapping in `rtdaas/spark/decode.py` and the local pipeline, DDL in `rtdaas/spark/ddl.py`,
  schema in `rtdaas/quality/schemas.py`, catalog entry in `rtdaas/serving/catalog.yml`.
- A new **check type** → one function in `rtdaas/quality/engine.py` + tests.
- A new **API endpoint** → backend method (both backends), route in `app.py`, entry in
  `openapi.yaml` (the parity test fails otherwise), UI view if user-facing.
- A new **platform component** → Argo CD Application under `infra/k8s/argocd/apps/`, Helm values
  under `helm-values/`, compose service under the matching profile.

## Conventions

- `from __future__ import annotations`, type hints, `logging`, no `print` in library code.
- Timestamps are UTC everywhere; ROS `Time` converts with
  `pd.Timestamp(sec * 10**9 + nanosec, unit="ns", tz="UTC")`.
- CLIs exit 0 (ok) / 1 (error) / 2 (data failed a gate).
- Every Spark write is a `MERGE` or a partition overwrite — never a blind append to a keyed table.
- Generated files (`contracts/avro`) are committed and CI-checked; do not hand-edit them.

## Running pieces of the stack

```bash
# only Kafka + registry + a producer, to iterate on the bridge or the streaming job
docker compose -f infra/compose/docker-compose.yml --profile core up kafka kafka-init apicurio registry-init
rtdaas-sim run --robots 1 --duration 60 --kafka
spark-submit --packages ... rtdaas/spark/streaming_kpis.py --trigger "10 seconds"
```

## Version pins

Spark 3.5.3 / Iceberg 1.6.1 / Kafka 3.8 / Airflow 2.10.3 / Elasticsearch 8.15 / ROS 2 Jazzy /
Avro 1.11.3. Bump in `pyproject.toml`, `infra/docker/*.Dockerfile`, `infra/compose` and
`rtdaas/spark/common.py` (jar coordinates) together.
