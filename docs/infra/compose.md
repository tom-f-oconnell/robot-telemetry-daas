# Local stack with docker compose

`infra/compose/docker-compose.yml` runs the whole platform on one machine (16 GB RAM
recommended). Three profiles:

| Profile | Services |
|---|---|
| `core` | kafka (KRaft), kafka-init, apicurio (schema registry), registry-init, minio + minio-init, iceberg-rest, elasticsearch, spark-master, spark-worker, postgres, airflow-init / webserver / scheduler, api |
| `demo` | sim (3 simulated robots → Kafka + MCAP bags under `./data/bags`), streaming (the Structured Streaming job), bag-lander (`rtdaas-land --watch`: closed bags → `s3://rtdaas-raw/bags/…`) |
| `observability` | prometheus, kafka-exporter, grafana (provisioned dashboard), kibana |

```bash
make demo            # = scripts/demo.sh: build images, up core, seed data, up demo, start the DAGs
make demo-down
```

`./data` (`infra/compose/data`) is a bind mount shared with the host. The containers that write
to it run as your user: `demo.sh` exports `RTDAAS_UID` / `RTDAAS_GID` from `id -u` / `id -g` (set
them in `infra/compose/.env` when running `docker compose` directly).

## Ports on the host

| Service | URL |
|---|---|
| API + UI | http://localhost:8000 (Swagger at `/docs`) |
| Airflow | http://localhost:8085 (airflow / airflow) |
| Spark master UI | http://localhost:8082 |
| Schema registry (ccompat) | http://localhost:8081/apis/ccompat/v7 |
| MinIO console | http://localhost:9001 (minioadmin / minioadmin) |
| Iceberg REST catalog | http://localhost:8181 |
| Elasticsearch | http://localhost:9200 |
| Kafka (external listener) | localhost:29092 |
| Grafana / Prometheus / Kibana | :3000 / :9090 / :5601 (observability profile) |

## What the init containers do

- `kafka-init` creates the four topics with the partition counts and retention from
  `contracts/registry.yml`.
- `registry-init` runs `rtdaas-contracts register` so the subjects exist with BACKWARD
  compatibility before any producer starts.
- `minio-init` (aws-cli) makes sure the `rtdaas-raw` and `rtdaas-lake` buckets exist and writes the layout markers; the same container is reused by `scripts/demo.sh` to upload the synthetic JIGSAWS dataset.
- `airflow-init` migrates the metadata DB, creates the admin user, sets the Airflow Variable
  `rtdaas_spark_mode=submit`, the `rtdaas_s3` connection pointing at MinIO and the
  `spark_default` connection (`spark://spark-master:7077`, client mode). It is the one container
  that runs as root, to hand the logs volume to `RTDAAS_UID` (the other Airflow containers run
  as `RTDAAS_UID:0`, which the official image supports).

## State that survives `make demo-down`

Named volumes keep Kafka, MinIO, Elasticsearch, Postgres (Airflow's metadata) and the Iceberg
catalog (`iceberg-catalog`: the REST catalog's SQLite file; without it, removing the container
dropped every table definition while the data files stayed in MinIO). `docker compose ... down -v`
removes them all; `./data` is a host directory and is left alone.

## Images

Built from `infra/docker/*.Dockerfile` with the repo as context:

| Image | Base | Contents |
|---|---|---|
| `rtdaas-api` | python:3.12-slim | `rtdaas[api]`, non-root, `/api/health` healthcheck |
| `rtdaas-spark` | apache/spark 3.5.3 | Python 3.11 venv (`/opt/venv`, matching the Airflow driver) with `rtdaas[spark]`, OpenCV runtime libs, Iceberg/Kafka/Avro/S3 jars baked into `$SPARK_HOME/jars` |
| `rtdaas-airflow` | apache/airflow 2.10.3 (Python 3.11) | providers (amazon, apache-spark, cncf-kubernetes), JRE + Spark with the same connector jars (jobs run their driver here, client mode), duckdb, the repo, DAGs |
| `rtdaas-sim` | python:3.12-slim | `rtdaas[kafka]`; entrypoint `rtdaas-sim` |
| `rtdaas-bridge` | ros:jazzy-ros-base | avro-cpp built from source, librdkafka, the ROS 2 packages |

Set `RTDAAS_IMAGE_PREFIX` / `RTDAAS_IMAGE_TAG` to run published GHCR images instead of local builds.

## Typical demo sequence

1. `make demo` seeds S3 before starting anything that reads it: a synthetic JIGSAWS dataset
   (`s3://rtdaas-raw/jigsaws`) and a closed batch of simulated bags dated *yesterday* (UTC)
   (`s3://rtdaas-raw/bags/<robot>/<yyyy>/<mm>/<dd>/`). The live sim then streams to Kafka for
   `SIM_DURATION_S` (600 s) and the streaming job writes the raw tables and `telemetry.kpi_1m`
   every 30 s. Its bags close when it exits (also on `docker stop`), and `bag-lander` then lands
   them under today's date, for the next scheduled `telemetry_bag_ingest` run.
2. Airflow: unpausing `telemetry_bag_ingest` immediately schedules yesterday's run, which picks
   up the seeded bags (`wait_for_bags → spark_batch_mcap → quality_gate → publish_marts →
   index_es_sessions`); `jigsaws_ingest` and `contracts_sync` are triggered directly. With two
   workers and the streaming job holding two cores, batch jobs run one at a time — allow ~10 min.
3. `dq_daily` stays paused (unpaused, it fires at once against empty partitions); trigger it from
   the UI once the ingest runs are green.
4. The API's Sessions, Trials, Quality and Catalog pages read the live Iceberg tables; the curated
   marts are Iceberg tables too (`lake.marts.*`), listed on the Catalog page.
