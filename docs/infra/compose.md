# Local stack with docker compose

`infra/compose/docker-compose.yml` runs the whole platform on one machine (16 GB RAM
recommended). Three profiles:

| Profile | Services |
|---|---|
| `core` | kafka (KRaft), kafka-init, apicurio (schema registry), registry-init, minio + minio-init, iceberg-rest, elasticsearch, spark-master, spark-worker, postgres, airflow-init / webserver / scheduler, api |
| `demo` | sim (3 simulated robots → Kafka + MCAP bags under `./data/bags`), streaming (the Structured Streaming job) |
| `observability` | prometheus, kafka-exporter, grafana (provisioned dashboard), kibana |

```bash
make demo            # = scripts/demo.sh: build images, up core+demo, wait, trigger the ingest DAG
make demo-down
```

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
- `minio-init` creates the `rtdaas-raw` and `rtdaas-lake` buckets.
- `airflow-init` migrates the metadata DB, creates the admin user, sets the Airflow Variable
  `rtdaas_spark_mode=submit` and the `rtdaas_s3` connection pointing at MinIO.

## Images

Built from `infra/docker/*.Dockerfile` with the repo as context:

| Image | Base | Contents |
|---|---|---|
| `rtdaas-api` | python:3.12-slim | `rtdaas[api]`, non-root, `/api/health` healthcheck |
| `rtdaas-spark` | apache/spark 3.5.3 (python3) | `rtdaas[spark]`, OpenCV runtime libs, Iceberg/Kafka/Avro/S3 jars baked into `$SPARK_HOME/jars` |
| `rtdaas-airflow` | apache/airflow 2.10.3 | providers (amazon, apache-spark, cncf-kubernetes), JRE for `spark-submit`, the repo, DAGs |
| `rtdaas-sim` | python:3.12-slim | `rtdaas[kafka]`; entrypoint `rtdaas-sim` |
| `rtdaas-bridge` | ros:jazzy-ros-base | avro-cpp built from source, librdkafka, the ROS 2 packages |

Set `RTDAAS_IMAGE_PREFIX` / `RTDAAS_IMAGE_TAG` to run published GHCR images instead of local builds.

## Typical demo sequence

1. `make demo` — after ~2 minutes the sim is producing ~450 msg/s and the streaming job is
   writing `telemetry.kpi_1m`; the UI's Sessions page fills as bags close (one per robot per run).
2. Airflow → `telemetry_bag_ingest` is triggered by the demo script for today's date once the
   sim has written bags; watch `wait_for_bags → spark_batch_mcap → quality_gate → publish_snowflake`.
3. `rtdaas-jigsaws synth --out data/jigsaws` then trigger `jigsaws_ingest` (or
   `make local-pipeline` to skip Spark and fill the API's SQLite backend directly).
4. Open the Quality and Catalog pages to see the gate results and freshness.
