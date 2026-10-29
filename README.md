# robot-telemetry-daas

**Data-as-a-Service for robot telemetry.** ROS 2 → Kafka → Spark (streaming + batch) →
Iceberg / Elasticsearch / Snowflake, orchestrated by Airflow, with generated-and-enforced data
contracts, quality gates, a documented API and a web UI — deployable on one machine with
docker compose or on a multi-node k3s cluster provisioned by Terraform + Ansible and reconciled
by Argo CD.

![System overview](docs/diagrams/system-overview.svg)

## What's in the box

| Area | Highlights |
|---|---|
| **Sources** | dVRK (da Vinci Research Kit) kinematic simulation in ROS 2; recorded MCAP bags; the JIGSAWS surgical-gesture dataset (kinematics + video). A ROS-free simulator produces wire-identical data for demos and tests. |
| **Contracts** | Avro schemas generated from ROS `.msg` files, BACKWARD-compatibility checked on every PR by a pure-Python Avro resolver, registered in a schema registry (Apicurio, Confluent-compatible). Producers/consumers never embed schemas. |
| **Real-time** | C++ ROS 2 → Kafka bridge (rosidl introspection, any message type), Spark Structured Streaming computing 1-minute KPIs per robot × arm into Iceberg + Elasticsearch. |
| **Batch** | Airflow DAGs: daily bag ingestion (MCAP/CDR decoding → raw tables → sessions with economy-of-motion metrics → exact KPIs), JIGSAWS ingestion (OpenCV frame features joined to 30 Hz kinematics → per-gesture metrics), backfills, daily DQ, contracts sync. |
| **Quality** | YAML check suites (null/range/unique/row-count/sample-rate/schema/freshness/SQL), one engine on pandas / SQLite / Spark, results in `dq.check_results`; blocking failures stop publishing. |
| **Serving** | Starlette API with OpenAPI + Swagger, data catalog with owners/SLAs/freshness, Prometheus metrics; UI with trajectory replay, log search, synchronized video + kinematics, gesture search, quality dashboard, one-click reprocess. |
| **Infra** | Dockerfiles, docker compose (core / demo / observability), Terraform (libvirt + AWS), Ansible (k3s + Argo CD), Argo CD app-of-apps (Strimzi, Apicurio, spark-operator, Airflow, ECK, MinIO, Iceberg REST, kube-prometheus-stack), GitHub Actions (ci, contracts, images, integration, deploy). |

## Quick start

```bash
make install           # pip install -e .[dev,api,video]
make test              # pure-Python unit tests — no cluster required
make synth             # synthetic JIGSAWS dataset + simulated robot bags
make local-pipeline    # bags → sessions/KPIs, JIGSAWS → gestures, quality gates → SQLite
make api               # http://localhost:8080 — UI; /docs for the API
```

Full stack (Kafka, schema registry, Spark, Airflow, Elasticsearch, MinIO, Iceberg REST, API):

```bash
make demo              # docker compose --profile core --profile demo
# Airflow http://localhost:8085 · Spark http://localhost:8082 · API/UI http://localhost:8000
```

Multi-node: `infra/terraform/envs/{local,aws}` → `infra/ansible/site.yml` → Argo CD. See
[docs/infra/kubernetes.md](docs/infra/kubernetes.md).

## Documentation

`mkdocs serve` (or read the markdown under `docs/`):

- [System design](docs/architecture/overview.md) · [Engineering spec](docs/architecture/spec.md) · [ADRs](docs/adr/)
- [Data contracts](docs/architecture/data-contracts.md) · [Data products & catalog](docs/architecture/data-products.md)
- Pipelines: [streaming](docs/pipelines/streaming.md) · [batch bags](docs/pipelines/batch-bags.md) · [JIGSAWS](docs/pipelines/jigsaws.md) · [quality](docs/pipelines/quality.md) · [publish](docs/pipelines/publish.md)
- Infra: [compose](docs/infra/compose.md) · [Kubernetes](docs/infra/kubernetes.md) · [CI/CD](docs/infra/cicd.md) · [ROS 2](docs/infra/ros2.md)
- Runbooks: [backfill](docs/runbooks/backfill.md) · [schema change](docs/runbooks/schema-change.md) · [streaming incident](docs/runbooks/streaming-incident.md)
- [Verification status](docs/dev/verification.md) — what has been executed where

## Repository layout

```
contracts/      .msg definitions (source of truth), generated Avro, registry.yml
rtdaas/         Python package: contracts · mcap_lite · sim · jigsaws · metrics · quality · local · spark · airflow · serving
quality/checks/ data-quality suites
ros2/           ROS 2 packages: rtdaas_msgs · rtdaas_kafka_bridge (C++) · rtdaas_adapters · rtdaas_launch
infra/          docker · compose · terraform · ansible · k8s (argocd, helm-values, manifests)
docs/           mkdocs site with diagrams, runbooks and ADRs
tests/          pytest (markers: spark, airflow, integration)
```

## License

MIT.
