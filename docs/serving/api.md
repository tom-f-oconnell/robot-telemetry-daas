# Data API

A Starlette application (`rtdaas/serving/app.py`) that serves the published tables and a few
operational endpoints. The OpenAPI document is maintained by hand next to the routes and verified
against them by a test ([ADR 0009](../adr/0009-starlette-api.md)); Swagger UI is at `/docs`.

## Endpoints

| Method & path | Purpose |
|---|---|
| `GET /api/health` | backend type, version, table freshness summary |
| `GET /api/catalog` | data products: owner, description, SLA, row count, freshness |
| `GET /api/contracts` | subjects, versions and schemas (from `contracts/avro` or the registry) |
| `GET /api/robots` | robots with first/last seen and session counts |
| `GET /api/sessions?robot_id&start&end&limit&offset` | session list |
| `GET /api/sessions/{id}` | one session with per-arm metrics |
| `GET /api/sessions/{id}/trajectory?arm&downsample` | tool-tip trajectory for replay |
| `GET /api/sessions/{id}/kpis` | 1-minute KPI rows in the session |
| `GET /api/events?q&robot_id&severity_min&start&end&limit` | log explorer (Elasticsearch in production, SQLite locally) |
| `GET /api/trials?task&skill_level&subject` | JIGSAWS trials |
| `GET /api/trials/{id}` | trial with gesture segments |
| `GET /api/trials/{id}/kinematics?arms&downsample` | arm traces for the synced view |
| `GET /api/trials/{id}/frames` | per-frame video features |
| `GET /api/trials/{id}/video` | the trial video (HTTP Range supported) |
| `GET /api/gestures?task&gesture&skill_level` | gesture search |
| `GET /api/quality/runs`, `GET /api/quality/results` | data-quality runs and per-check results |
| `POST /api/jobs/reprocess` `{"kind": "session"\|"trial"\|"date", "target": "..."}` | triggers the matching Airflow DAG; `GET /api/jobs` lists requests |
| `GET /openapi.json`, `GET /docs`, `GET /metrics` | OpenAPI, Swagger UI, Prometheus |

Errors are JSON `{"error": "...", "detail": "..."}` with 400 for bad parameters and 404 for
unknown ids.

## Backends

| `SERVING_BACKEND` | Reads from | Use |
|---|---|---|
| `sqlite` | the mirror written by `rtdaas-local pipeline` | development, tests, demo without Spark |
| `duckdb` | Iceberg tables through DuckDB (REST catalog attach) or the Parquet stage; events/gestures from Elasticsearch when `ES_URL` is set | compose and Kubernetes |

Both implement the same `Backend` protocol (spec §4.10); routes never touch SQL directly.

## Operations

- Stateless; scale by replicas. The only writes are `jobs` rows (reprocess requests).
- `/metrics` exposes request counts and latency histograms per route for Prometheus.
- Video streaming resolves `video_path` under `MEDIA_ROOT`; mount the JIGSAWS directory (or an
  S3 gateway) there.
- Put OIDC in front at the ingress; the API itself has no auth (internal platform).
