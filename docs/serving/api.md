# Data API

A Starlette application (`rtdaas/serving/app.py`) that serves the published tables and a few
operational endpoints. The OpenAPI document is maintained by hand next to the routes and verified
against them by a test ([ADR 0009](../adr/0009-starlette-api.md)); Swagger UI is at `/docs`.

## Endpoints

| Method & path | Purpose |
|---|---|
| `GET /api/health` | backend type, version, table freshness summary |
| `GET /api/features` | what this deployment shows: whether JIGSAWS data is served, and its tables |
| `GET /api/catalog` | data products: owner, description, freshness target, row count, freshness |
| `GET /api/contracts` | subjects, versions and schemas (from `contracts/avro` or the registry) |
| `GET /api/robots` | robots with first/last seen and session counts |
| `GET /api/robots/{id}/joints?arm&start&end&downsample&limit&offset` | joint positions, velocities and efforts by time, across sessions |
| `GET /api/robots/{id}/poses?arm&start&end&downsample&limit&offset` | tool poses by time, across sessions |
| `GET /api/robots/{id}/heartbeats?start&end&limit&offset` | robot state and uptime by time |
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
| `GET /api/kafka/metrics?minutes=` | brokers, rates, partition health, per-topic sizes, consumer lag and recent history, from `PROMETHEUS_URL`; `available` is false, with a `reason`, when there are none |
| `GET /api/quality/runs`, `GET /api/quality/results` | data-quality runs and per-check results |
| `POST /api/jobs/reprocess` `{"kind": "session"\|"trial"\|"date", "target": "..."}` | triggers the matching Airflow DAG; `GET /api/jobs` lists requests |
| `GET /openapi.json`, `GET /docs`, `GET /metrics` | OpenAPI, Swagger UI, Prometheus |

Errors are JSON `{"error": "...", "detail": "..."}` with 400 for bad parameters and 404 for
unknown ids.

## JIGSAWS data on the site

JIGSAWS' licence restricts it to academic research, so the site says where it appears, and can leave it
out. With `SERVE_JIGSAWS` on, the default, a `JIGSAWS` tag marks what shows it:

| Page | Holds | With `SERVE_JIGSAWS=false` |
|---|---|---|
| Sessions, Log explorer, Kafka, Contracts | Robot telemetry only | Unchanged, and no tag |
| Trials, Gesture search | Only JIGSAWS (or a synthetic stand-in in its format) | Gone. Their routes return 404, and their links redirect to Sessions |
| Example uses | Links to projects built on the API, tagged if they need JIGSAWS | `rtdaas-training-eval` is left out |
| Catalog, Quality, Jobs | Both, tagged per table, check or job | JIGSAWS tables, checks and `trial` jobs are left out, and new `trial` jobs are refused |

The rule is enforced by the API, not the page, so nothing is one `curl` away: `rtdaas/serving/jigsaws.py`
filters the backend, and `catalog.yml`'s `dataset` field says which tables count. `/api/health`,
`/openapi.json` and Swagger follow it too. The UI asks `/api/features` what to show, and shows nothing from
JIGSAWS until it has an answer.

`make demo-no-jigsaws` starts the stack this way, and neither prepares nor ingests a dataset. It does not
delete anything: data from an earlier `make demo` stays in MinIO, Iceberg and Elasticsearch, and those
services' own consoles can still show it. To be sure none is present, start from empty volumes
(`docker compose down -v`). The same setting works anywhere the API runs, for example with `make api`.

The Sessions page's 3-D view is independent of JIGSAWS. The trial page's view uses a constant measured from
it (`DEFAULT_ARM_OFFSET`), and that view is hidden with the page.

## Backends

| `SERVING_BACKEND` | Reads from | Use |
|---|---|---|
| `sqlite` | the mirror written by `rtdaas-local pipeline` | development, tests, demo without Spark |
| `duckdb` | Iceberg tables through DuckDB (REST catalog attach) or the Parquet stage; events/gestures from Elasticsearch when `ES_URL` is set | compose and Kubernetes |

Both implement the same `Backend` protocol (spec §4.10); routes never touch SQL directly.

## da Vinci simulation

Trial and session pages show a 3-D replay of the arms when the data comes from a da Vinci
(`/api/trials/{id}/simulation`, `/api/sessions/{id}/simulation`; `system` is null otherwise, and the
page leaves the box out). It uses dVRK's Classic PSM model, built by `scripts/build-psm-model.py` from
[jhu-dvrk/dvrk_model](https://github.com/jhu-dvrk/dvrk_model) (MIT; see
`static/models/LICENSE.dvrk_model`), and three.js from a CDN. Without the CDN, the box says so and the
page otherwise works.

The data records tool poses, not joints, so `rtdaas/metrics/da_vinci.py` solves for them with inverse
kinematics. Tool tips land where the data says (within 0.01 mm in the browser). The rest depends on the
source:

| Source | Follows | Recorded | Estimated |
|---|---|---|---|
| JIGSAWS | the video, frame for frame | tool tips, tool rotation, gripper | each arm's pivot and base, which way the jaws face, the scene's orientation |
| Telemetry | its own clock and the Replay slider | tool tips, jaw, tool rotation if it fits the wrist | where each arm stands around the table |

The cart is schematic in both. On a trial page the simulation reads the video's clock every frame, its
play button and slider drive the video, and it runs on its own clock only when it has to: no video, a
video that fails, or kinematics that continue after the video ends (`Knot_Tying_F005`, by 15.6 s).

JIGSAWS' two arms are not quite in one frame: at their closest in needle transfers (G4), where the
graspers should meet, they are about 5 cm apart, by an offset close to constant per surgeon. The scene
removes the offset measured from the trial's own G4 segments, or the dataset's median without any.

## Operations

- One replica on the DuckDB backend: its file (holding the `jobs` rows from reprocess requests)
  takes one process.
- `/metrics` exposes request counts and latency histograms per route for Prometheus.
- Video streaming looks for `video_path` under `MEDIA_ROOT`, then streams `s3a://` paths from the
  object store through the API, with Range support. Seeking works either way; the local copy is
  optional.
- Put OIDC in front at the ingress; the API itself has no auth (internal platform).
