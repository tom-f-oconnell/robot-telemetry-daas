# Web UI

A single-page app (vanilla JS + Plotly, no build step) served by the API at `/`. It is the
self-service surface for engineers who do not want to write SQL.

| Page | What it shows | Backed by |
|---|---|---|
| Sessions | sessions per robot with date filters; economy-of-motion columns | `/api/sessions` |
| Session detail | 3-D tool-tip trajectory per arm with a replay slider; x/y/z/speed/jaw time series; 1-minute KPI bars; event list; **Reprocess** button | `/api/sessions/{id}/…`, `POST /api/jobs/reprocess` |
| Log explorer | free-text + severity/robot/date search over system events | `/api/events` |
| Procedures | JIGSAWS trials filterable by task, skill level, subject | `/api/trials` |
| Trial detail | video player synchronized with PSM kinematics (cursor follows playback), gesture ribbon (click to seek), frame-feature strip, scrubber | `/api/trials/{id}/video`, `/kinematics`, `/frames` |
| Gesture search | find segments by task / gesture code / skill level, jump into the trial at that frame | `/api/gestures` |
| Catalog | every data product with owner, SLA, freshness badge and column docs | `/api/catalog` |
| Quality | latest runs, pass/warn/fail counts, filterable results | `/api/quality/*` |
| Contracts | subjects, versions, pretty-printed Avro schemas | `/api/contracts` |
| Jobs | reprocess requests and their Airflow run ids | `/api/jobs` |

Design notes:

- Everything is driven by the public API, so anything the UI shows can be scripted.
- Video/kinematics sync uses the `<video>` `timeupdate` event and `frame = round(t × fps)`; the
  kinematics are fetched once per trial, downsampled server-side for long trials.
- No framework and no bundler on purpose: the UI is small, and this keeps the image minimal and
  the page inspectable.
