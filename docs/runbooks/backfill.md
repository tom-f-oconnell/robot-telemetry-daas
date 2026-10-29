# Runbook: backfill and reprocess

Use this when a metric definition changed, a bug was fixed in `rtdaas.metrics`, or bags for past
dates were landed late.

## 1. Decide the scope

| Situation | Scope |
|---|---|
| Bags landed late for one robot/day | that date + robot |
| Metric bug fix (sessions/KPIs only) | every date since the bug was introduced; raw tables untouched |
| Decoder change (`decode_bag`, `.msg` fix) | every date; raw tables are re-merged |

Bump `PIPELINE_VERSION` (env) or the package version so `telemetry.sessions.pipeline_version`
records which run produced each row.

## 2. Trigger

**Airflow UI** → `telemetry_backfill` → *Trigger DAG w/ config*:

```json
{"start_date": "2026-10-01", "end_date": "2026-10-07", "robot_id": null}
```

or from the API/UI: **Session page → Reprocess** posts `{"kind": "date", "target": "2026-10-03"}` to
`POST /api/jobs/reprocess`, which calls Airflow's REST API and records the run under `/api/jobs`.

The DAG expands one `batch_mcap` + `quality_gate` pair per date (max 4 in parallel), then runs
`publish_snowflake` once for the range.

## 3. Verify

- `GET /api/sessions?start=…&end=…` shows the new `pipeline_version` and `processed_at`.
- Quality page → latest run for `telemetry` has no blocking failures.
- Snowflake `TELEMETRY.SESSION_DAILY` row counts per day match `telemetry.sessions`.
- If the result is wrong, Iceberg time travel lets you compare:
  `SELECT … FROM lake.telemetry.sessions VERSION AS OF <snapshot_id>` and roll back with
  `CALL lake.system.rollback_to_snapshot('telemetry.sessions', <snapshot_id>)`.

## 4. Local dry run first

```bash
rtdaas-sim run --robots 2 --duration 120 --mcap-out data/bags --no-realtime
rtdaas-local pipeline --bags data/bags --out data/rtdaas.sqlite
rtdaas-api   # open http://localhost:8080 and check the sessions
```

The local pipeline runs the exact same `decode_bag`, `session_metrics` and `kpi_windows` code.
