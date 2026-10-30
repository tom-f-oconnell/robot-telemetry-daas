# Publishing the curated marts

`rtdaas/spark/publish_marts.py` builds the curated marts in `rtdaas/spark/marts.sql` from the
lake with Spark SQL and writes them back to Iceberg, in their own namespace, then refreshes the
data catalog. It runs as the `publish_marts` task of `telemetry_bag_ingest`, `jigsaws_ingest`
(gesture summary only) and `telemetry_backfill` (every date of the range).

| Iceberg table | Grain | Source | Re-run replaces |
|---|---|---|---|
| `marts.robot_daily` | robot × day | `telemetry.sessions` | that day's partition |
| `marts.session_daily` | session × arm | `telemetry.sessions` | that day's partition |
| `marts.gesture_skill_summary` | task × gesture × skill level | `procedures.gestures` | the whole table |
| `marts.check_results` | check run | `dq.check_results` | that day's partition |

```
lake.telemetry.* / procedures.* / dq.* ──Spark SQL (marts.sql)──▶ lake.marts.<table>
                                         (dynamic partition overwrite on `day`)
catalog.yml + row counts / freshness ──▶ lake.dq.catalog          (GET /api/catalog)
```

Because the marts are ordinary Iceberg tables, anything that reads the lake reads them: the API's
DuckDB backend (`lake.marts.robot_daily`), Spark, PyIceberg, or a query engine such as Trino. No
warehouse account or credentials are involved; see [ADR 0012](../adr/0012-marts-in-iceberg.md) for
why the original Snowflake copy was dropped.

```bash
spark-submit rtdaas/spark/publish_marts.py --date 2026-10-29
spark-submit rtdaas/spark/publish_marts.py --date 2026-10-28,2026-10-29 --marts TELEMETRY.ROBOT_DAILY
```

## Catalog refresh

The same job rewrites `dq.catalog` from `rtdaas/serving/catalog.yml`, filling in row counts and
last-updated timestamps per table (marts included), which the API's `/api/catalog` endpoint serves.
