# Runbook: streaming job incident

Symptoms: `rtdaas-kpi-1m` freshness alert (streaming suite), rising consumer lag in Grafana, or
the Spark UI shows growing `batchDuration`.

## Triage in order

1. **Is Kafka receiving data?**
   `kcat -b kafka:9092 -t rtdaas.telemetry.tool_pose -C -o -1 -c 1 -f '%k %h\n'` — if nothing
   arrives, the problem is upstream (bridge/adapters/dVRK); check bridge logs for
   delivery-report errors or a registry fetch failure at startup.
2. **Is the job alive?** `kubectl -n rtdaas get sparkapplication streaming-kpis` /
   `docker compose logs streaming`. A crash loop right after a deploy usually means a schema
   mismatch: the registry has a new subject version and the job's `from_avro` schema is stale →
   restart the job (it fetches the latest at startup).
3. **Is it slow rather than dead?** Check executor count and `inputRowsPerSecond`. Scale by
   editing `executor.instances` in `infra/k8s/manifests/rtdaas/streaming-kpis.yaml`, or raise the
   trigger interval (`--trigger "60 seconds"`) to trade latency for throughput.
4. **Sink failures?** `foreachBatch` fails the whole batch if Elasticsearch or the Iceberg catalog
   is down; the checkpoint guarantees the batch is retried after restart (at-least-once; sinks are
   idempotent). Check ES health (`/_cluster/health`) and the REST catalog (`/v1/config`).

## Recovery

- Restart with the same checkpoint to resume from the committed offsets.
- If the checkpoint is corrupt or offsets expired (retention 7 days), delete the checkpoint
  prefix and start with `--starting-offsets earliest` for a bounded replay, or `latest` and rely
  on the batch path to fill the gap from bags (ADR 0005).
- After recovery, trigger `telemetry_backfill` for the affected dates so exact KPIs replace any
  windows the stream dropped.

## Postmortem checklist

- Add the failure mode to `quality/checks/streaming.yml` if a check could have caught it earlier.
- If the cause was a contract change, confirm the [schema change runbook](schema-change.md)
  order was followed and update it if a step was missing.
