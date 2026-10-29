# Runbook: changing a message schema

## Compatible change (add a field with a default, widen a type)

1. Edit the `.msg` in `contracts/msgs/<pkg>/msg/` (and the identical copy in `ros2/rtdaas_msgs/msg/`
   for custom types — `ros2/check-msgs-in-sync.sh` enforces this).
2. `rtdaas-contracts gen` → commit the regenerated `.avsc` files.
3. PR: `contracts.yml` must pass (`gen` diff + `check --mode BACKWARD`).
4. Merge. Deploy order:
    1. `contracts_sync` DAG registers the new version (`rtdaas-contracts register`).
    2. Restart consumers that pin a schema at startup: `SparkApplication/streaming-kpis`
       (`kubectl -n rtdaas delete sparkapplication streaming-kpis` and let Argo CD recreate it).
    3. Roll out producers (bridge / adapters image). Old records remain readable by the new
       schema; new records remain readable by any consumer still on the old schema only if the
       change is also forward-compatible — plan a consumer restart otherwise.
5. If the field should appear in an Iceberg table, add the column in `rtdaas/spark/ddl.py`
   (`ALTER TABLE … ADD COLUMN` is run by `ensure_tables`), in `rtdaas/quality/schemas.py`, in
   `catalog.yml`, and in the local SQLite mirror writer.

## Incompatible change (remove/rename a field, narrow a type)

Do not fight the checker. Create a new topic (`rtdaas.telemetry.tool_pose.v2`) with its own
subject, run both producers during the migration, and retire the old topic once consumers have
moved. Bags are unaffected: each bag embeds the definition it was recorded with and
`decode_bag` decodes with that definition, emitting NULL for columns it lacks.

## Checking what changed

```bash
rtdaas-contracts diff --base origin/main --head contracts/avro
rtdaas-contracts check --base origin/main --head contracts/avro --mode FULL   # stricter than CI
```
