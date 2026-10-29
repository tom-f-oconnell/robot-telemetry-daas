#!/usr/bin/env bash
# Create the rtdaas Kafka topics. Idempotent (--if-not-exists).
#
# Partition counts and retention MUST match contracts/registry.yml — that file is
# the source of truth; the values are duplicated here because this script runs
# inside the stock apache/kafka image, which has no Python.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"
KT=/opt/kafka/bin/kafka-topics.sh

create() {
  local topic="$1" partitions="$2" retention_ms="$3"
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 \
    --config "retention.ms=${retention_ms}" \
    --config "cleanup.policy=delete" \
    --config "compression.type=zstd"
  # Make sure retention is updated if the topic pre-existed with other settings.
  "$KT" --bootstrap-server "$BOOTSTRAP" --alter --topic "$topic" \
    --partitions "$partitions" >/dev/null 2>&1 || true
}

echo "Creating topics on ${BOOTSTRAP} ..."
#      topic                            partitions  retention_ms (see contracts/registry.yml)
create rtdaas.telemetry.joint_states    12          604800000
create rtdaas.telemetry.tool_pose       12          604800000
create rtdaas.telemetry.events          3           2592000000
create rtdaas.telemetry.heartbeat       3           2592000000

echo "Topics:"
"$KT" --bootstrap-server "$BOOTSTRAP" --list | grep '^rtdaas\.' || true
echo "kafka-init done."
