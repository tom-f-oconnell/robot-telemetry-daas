#!/usr/bin/env bash
# Tests rtdaas-bridge on a cluster with no hardware: the dVRK's kinematic simulation in one pod, the real
# bridge Deployment in another, and a consumer decoding what reaches Kafka while the arm moves.
#
# Needs Strimzi's Kafka, the registry with contracts registered, and two images on the cluster's nodes:
#   SIM_IMAGE      any rtdaas-sim image, for the consumer (default rtdaas-sim:dev)
#   BRIDGE_IMAGE   default ghcr.io/tom-f-oconnell/rtdaas-bridge:latest
# and rtdaas-dvrk-sim:latest, built from infra/docker/dvrk-sim.Dockerfile (see infra/k8s/testing/dvrk-sim.yaml).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SIM_IMAGE="${SIM_IMAGE:-rtdaas-sim:dev}"
BRIDGE_IMAGE="${BRIDGE_IMAGE:-ghcr.io/tom-f-oconnell/rtdaas-bridge:latest}"
ROS='source /opt/ros/jazzy/setup.bash; source /dvrk_ws/install/setup.bash; source /ws/install/setup.bash'
log() { printf '[bridge check] %s\n' "$*"; }

log "starting the simulated dVRK"
kubectl apply -f "$ROOT/infra/k8s/testing/dvrk-sim.yaml" >/dev/null
kubectl -n rtdaas rollout status deployment/dvrk-sim --timeout=5m >/dev/null
until kubectl -n rtdaas logs deploy/dvrk-sim 2>/dev/null | grep -q "fleet_monitor for dv_001"; do sleep 5; done

# After the simulated robot: the bridge resolves its DDS peer once, when it starts.
log "starting the bridge"
sed -e "s#ghcr.io/OWNER/rtdaas-bridge:latest#$BRIDGE_IMAGE#" -e 's/replicas: 0/replicas: 1/' \
  "$ROOT/infra/k8s/manifests/rtdaas/bridge.yaml" | kubectl apply -f - >/dev/null
kubectl -n rtdaas rollout restart deployment/rtdaas-bridge >/dev/null
kubectl -n rtdaas rollout status deployment/rtdaas-bridge --timeout=5m >/dev/null

log "moving the arm with the dVRK's own test script"
kubectl -n rtdaas exec deploy/dvrk-sim -- bash -c "$ROS; timeout 150 ros2 run dvrk_python dvrk_arm_test.py -a PSM1 --ros-args -r __ns:=/dv_001" \
  >/tmp/dvrk-arm-test.log 2>&1 &
mover=$!

log "reading Kafka"
kubectl -n rtdaas delete pod bridge-check --ignore-not-found --wait=true >/dev/null
kubectl -n rtdaas run bridge-check --image="$SIM_IMAGE" --image-pull-policy=IfNotPresent --restart=Never \
  --env=KAFKA_BOOTSTRAP=rtdaas-kafka-bootstrap.kafka.svc.cluster.local:9092 \
  --env=SCHEMA_REGISTRY_URL=http://apicurio.rtdaas.svc.cluster.local:8080/apis/ccompat/v7 \
  --command -- python -c "$(cat <<'PY'
import collections
import functools
import os
import sys
import time

import numpy as np
from confluent_kafka import Consumer

from rtdaas.contracts.registry import RegistryClient
from rtdaas.contracts.wire import decode_wire

schema_for_id = functools.cache(RegistryClient(os.environ["SCHEMA_REGISTRY_URL"]).by_id)
consumer = Consumer(
    {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
        "group.id": "bridge-check",
        "auto.offset.reset": "latest",
    }
)
consumer.subscribe([f"rtdaas.telemetry.{t}" for t in ("joint_states", "tool_pose", "events", "heartbeat")])
counts, keys, poses, sample = collections.Counter(), collections.Counter(), [], {}
end = time.time() + 75
while time.time() < end:
    message = consumer.poll(1.0)
    if message is None or message.error():
        continue
    topic = message.topic().rsplit(".", 1)[-1]
    counts[topic] += 1
    keys[(message.key() or b"").decode()] += 1
    _, record = decode_wire(message.value(), schema_for_id)
    sample.setdefault(topic, record)
    if topic == "tool_pose":
        position = record["msg"]["position"]
        poses.append((position["x"], position["y"], position["z"]))
consumer.commit()
consumer.close()
print("messages by topic:", dict(counts))
print("keys:", dict(keys))
for topic, record in sorted(sample.items()):
    print(topic, "sample:", str(record)[:260])
travel = np.ptp(np.array(poses), axis=0) if poses else np.zeros(3)
print("tool tip travel in x, y, z (m):", np.round(travel, 4).tolist())
ok = counts["tool_pose"] and counts["joint_states"] and counts["heartbeat"] and set(keys) == {"dv_001"}
sys.exit(0 if ok and travel.max() > 0.005 else 1)
PY
)" >/dev/null
kubectl -n rtdaas wait pod/bridge-check --for=jsonpath='{.status.phase}'=Running --timeout=3m >/dev/null || true
until phase=$(kubectl -n rtdaas get pod bridge-check -o jsonpath='{.status.phase}') && [[ "$phase" == Succeeded || "$phase" == Failed ]]; do sleep 5; done
kubectl -n rtdaas logs bridge-check
wait "$mover" || true
tail -n 3 /tmp/dvrk-arm-test.log | cut -c1-120
kubectl -n rtdaas logs deploy/rtdaas-bridge --tail=1 | cut -c1-120
log "consumer: $phase"
[[ "$phase" == Succeeded ]]
