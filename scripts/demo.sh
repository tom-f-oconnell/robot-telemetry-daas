#!/usr/bin/env bash
# demo.sh up|down|status — bring the full compose stack up, seed it, and kick off the first DAG run.
#
#   up      build images (unless RTDAAS_IMAGE_PREFIX points at a registry), start core + demo
#           profiles, wait for the services, generate a synthetic JIGSAWS dataset under
#           infra/compose/data/jigsaws and a batch of simulated bags under infra/compose/data/bags-batch,
#           upload both to MinIO, trigger jigsaws_ingest and telemetry_bag_ingest, and start the
#           live simulator (Kafka) + the streaming job.
#   down    stop everything (volumes kept)
#   status  print service health and the URLs
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
COMPOSE_DIR="$ROOT/infra/compose"
COMPOSE=(docker compose --project-directory "$COMPOSE_DIR" -f "$COMPOSE_DIR/docker-compose.yml")
PROFILES=(--profile core --profile demo)
export RTDAAS_IMAGE_PREFIX="${RTDAAS_IMAGE_PREFIX:-rtdaas}"
export RTDAAS_IMAGE_TAG="${RTDAAS_IMAGE_TAG:-dev}"
# api/sim write to the ./data bind mount; run them as the invoking user so the files stay ours
export RTDAAS_UID="${RTDAAS_UID:-$(id -u)}"
export RTDAAS_GID="${RTDAAS_GID:-$(id -g)}"
AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8085}"
API_URL="${API_URL:-http://localhost:8000}"
WAIT="$HERE/wait-for.sh"

log() { printf '\033[1;35m[demo]\033[0m %s\n' "$*"; }

build_images() {
  if [[ "$RTDAAS_IMAGE_PREFIX" == "rtdaas" ]]; then
    log "building images (set RTDAAS_IMAGE_PREFIX=ghcr.io/<owner> to pull instead)"
    for img in api spark airflow sim; do
      docker build -f "$ROOT/infra/docker/$img.Dockerfile" -t "rtdaas-$img:$RTDAAS_IMAGE_TAG" "$ROOT"
    done
  fi
}

trigger_dag() {  # trigger_dag <dag_id> <json conf>
  curl -fsS -u airflow:airflow -H 'Content-Type: application/json' \
    -X POST "$AIRFLOW_URL/api/v1/dags/$1/dagRuns" -d "{\"conf\": $2}" >/dev/null \
    && log "triggered $1" || log "could not trigger $1 (is Airflow up?)"
}

up() {
  build_images
  mkdir -p "$COMPOSE_DIR/data"
  log "starting core services"
  "${COMPOSE[@]}" --profile core up -d
  "$WAIT" localhost:29092 180
  "$WAIT" http://localhost:8081/apis/ccompat/v7/subjects 180
  "$WAIT" http://localhost:9000/minio/health/live 120
  "$WAIT" http://localhost:8181/v1/config 120
  "$WAIT" http://localhost:9200/_cluster/health 240
  "$WAIT" "$AIRFLOW_URL/health" 300
  "$WAIT" "$API_URL/api/health" 120

  log "generating a synthetic JIGSAWS dataset and uploading it to s3://rtdaas-raw/jigsaws"
  # --profile flags so compose can resolve sim's depends_on (kafka-init etc.), even with --no-deps
  "${COMPOSE[@]}" "${PROFILES[@]}" run --rm --no-deps --entrypoint rtdaas-jigsaws sim synth --out /data/jigsaws --trials 2
  "${COMPOSE[@]}" "${PROFILES[@]}" run --rm --no-deps --entrypoint aws minio-init \
    --endpoint-url http://minio:9000 s3 sync /data/jigsaws s3://rtdaas-raw/jigsaws || true

  # The live simulator only closes its bags when it exits, and nothing lands them in S3, so seed
  # a closed batch (simulated time, no Kafka) for telemetry_bag_ingest. Date it yesterday (UTC):
  # unpausing the @daily, catchup=False DAG immediately schedules the latest complete interval,
  # i.e. ds = yesterday, and with max_active_runs=1 that run is the one that gets to ingest.
  local bag_day
  bag_day="$(date -u -d yesterday +%Y-%m-%d 2>/dev/null || date -u -v-1d +%Y-%m-%d)"  # GNU || BSD
  log "simulating ${SIM_BATCH_ROBOTS:-2} robots x ${SIM_BATCH_SECONDS:-120} s of bags for $bag_day and uploading them to s3://rtdaas-raw/bags"
  rm -rf "$COMPOSE_DIR/data/bags-batch"  # this run's batch only; earlier ones would be re-synced
  "${COMPOSE[@]}" "${PROFILES[@]}" run --rm --no-deps sim run --robots "${SIM_BATCH_ROBOTS:-2}" \
    --duration "${SIM_BATCH_SECONDS:-120}" --rate 50 --no-realtime \
    --start-time "${bag_day}T12:00:00Z" --mcap-out /data/bags-batch
  "${COMPOSE[@]}" "${PROFILES[@]}" run --rm --no-deps bag-lander --root /data/bags-batch

  log "starting the simulator, the streaming job and the bag lander (demo profile)"
  "${COMPOSE[@]}" "${PROFILES[@]}" up -d sim streaming bag-lander

  # Unpausing telemetry_bag_ingest is what starts its run (ds = yesterday, see above).
  # dq_daily stays paused: unpaused it would fire at once, before anything has been ingested;
  # trigger it from the UI once the ingest runs are green.
  log "unpausing DAGs and triggering the first runs"
  for d in telemetry_bag_ingest jigsaws_ingest contracts_sync; do
    curl -fsS -u airflow:airflow -H 'Content-Type: application/json' -X PATCH \
      "$AIRFLOW_URL/api/v1/dags/$d" -d '{"is_paused": false}' >/dev/null || true
  done
  trigger_dag contracts_sync '{}'
  trigger_dag jigsaws_ingest '{}'
  status
}

down() {
  "${COMPOSE[@]}" "${PROFILES[@]}" --profile observability down
}

status() {
  "${COMPOSE[@]}" "${PROFILES[@]}" ps
  cat <<EOF

  API + UI        $API_URL        (Swagger: $API_URL/docs)
  Airflow         $AIRFLOW_URL    (airflow / airflow)
  Spark master    http://localhost:8082
  Schema registry http://localhost:8081/apis/ccompat/v7/subjects
  MinIO console   http://localhost:9001   (minioadmin / minioadmin)
  Elasticsearch   http://localhost:9200
  Kafka           localhost:29092         (kcat -b localhost:29092 -L)
EOF
}

case "${1:-up}" in
  up) up ;;
  down) down ;;
  status) status ;;
  *) echo "usage: $0 up|down|status" >&2; exit 1 ;;
esac
