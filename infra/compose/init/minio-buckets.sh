#!/bin/sh
# Create the raw and lake buckets on MinIO. Idempotent.
set -eu

ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
ACCESS="${S3_ACCESS_KEY:-minioadmin}"
SECRET="${S3_SECRET_KEY:-minioadmin}"
RAW="${S3_BUCKET_RAW:-rtdaas-raw}"
LAKE="${S3_BUCKET_LAKE:-rtdaas-lake}"

i=0
until mc alias set rtdaas "$ENDPOINT" "$ACCESS" "$SECRET" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 30 ]; then echo "MinIO not reachable at $ENDPOINT" >&2; exit 1; fi
  sleep 2
done

for b in "$RAW" "$LAKE"; do
  if mc ls "rtdaas/$b" >/dev/null 2>&1; then
    echo "bucket $b exists"
  else
    mc mb "rtdaas/$b"
    echo "created bucket $b"
  fi
done

# Layout prefixes (S3 has no directories; these are markers for humans browsing the console).
mc cp /dev/null "rtdaas/$RAW/bags/.keep" >/dev/null 2>&1 || true
mc cp /dev/null "rtdaas/$RAW/jigsaws/.keep" >/dev/null 2>&1 || true
mc cp /dev/null "rtdaas/$LAKE/warehouse/.keep" >/dev/null 2>&1 || true
mc cp /dev/null "rtdaas/$LAKE/checkpoints/.keep" >/dev/null 2>&1 || true
mc cp /dev/null "rtdaas/$LAKE/stage/snowflake/.keep" >/dev/null 2>&1 || true

echo "minio-init done."
