#!/bin/sh
# Create the raw and lake buckets (idempotent) with the AWS CLI; works against MinIO, RustFS or S3.
set -eu

ENDPOINT="${S3_ENDPOINT:-http://minio:9000}"
RAW="${S3_BUCKET_RAW:-rtdaas-raw}"
LAKE="${S3_BUCKET_LAKE:-rtdaas-lake}"
s3() { aws --endpoint-url "$ENDPOINT" "$@"; }

i=0
until s3 s3 ls >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 30 ]; then echo "object store not reachable at $ENDPOINT" >&2; exit 1; fi
  sleep 2
done

for b in "$RAW" "$LAKE"; do
  if s3 s3api head-bucket --bucket "$b" >/dev/null 2>&1; then
    echo "bucket $b exists"
  else
    s3 s3 mb "s3://$b"
    echo "created bucket $b"
  fi
done
s3 s3api put-bucket-versioning --bucket "$RAW" --versioning-configuration Status=Enabled >/dev/null 2>&1 || true

# Layout prefixes (S3 has no directories; these are markers for humans browsing the console).
for key in "$RAW/bags/.keep" "$RAW/jigsaws/.keep" "$LAKE/warehouse/.keep" "$LAKE/checkpoints/.keep" "$LAKE/stage/snowflake/.keep"; do
  printf '' | s3 s3 cp - "s3://$key" >/dev/null 2>&1 || true
done

echo "minio-init done."
