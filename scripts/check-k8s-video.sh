#!/usr/bin/env bash
# Asserts the deployed API serves a trial video that exists only in the object store, as it does on
# k8s, where nothing copies videos to the API's volume. Needs the API on the sqlite backend, whose
# mirror this adds a trial to. Used by the integration workflow.
set -euo pipefail

kubectl -n rtdaas exec -i deploy/rtdaas-api -- python - <<'PY'
import sqlite3

from rtdaas.config import get_settings
from rtdaas.serving.app import make_s3_client

s = get_settings()
key = "jigsaws/Suturing/video/Suturing_B001_capture1.mp4"
make_s3_client(s).put_object(Bucket=s.s3_bucket_raw, Key=key, Body=bytes(range(256)) * 4096)
db = sqlite3.connect(s.serving_sqlite_path)
db.execute("CREATE TABLE procedures__trials (trial_id TEXT, video_path TEXT)")
db.execute(
    "INSERT INTO procedures__trials VALUES (?, ?)",
    ("Suturing_B001", f"s3a://{s.s3_bucket_raw}/{key}"),
)
db.commit()
PY

tmp=$(mktemp -d)
kubectl -n rtdaas port-forward svc/rtdaas-api 18081:8080 >"$tmp/port-forward.log" 2>&1 &
forward=$!
trap 'kill "$forward" 2>/dev/null || true; rm -rf "$tmp"' EXIT
for _ in $(seq 1 30); do
  curl -fsS -o /dev/null http://127.0.0.1:18081/api/live 2>/dev/null && break
  sleep 1
done

url=http://127.0.0.1:18081/api/trials/Suturing_B001/video
python3 -c 'import sys; sys.stdout.buffer.write(bytes(range(256)) * 4096)' >"$tmp/expected"

code=$(curl -sS -o "$tmp/full" -w '%{http_code}' "$url")
echo "whole file: HTTP $code"
test "$code" = 200
cmp "$tmp/full" "$tmp/expected"

code=$(curl -sS -o "$tmp/part" -w '%{http_code}' -H 'Range: bytes=1000-1999' "$url")
echo "range: HTTP $code"
test "$code" = 206
dd if="$tmp/expected" of="$tmp/expected-part" bs=1000 skip=1 count=1 status=none
cmp "$tmp/part" "$tmp/expected-part"
echo "video served from the object store"
