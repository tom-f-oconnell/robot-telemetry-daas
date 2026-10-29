#!/usr/bin/env bash
# wait-for.sh host:port [timeout_seconds]   — or —   wait-for.sh http://url [timeout_seconds]
# Blocks until a TCP port accepts connections or an HTTP URL returns 2xx/3xx.
set -euo pipefail

target="${1:?usage: wait-for.sh host:port|http(s)://url [timeout]}"
timeout="${2:-120}"
start=$(date +%s)

ok() { echo "ready: $target"; }

while true; do
  if [[ "$target" == http://* || "$target" == https://* ]]; then
    if curl -fsS -o /dev/null --max-time 3 "$target"; then ok; exit 0; fi
  else
    host="${target%%:*}"; port="${target##*:}"
    if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then exec 3>&- 3<&-; ok; exit 0; fi
  fi
  if (( $(date +%s) - start >= timeout )); then
    echo "timeout after ${timeout}s waiting for $target" >&2
    exit 1
  fi
  sleep 2
done
