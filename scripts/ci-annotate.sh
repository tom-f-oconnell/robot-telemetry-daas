#!/usr/bin/env bash
# ci-annotate.sh <label> -- <command...>
#
# Runs the command, tees its output to a temp log and, on failure inside GitHub Actions, emits the
# tail of that log as `::error::` workflow-command annotations. Annotations are readable through
# the Checks API (GET /repos/{o}/{r}/check-runs/{job_id}/annotations), which is useful when the
# full log download (Azure blob storage) is not reachable from where you are debugging.
set -uo pipefail
label="${1:?label}"; shift
[[ "${1:-}" == "--" ]] && shift
log="$(mktemp)"
"$@" 2>&1 | tee "$log"
rc=${PIPESTATUS[0]}
if [[ $rc -ne 0 && -n "${GITHUB_ACTIONS:-}" ]]; then
  LABEL="$label" python3 - "$log" <<'PY'
import os, sys
data = open(sys.argv[1], errors="replace").read()[-14000:]
enc = lambda s: s.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
chunks = [data[i:i + 3500] for i in range(0, len(data), 3500)] or ["(no output)"]
label = os.environ["LABEL"].replace(",", " ")
for i, c in enumerate(chunks, 1):
    print(f"::error title={label} [{i}/{len(chunks)}]::{enc(c)}")
PY
fi
exit "$rc"
