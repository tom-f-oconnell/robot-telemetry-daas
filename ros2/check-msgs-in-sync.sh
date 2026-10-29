#!/usr/bin/env bash
# Verify that ros2/rtdaas_msgs/msg/*.msg are byte-identical to the data-contract
# source of truth in contracts/msgs/rtdaas_msgs/msg/. Exit 1 on any drift.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${here}/.." && pwd)"
contracts_dir="${repo_root}/contracts/msgs/rtdaas_msgs/msg"
ros_dir="${here}/rtdaas_msgs/msg"

status=0
for src in "${contracts_dir}"/*.msg; do
  name="$(basename "${src}")"
  dst="${ros_dir}/${name}"
  if [[ ! -f "${dst}" ]]; then
    echo "MISSING: ${dst} (exists in contracts/)" >&2
    status=1
    continue
  fi
  if ! cmp -s "${src}" "${dst}"; then
    echo "DRIFT: ${name} differs between contracts/ and ros2/rtdaas_msgs/" >&2
    diff -u "${src}" "${dst}" >&2 || true
    status=1
  fi
done
for dst in "${ros_dir}"/*.msg; do
  name="$(basename "${dst}")"
  if [[ ! -f "${contracts_dir}/${name}" ]]; then
    echo "EXTRA: ${dst} has no counterpart in contracts/" >&2
    status=1
  fi
done

if [[ ${status} -eq 0 ]]; then
  echo "rtdaas_msgs in sync with contracts/msgs/rtdaas_msgs"
fi
exit ${status}
