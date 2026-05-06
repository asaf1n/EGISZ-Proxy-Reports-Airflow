#!/bin/bash
set -euo pipefail

MB_URL="${MB_URL:-http://localhost:3000}"
TIMEOUT_SECONDS="${SMOKE_TIMEOUT_SECONDS:-180}"
SLEEP_SECONDS="${SMOKE_SLEEP_SECONDS:-3}"

echo "[smoke] checking Metabase UI health at ${MB_URL}"

elapsed=0
while [ "${elapsed}" -lt "${TIMEOUT_SECONDS}" ]; do
  if curl --silent --fail "${MB_URL}/api/health" >/dev/null; then
    echo "[smoke] Metabase health check passed"
    exit 0
  fi

  sleep "${SLEEP_SECONDS}"
  elapsed=$((elapsed + SLEEP_SECONDS))
done

echo "[smoke] Metabase health check failed after ${TIMEOUT_SECONDS}s" >&2
exit 1
