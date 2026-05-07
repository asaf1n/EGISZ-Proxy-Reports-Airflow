#!/bin/bash
set -euo pipefail

MB_URL="${MB_URL:-http://localhost:3000}"

log() {
  echo "[provision] $*"
}

log "waiting for Metabase API at ${MB_URL}"
until curl --silent --fail "${MB_URL}/api/health" >/dev/null; do
  sleep 5
done

log "Metabase is ready, calling setup-dashboards.sh"
exec /app/setup-dashboards.sh
