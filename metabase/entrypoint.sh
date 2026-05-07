#!/bin/bash
set -euo pipefail

echo "[metabase] starting Metabase..."
/app/run_metabase.sh &
METABASE_PID=$!
trap 'kill "${METABASE_PID}" 2>/dev/null || true' INT TERM

# Run provision in background; use localhost internally since we're inside the container
( MB_URL="http://localhost:3000" /app/provision.sh || echo "[metabase] provisioning failed; see logs above" ) &

wait "${METABASE_PID}"
