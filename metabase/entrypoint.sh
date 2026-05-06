#!/bin/bash
set -euo pipefail

echo "[metabase] starting Metabase..."
/app/run_metabase.sh &
METABASE_PID=$!
trap 'kill "${METABASE_PID}" 2>/dev/null || true' INT TERM

( /app/provision.sh || echo "[metabase] provisioning failed; see logs above" ) &

wait "${METABASE_PID}"
