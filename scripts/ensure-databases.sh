#!/bin/bash
set -euo pipefail

DWH_DB_NAME="${DWH_DB_NAME:-dwh_egisz}"
METABASE_APP_DB_NAME="${METABASE_APP_DB_NAME:-metabase_app}"

wait_for_pg() {
  until pg_isready -h "${PGHOST}" -p "${PGPORT:-5432}" -U "${PGUSER}" -d postgres >/dev/null 2>&1; do
    echo "[db-init] waiting for Postgres at ${PGHOST}:${PGPORT:-5432}..."
    sleep 2
  done
}

ensure_db() {
  local db="$1"
  local exists
  exists="$(psql -d postgres -Atqc "SELECT 1 FROM pg_database WHERE datname='${db}'" | tr -d '[:space:]' || true)"
  if [ "${exists}" = "1" ]; then
    echo "[db-init] database ${db} already exists"
  else
    psql -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"${db}\";"
    echo "[db-init] created database ${db}"
  fi
}

wait_for_pg
ensure_db "${DWH_DB_NAME}"
ensure_db "${METABASE_APP_DB_NAME}"
