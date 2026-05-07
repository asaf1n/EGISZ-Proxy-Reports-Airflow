#!/bin/bash
set -euo pipefail

DWH_DB_NAME="${DWH_DB_NAME:-dwh_egisz}"
METABASE_APP_DB_NAME="${METABASE_APP_DB_NAME:-metabase_app}"
DWH_APP_USER="${DWH_APP_USER:-egisz}"
DWH_APP_PASSWORD="${DWH_APP_PASSWORD:-egisz}"

wait_for_pg() {
  until pg_isready -h "${PGHOST}" -p "${PGPORT:-5432}" -U "${PGUSER}" -d postgres >/dev/null 2>&1; do
    echo "[db-init] waiting for Postgres at ${PGHOST}:${PGPORT:-5432}..."
    sleep 2
  done
}

ensure_db() {
  local db="$1"
  local owner="${2:-${PGUSER}}"
  local exists
  exists="$(psql -d postgres -Atqc "SELECT 1 FROM pg_database WHERE datname='${db}'" | tr -d '[:space:]' || true)"
  if [ "${exists}" = "1" ]; then
    echo "[db-init] database ${db} already exists"
  else
    psql -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"${db}\" OWNER \"${owner}\";"
    echo "[db-init] created database ${db}"
  fi
}

ensure_role() {
  local role="$1"
  local password="$2"
  local exists
  exists="$(psql -d postgres -Atqc "SELECT 1 FROM pg_roles WHERE rolname='${role}'" | tr -d '[:space:]' || true)"
  if [ "${exists}" = "1" ]; then
    echo "[db-init] role ${role} already exists"
  else
    psql -d postgres -v ON_ERROR_STOP=1 -c "CREATE ROLE \"${role}\" LOGIN PASSWORD '${password}';"
    echo "[db-init] created role ${role}"
  fi
}

grant_dwh_privileges() {
  psql -d postgres -v ON_ERROR_STOP=1 -c "GRANT ALL PRIVILEGES ON DATABASE \"${DWH_DB_NAME}\" TO \"${DWH_APP_USER}\";"
  psql -d "${DWH_DB_NAME}" -v ON_ERROR_STOP=1 -c "GRANT USAGE, CREATE ON SCHEMA public TO \"${DWH_APP_USER}\";"
  psql -d "${DWH_DB_NAME}" -v ON_ERROR_STOP=1 -c "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO \"${DWH_APP_USER}\";"
  psql -d "${DWH_DB_NAME}" -v ON_ERROR_STOP=1 -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO \"${DWH_APP_USER}\";"
}

wait_for_pg
ensure_role "${DWH_APP_USER}" "${DWH_APP_PASSWORD}"
ensure_db "${DWH_DB_NAME}" "${DWH_APP_USER}"
ensure_db "${METABASE_APP_DB_NAME}"
grant_dwh_privileges
