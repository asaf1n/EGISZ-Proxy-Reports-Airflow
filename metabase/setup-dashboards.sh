#!/bin/bash
set -euo pipefail

_script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${_script_dir}/include/mb_list.sh"

METABASE_URL="${METABASE_URL:-http://localhost:3000}"
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@egisz.local}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-egisz}"
DB_NAME="${APP_DB_NAME:-dwh_egisz}"
DB_USER="${APP_DB_USER:-egisz}"
DB_PASSWORD="${APP_DB_PASSWORD:-egisz}"
DB_DISPLAY_NAME="${APP_DB_DISPLAY_NAME:-DWH: Интеграция с ЕГИСЗ}"
PGHOST="${APP_DB_HOST:-host.docker.internal}"
PGPORT="${APP_DB_PORT:-5432}"
DASHBOARDS_DIR="${METABASE_DASHBOARDS_DIR:-/app/metabase_dashboards}"
MB_AUTO_APPLY_FILTERS="${METABASE_AUTO_APPLY_FILTERS:-true}"

log_info() {
  echo "[dashboards] $1" >&2
}

api_request() {
  local method="$1"
  local path="$2"
  local payload="${3:-}"
  local response

  if [ -n "${payload}" ]; then
    response="$(curl -sS -w $'\n%{http_code}' -X "${method}" "${METABASE_URL}${path}" \
      -H "Content-Type: application/json" \
      -H "X-Metabase-Session: ${SESSION_TOKEN}" \
      -d "${payload}")"
  else
    response="$(curl -sS -w $'\n%{http_code}' -X "${method}" "${METABASE_URL}${path}" \
      -H "X-Metabase-Session: ${SESSION_TOKEN}")"
  fi

  HTTP_CODE="$(echo "${response}" | tail -n1)"
  RESPONSE_BODY="$(echo "${response}" | sed '$d')"

  if [[ ! "${HTTP_CODE}" =~ ^2 ]]; then
    echo "Metabase API ${method} ${path} failed with HTTP ${HTTP_CODE}" >&2
    echo "${RESPONSE_BODY}" >&2
    return 1
  fi

  printf '%s' "${RESPONSE_BODY}"
}

authenticate() {
  SESSION_TOKEN="$(curl -sS -X POST "${METABASE_URL}/api/session" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"${ADMIN_EMAIL}\",\"password\":\"${ADMIN_PASSWORD}\"}" | jq -r '.id')"

  if [ -z "${SESSION_TOKEN}" ] || [ "${SESSION_TOKEN}" = "null" ]; then
    echo "Failed to authenticate in Metabase" >&2
    exit 1
  fi
}

ensure_app_database() {
  local databases_json db_id payload

  databases_json="$(api_request GET "/api/database")"
  db_id="$(echo "${databases_json}" | jq -r --arg dbName "${DB_NAME}" --arg display "${DB_DISPLAY_NAME}" '
    [
      .data[]
      | select(
          (.name == $display)
          or (.name == $dbName)
          or (.details.dbname? == $dbName)
        )
    ]
    | sort_by(.id)
    | last
    | .id // empty
  ')"

  if [ -n "${db_id}" ]; then
    printf '%s' "${db_id}"
    return 0
  fi

  payload="$(jq -n \
    --arg name "${DB_DISPLAY_NAME}" \
    --arg dbname "${DB_NAME}" \
    --arg user "${DB_USER}" \
    --arg password "${DB_PASSWORD}" \
    --arg pgHost "${PGHOST}" \
    --arg pgPort "${PGPORT}" \
    '{
      engine: "postgres",
      name: $name,
      details: {
        host: $pgHost,
        port: ($pgPort | tonumber),
        dbname: $dbname,
        user: $user,
        password: $password,
        ssl: false,
        "tunnel-enabled": false,
        "advanced-options": false
      },
      is_full_sync: true,
      is_on_demand: false,
      auto_run_queries: true
    }')"

  db_id="$(api_request POST "/api/database" "${payload}" | jq -r '.id // empty')"

  if [ -z "${db_id}" ] || [ "${db_id}" = "null" ]; then
    echo "Failed to register application database" >&2
    exit 1
  fi

  api_request POST "/api/database/${db_id}/sync_schema" "{}" >/dev/null || true
  printf '%s' "${db_id}"
}

create_collection() {
  local name="$1"
  local description="$2"
  local color="$3"
  local parent_id="${4:-}"
  local payload

  if [ -n "${parent_id}" ]; then
    payload="$(jq -n \
      --arg name "${name}" \
      --arg description "${description}" \
      --arg color "${color}" \
      --arg parentId "${parent_id}" \
      '{name: $name, description: $description, color: $color, parent_id: ($parentId | tonumber)}')"
  else
    payload="$(jq -n \
      --arg name "${name}" \
      --arg description "${description}" \
      --arg color "${color}" \
      '{name: $name, description: $description, color: $color}')"
  fi

  api_request POST "/api/collection" "${payload}" | jq -r '.id'
}

log_info "Waiting for Metabase at ${METABASE_URL}..."
until curl --silent --fail "${METABASE_URL}/api/health" >/dev/null; do
  sleep 3
done

log_info "METABASE_AUTO_APPLY_FILTERS=${MB_AUTO_APPLY_FILTERS}"

authenticate
APP_DB_ID="$(ensure_app_database)"

log_info "Metabase: sync_schema for application database id=${APP_DB_ID}…"
api_request POST "/api/database/${APP_DB_ID}/sync_schema" "{}" >/dev/null || true

# Create or find root collection "Интеграция с ЕГИСЗ"
log_info "Creating or finding root collection 'Интеграция с ЕГИСЗ'"
ROOT_COLLECTION_ID="$(api_request GET "/api/collection" | jq -r '.[] | select(.name == "Интеграция с ЕГИСЗ") | .id' | head -n1)"

if [ -z "${ROOT_COLLECTION_ID}" ] || [ "${ROOT_COLLECTION_ID}" = "null" ]; then
  ROOT_COLLECTION_ID="$(create_collection "Интеграция с ЕГИСЗ" "Дашборды интеграции с ЕГИСЗ" "#509EE3")"
  log_info "Created new root collection: Интеграция с ЕГИСЗ (id=${ROOT_COLLECTION_ID})"
else
  log_info "Using existing root collection: Интеграция с ЕГИСЗ (id=${ROOT_COLLECTION_ID})"
fi

# Count dashboards to import
_count=0
for dashboard_file in "${DASHBOARDS_DIR}"/*.json; do
  if [ -f "$dashboard_file" ]; then
    _count=$((${_count} + 1))
  fi
done

log_info "Found ${_count} dashboard JSON files"
log_info "Database: ${APP_DB_ID}"
log_info "Target collection: ${ROOT_COLLECTION_ID}"
log_info "Setup complete - dashboards ready for import"
