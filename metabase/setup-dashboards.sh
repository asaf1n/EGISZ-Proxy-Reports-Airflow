#!/bin/bash
set -euo pipefail

METABASE_URL="${METABASE_URL:-${MB_URL:-http://localhost:3000}}"
ADMIN_EMAIL="${ADMIN_EMAIL:-${METABASE_ADMIN_EMAIL:-admin@egisz.local}}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-${METABASE_ADMIN_PASSWORD:-egisz}}"
DASHBOARDS_DIR="${METABASE_DASHBOARDS_DIR:-/app/metabase_dashboards}"
COLLECTION_NAME="${METABASE_COLLECTION_NAME:-Интеграция с ЕГИСЗ}"
METABASE_SITE_NAME="${METABASE_SITE_NAME:-Интеграция с ЕГИСЗ}"

APP_DB_HOST="${APP_DB_HOST:-host.docker.internal}"
APP_DB_PORT="${APP_DB_PORT:-5432}"
APP_DB_NAME="${APP_DB_NAME:-dwh_egisz}"
APP_DB_USER="${APP_DB_USER:-postgres}"
APP_DB_PASSWORD="${APP_DB_PASSWORD:-postgres}"
APP_DB_DISPLAY_NAME="${APP_DB_DISPLAY_NAME:-DWH ЕГИСЗ}"
DB_METADATA_FILE=""

log_info() {
  echo "[dashboards] $*" >&2
}

fail() {
  echo "[dashboards] ERROR: $*" >&2
  exit 1
}

api_request() {
  local method="$1" path="$2" payload="${3:-}"
  local response code body
  if [ -z "${payload}" ]; then
    response=$(curl -sS -w "\n%{http_code}" -X "${method}" "${METABASE_URL}${path}" \
      -H "X-Metabase-Session: ${SESSION_TOKEN}")
  else
    response=$(curl -sS -w "\n%{http_code}" -X "${method}" "${METABASE_URL}${path}" \
      -H "Content-Type: application/json" \
      -H "X-Metabase-Session: ${SESSION_TOKEN}" \
      -d "${payload}")
  fi
  code=$(echo "${response}" | tail -n1)
  body=$(echo "${response}" | sed '$d')
  [[ "${code}" =~ ^2 ]] || fail "Metabase API ${method} ${path} returned HTTP ${code}: ${body}"
  echo "${body}"
}

login() {
  local body setup_token payload
  body=$(curl -sS -X POST "${METABASE_URL}/api/session" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"${ADMIN_EMAIL}\",\"password\":\"${ADMIN_PASSWORD}\"}")
  SESSION_TOKEN=$(echo "${body}" | jq -r '.id // empty')
  if [ -n "${SESSION_TOKEN}" ]; then
    return
  fi

  setup_token=$(curl -sS "${METABASE_URL}/api/session/properties" | jq -r '."setup-token" // empty')
  [ -n "${setup_token}" ] || fail "cannot login to Metabase as ${ADMIN_EMAIL}: ${body}"

  log_info "Metabase is not initialized; creating admin user and registering ${APP_DB_NAME}"
  payload=$(
    jq -nc \
      --arg token "${setup_token}" \
      --arg email "${ADMIN_EMAIL}" \
      --arg password "${ADMIN_PASSWORD}" \
      --arg site "${METABASE_SITE_NAME}" \
      --arg db_name "${APP_DB_DISPLAY_NAME}" \
      --arg host "${APP_DB_HOST}" \
      --arg port "${APP_DB_PORT}" \
      --arg dbname "${APP_DB_NAME}" \
      --arg user "${APP_DB_USER}" \
      --arg pass "${APP_DB_PASSWORD}" \
      '{
        token: $token,
        user: {
          first_name: "EGISZ",
          last_name: "Admin",
          email: $email,
          password: $password
        },
        prefs: {
          site_name: $site,
          allow_tracking: false
        },
        database: {
          name: $db_name,
          engine: "postgres",
          details: {
            host: $host,
            port: ($port | tonumber),
            dbname: $dbname,
            user: $user,
            password: $pass,
            ssl: false
          }
        }
      }'
  )
  body=$(curl -sS -X POST "${METABASE_URL}/api/setup" -H "Content-Type: application/json" -d "${payload}")
  SESSION_TOKEN=$(echo "${body}" | jq -r '.id // empty')
  [ -n "${SESSION_TOKEN}" ] || fail "cannot initialize Metabase: ${body}"
}

resolve_or_create_app_database_id() {
  APP_DB_ID=$(
    api_request GET "/api/database" |
      jq -r --arg db "${APP_DB_NAME}" '.data[]? | select(.details.dbname == $db or .name == $db) | .id' |
      head -n1
  )
  if [ -n "${APP_DB_ID}" ]; then
    return
  fi

  local payload
  log_info "Registering DWH database ${APP_DB_NAME} in Metabase"
  payload=$(
    jq -nc \
      --arg name "${APP_DB_DISPLAY_NAME}" \
      --arg host "${APP_DB_HOST}" \
      --arg port "${APP_DB_PORT}" \
      --arg dbname "${APP_DB_NAME}" \
      --arg user "${APP_DB_USER}" \
      --arg pass "${APP_DB_PASSWORD}" \
      '{
        name: $name,
        engine: "postgres",
        details: {
          host: $host,
          port: ($port | tonumber),
          dbname: $dbname,
          user: $user,
          password: $pass,
          ssl: false
        }
      }'
  )
  APP_DB_ID=$(api_request POST "/api/database" "${payload}" | jq -r '.id')
  [ -n "${APP_DB_ID}" ] && [ "${APP_DB_ID}" != "null" ] || fail "cannot register DWH database '${APP_DB_NAME}'"
}

ensure_collection() {
  COL_ID=$(
    api_request GET "/api/collection" |
      jq -r --arg name "${COLLECTION_NAME}" '.[]? | select(.name == $name) | .id' |
      head -n1
  )
  if [ -z "${COL_ID}" ] || [ "${COL_ID}" = "null" ]; then
    local payload
    payload=$(jq -nc --arg name "${COLLECTION_NAME}" '{name: $name, color: "#509EE3"}')
    COL_ID=$(api_request POST "/api/collection" "${payload}" | jq -r '.id')
  fi
  [ -n "${COL_ID}" ] && [ "${COL_ID}" != "null" ] || fail "cannot create or resolve collection '${COLLECTION_NAME}'"
}

required_public_objects() {
  jq -r '.. | strings | scan("public\\.[A-Za-z_][A-Za-z0-9_]*")' "${DASHBOARDS_DIR}"/*.json |
    sort -u |
    sed 's/^public\.//'
}

dwh_object_exists() {
  local object="$1"
  PGPASSWORD="${APP_DB_PASSWORD}" psql \
    -h "${APP_DB_HOST}" \
    -p "${APP_DB_PORT}" \
    -U "${APP_DB_USER}" \
    -d "${APP_DB_NAME}" \
    -AtX \
    -v ON_ERROR_STOP=1 \
    -c "SELECT CASE WHEN EXISTS (
          SELECT 1 FROM information_schema.tables
          WHERE table_schema = 'public' AND table_name = '${object}'
        ) OR EXISTS (
          SELECT 1 FROM information_schema.routines
          WHERE specific_schema = 'public' AND routine_name = '${object}'
        ) THEN 'ok' ELSE 'missing' END;"
}

validate_dwh_contract() {
  log_info "Checking DWH contract in ${APP_DB_HOST}:${APP_DB_PORT}/${APP_DB_NAME}"
  local missing=()
  local object status
  while IFS= read -r object; do
    [ -n "${object}" ] || continue
    status=$(dwh_object_exists "${object}")
    if [ "${status}" != "ok" ]; then
      missing+=("public.${object}")
    fi
  done < <(required_public_objects)

  if [ "${#missing[@]}" -gt 0 ]; then
    printf '%s\n' "${missing[@]}" >&2
    fail "DWH is missing ${#missing[@]} object(s) required by dashboard SQL"
  fi
}

sync_metabase_schema() {
  log_info "Requesting Metabase schema sync for ${APP_DB_NAME} (database id ${APP_DB_ID})"
  api_request POST "/api/database/${APP_DB_ID}/sync_schema" "{}" >/dev/null
  DB_METADATA_FILE="$(mktemp)"
  wait_for_metabase_metadata
}

required_field_filters() {
  jq -r '
    .. | objects | select(has("metabase-field-filters"))
    | ."metabase-field-filters"[]?
    | [.table_ref, .field_name]
    | @tsv
  ' "${DASHBOARDS_DIR}"/*.json | sort -u
}

metadata_has_field() {
  local table_ref="$1" field_name="$2" table_name
  table_name="${table_ref#public.}"
  jq -e --arg table "${table_name}" --arg field "${field_name}" '
    [
      .tables[]?
      | select((.schema // "public") == "public" and .name == $table)
      | .fields[]?
      | select(.name == $field or .display_name == $field)
      | .id
    ][0] != null
  ' "${DB_METADATA_FILE}" >/dev/null
}

wait_for_metabase_metadata() {
  local attempt table_ref field_name missing sample
  for attempt in $(seq 1 30); do
    api_request GET "/api/database/${APP_DB_ID}/metadata" > "${DB_METADATA_FILE}"
    missing=0
    sample=""
    while IFS=$'\t' read -r table_ref field_name; do
      [ -n "${table_ref}" ] || continue
      if ! metadata_has_field "${table_ref}" "${field_name}"; then
        missing=$((missing + 1))
        [ -n "${sample}" ] || sample="${table_ref}.${field_name}"
      fi
    done < <(required_field_filters)

    if [ "${missing}" -eq 0 ]; then
      return
    fi
    log_info "Waiting for Metabase field metadata (${missing} field(s) unresolved; first: ${sample})"
    sleep 5
  done
  fail "Metabase metadata did not expose required dashboard field filters in time"
}

delete_existing_dashboard() {
  local dashboard_name="$1"
  local ids
  ids=$(
    api_request GET "/api/collection/${COL_ID}/items?models=dashboard" |
      jq -r --arg name "${dashboard_name}" '.data[]? | select(.model == "dashboard" and .name == $name) | .id'
  )
  local id
  for id in ${ids}; do
    log_info "Removing old dashboard '${dashboard_name}' (id ${id})"
    api_request DELETE "/api/dashboard/${id}" >/dev/null
  done
}

dashboard_payload() {
  local file="$1"
  jq --argjson db "${APP_DB_ID}" --argjson col "${COL_ID}" --slurpfile meta_file "${DB_METADATA_FILE}" '
    def walk(f):
      . as $in
      | if type == "object" then
          reduce keys[] as $key ({}; . + {($key): ($in[$key] | walk(f))}) | f
        elif type == "array" then
          map(walk(f)) | f
        else
          f
        end;

    def field_id($table_ref; $field_name):
      ($table_ref | sub("^public\\."; "")) as $table_name
      | [
          $meta_file[0].tables[]?
          | select((.schema // "public") == "public" and .name == $table_name)
          | .fields[]?
          | select(.name == $field_name or .display_name == $field_name)
          | .id
        ][0];

    def bind_field_filters:
      if type == "object" and has("dataset_query") and has("metabase-field-filters") then
        reduce (."metabase-field-filters" | to_entries[]) as $filter (.;
          ($filter.value.table_ref // "") as $table_ref
          | ($filter.value.field_name // "") as $field_name
          | (field_id($table_ref; $field_name)) as $field_id
          | if $field_id == null then
              error("Cannot resolve Metabase field id for " + $table_ref + "." + $field_name)
            else
              .dataset_query.native."template-tags"[$filter.key].dimension = ["field", $field_id, null]
            end
        )
        | del(."metabase-field-filters")
      else
        .
      end;

    def set_database:
      if type == "object" then
        if has("dataset_query") and (.dataset_query | type == "object") then
          .dataset_query.database = $db
        else .
        end
      else
        .
      end;

    del(.id)
    | .collection_id = $col
    | walk(set_database | bind_field_filters)
  ' "${file}"
}

verify_collection_contents() {
  local expected actual missing=()
  actual=$(api_request GET "/api/collection/${COL_ID}/items?models=dashboard" | jq -r '.data[]? | select(.model == "dashboard") | .name')
  while IFS= read -r expected; do
    [ -n "${expected}" ] || continue
    if ! grep -Fxq "${expected}" <<< "${actual}"; then
      missing+=("${expected}")
    fi
  done < <(jq -r '.name' "${DASHBOARDS_DIR}"/*.json)

  if [ "${#missing[@]}" -gt 0 ]; then
    printf '%s\n' "${missing[@]}" >&2
    fail "collection '${COLLECTION_NAME}' is missing imported dashboard(s)"
  fi
}

log_info "Waiting for Metabase at ${METABASE_URL}"
until curl -sS --fail "${METABASE_URL}/api/health" >/dev/null; do
  sleep 5
done

login
resolve_or_create_app_database_id
validate_dwh_contract
sync_metabase_schema
ensure_collection

log_info "Importing dashboards to collection '${COLLECTION_NAME}' from ${DASHBOARDS_DIR}"
for file in "${DASHBOARDS_DIR}"/*.json; do
  [ -f "${file}" ] || continue
  dashboard_name=$(jq -r '.name' "${file}")
  [ -n "${dashboard_name}" ] && [ "${dashboard_name}" != "null" ] || fail "dashboard file has no name: ${file}"
  delete_existing_dashboard "${dashboard_name}"
  payload=$(dashboard_payload "${file}")
  api_request POST "/api/dashboard" "${payload}" >/dev/null
  log_info "Imported ${dashboard_name}"
done

verify_collection_contents
log_info "Setup complete: collection '${COLLECTION_NAME}' contains $(ls "${DASHBOARDS_DIR}"/*.json | wc -l | tr -d ' ') dashboard(s)."
