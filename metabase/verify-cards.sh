#!/bin/bash
set -euo pipefail

METABASE_URL="${METABASE_URL:-${MB_URL:-http://localhost:3000}}"
ADMIN_EMAIL="${ADMIN_EMAIL:-${METABASE_USER:-${METABASE_ADMIN_EMAIL:-admin@egisz.local}}}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-${METABASE_PASSWORD:-${METABASE_ADMIN_PASSWORD:-egisz}}}"
DASHBOARDS_DIR="${METABASE_DASHBOARDS_DIR:-/app/metabase_dashboards}"
COLLECTION_NAME="${METABASE_COLLECTION_NAME:-Интеграция с ЕГИСЗ}"
APP_DB_HOST="${APP_DB_HOST:-${DWH_HOST:-host.docker.internal}}"
APP_DB_PORT="${APP_DB_PORT:-${DWH_PORT:-5432}}"
APP_DB_NAME="${APP_DB_NAME:-${DWH_NAME:-dwh_egisz}}"
APP_DB_USER="${APP_DB_USER:-${DWH_USER:-postgres}}"
APP_DB_PASSWORD="${APP_DB_PASSWORD:-${DWH_PASSWORD:-postgres}}"

log() {
  echo "[verify-cards] $*"
}

fail() {
  echo "[verify-cards] ERROR: $*" >&2
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
  local body
  body=$(curl -sS -X POST "${METABASE_URL}/api/session" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"${ADMIN_EMAIL}\",\"password\":\"${ADMIN_PASSWORD}\"}")
  SESSION_TOKEN=$(echo "${body}" | jq -r '.id // empty')
  [ -n "${SESSION_TOKEN}" ] || fail "cannot login to Metabase as ${ADMIN_EMAIL}: ${body}"
}

expected_card_count() {
  jq -s '[.[].cards[]?] | length' "${DASHBOARDS_DIR}"/*.json
}

resolve_collection_id() {
  COL_ID=$(
    api_request GET "/api/collection" |
      jq -r --arg name "${COLLECTION_NAME}" '[.[]? | select(.name == $name) | .id][0] // empty'
  )
  [ -n "${COL_ID}" ] || fail "cannot resolve collection '${COLLECTION_NAME}'"
}

dwh_query() {
  PGPASSWORD="${APP_DB_PASSWORD}" psql \
    -h "${APP_DB_HOST}" \
    -p "${APP_DB_PORT}" \
    -U "${APP_DB_USER}" \
    -d "${APP_DB_NAME}" \
    -AtX \
    -v ON_ERROR_STOP=1 \
    -c "$1"
}

# Map required template-tag → DWH query that returns a sample value to plug in.
# Cards with a required tag we don't know how to sample will fail with a clear message
# so adding a new filter forces an explicit entry here.
sample_for_required_tag() {
  case "$1" in
    doc_key_filter)
      dwh_query "SELECT \"Идентификатор документа\" FROM public.v_doc_timeline_ui WHERE \"Идентификатор документа\" IS NOT NULL LIMIT 1;"
      ;;
    org_filter)
      dwh_query "SELECT \"JID клиники\" FROM public.v_stat_orgs_ui WHERE \"JID клиники\" IS NOT NULL LIMIT 1;"
      ;;
    semd_filter)
      dwh_query "SELECT code FROM public.dim_semd_types WHERE code IS NOT NULL LIMIT 1;"
      ;;
    *)
      return 1
      ;;
  esac
}

verify_required_param_card() {
  local card_payload card_name query tag sample escaped
  card_payload="$1"
  shift
  card_name="$(echo "${card_payload}" | jq -r '.name')"
  # Newer Metabase persists cards in MBQL "stages" shape with the SQL string
  # under .dataset_query.stages[0].native; older payloads use .dataset_query.native.query.
  query="$(echo "${card_payload}" | jq -r '
    if (.dataset_query.stages // [] | length) > 0
    then .dataset_query.stages[0].native
    else .dataset_query.native.query
    end
  ')"

  local placeholder replacement
  for tag in "$@"; do
    sample="$(sample_for_required_tag "${tag}")" \
      || fail "card '${card_name}' has unsupported required template tag: ${tag}"
    [ -n "${sample}" ] \
      || fail "cannot find sample for required tag '${tag}' on card '${card_name}'"
    escaped="${sample//\'/\'\'}"
    placeholder="{{${tag}}}"
    replacement="'${escaped}'"
    query="${query//${placeholder}/${replacement}}"
    log "Resolved required tag '${tag}' for card '${card_name}' with sample '${sample}'"
  done

  log "Executing card '${card_name}' (with required params substituted) directly on ${APP_DB_NAME}"
  printf '%s\n' "${query}" | PGPASSWORD="${APP_DB_PASSWORD}" psql \
    -h "${APP_DB_HOST}" \
    -p "${APP_DB_PORT}" \
    -U "${APP_DB_USER}" \
    -d "${APP_DB_NAME}" \
    -v ON_ERROR_STOP=1 >/dev/null
}

verify_card_execution() {
  local card_id="$1" card_name="$2" card_payload required_tags query_result
  card_payload="$(api_request GET "/api/card/${card_id}")"
  # Locate template-tags: newer Metabase uses .dataset_query.stages[].template-tags,
  # older payloads put them under .dataset_query.native.template-tags.
  required_tags="$(echo "${card_payload}" | jq -r '
    [
      ( .dataset_query.stages // []
        | map(.["template-tags"] // {}) )
      , [ .dataset_query.native["template-tags"] // {} ]
    ]
    | flatten
    | map(to_entries[])
    | map(select(.value.required == true) | .key)
    | unique[]
  ' 2>/dev/null || true)"

  api_request GET "/api/card/${card_id}/query_metadata" >/dev/null

  if [ -z "${required_tags}" ]; then
    log "Executing Metabase card '${card_name}' (id ${card_id}) through API"
    query_result="$(api_request POST "/api/card/${card_id}/query/json" '{"ignore_cache":true}')"
    # /query/json returns a JSON array of rows on success; *.xlsx/csv variants
    # and error paths return an object. Accept either shape, then reject any
    # object whose status is "failed" (Metabase returns HTTP 202 with a failed
    # status object when a required param is missing or the SQL errored out).
    echo "${query_result}" | jq -e '
      (type == "array")
      or
      (
        (has("data") or has("rows") or has("database_id") or has("status"))
        and ((.status // "ok") != "failed")
        and ((.error // null) == null)
      )
    ' >/dev/null \
      || fail "unexpected query result shape or failed status for card '${card_name}': $(echo "${query_result}" | jq -c '{status, error, error_type}' 2>/dev/null || echo "${query_result}" | head -c 200)"
    return
  fi

  # Cards with required template-tags can't be executed via Metabase API without
  # explicit values, so substitute samples and run the rendered SQL against DWH.
  # shellcheck disable=SC2086
  verify_required_param_card "${card_payload}" ${required_tags}
}

main() {
  local expected_count actual_count verified_count=0
  login
  resolve_collection_id
  expected_count="$(expected_card_count)"
  actual_count="$(
    api_request GET "/api/collection/${COL_ID}/items?models=card&limit=1000" |
      jq '[.data[]? | select(.model == "card")] | length'
  )"

  [ "${actual_count}" -eq "${expected_count}" ] || fail "collection '${COLLECTION_NAME}' has ${actual_count} cards, expected ${expected_count}"

  while IFS=$'\t' read -r card_id card_name; do
    [ -n "${card_id}" ] || continue
    verify_card_execution "${card_id}" "${card_name}"
    verified_count=$((verified_count + 1))
  done < <(
    api_request GET "/api/collection/${COL_ID}/items?models=card&limit=1000" |
      jq -r '.data[]? | select(.model == "card") | [.id, .name] | @tsv'
  )

  log "Verified ${verified_count} Metabase card(s) in collection '${COLLECTION_NAME}'."
}

main "$@"
