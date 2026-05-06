#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=include/mb_list.sh
. "${SCRIPT_DIR}/include/mb_list.sh"

METABASE_URL="${METABASE_URL:-http://localhost:3000}"
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@egisz.local}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-egisz}"
APP_DB_NAME="${APP_DB_NAME:-dwh_egisz}"
APP_DB_HOST="${APP_DB_HOST:-host.docker.internal}"
APP_DB_PORT="${APP_DB_PORT:-5432}"
APP_DB_USER="${APP_DB_USER:-egisz}"
APP_DB_PASSWORD="${APP_DB_PASSWORD:-egisz}"
APP_DB_DISPLAY_NAME="${APP_DB_DISPLAY_NAME:-DWH: Интеграция с ЕГИСЗ}"
DASHBOARDS_DIR="${METABASE_DASHBOARDS_DIR:-/app/metabase_dashboards}"
ROOT_COLLECTION_NAME="${METABASE_COLLECTION_NAME:-Интеграция с ЕГИСЗ}"

log() {
  echo "[dashboards] $*"
}

api() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  if [ -n "${body}" ]; then
    curl -sS --fail -X "${method}" "${METABASE_URL}${path}" \
      -H "Content-Type: application/json" \
      -H "X-Metabase-Session: ${SESSION_TOKEN}" \
      -d "${body}"
  else
    curl -sS --fail -X "${method}" "${METABASE_URL}${path}" \
      -H "Content-Type: application/json" \
      -H "X-Metabase-Session: ${SESSION_TOKEN}"
  fi
}

SESSION_TOKEN="$(curl -sS -X POST "${METABASE_URL}/api/session" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${ADMIN_EMAIL}\",\"password\":\"${ADMIN_PASSWORD}\"}" | jq -r '.id // empty')"
[ -n "${SESSION_TOKEN}" ] && [ "${SESSION_TOKEN}" != "null" ] || { echo "Metabase auth failed" >&2; exit 1; }

ensure_database() {
  local dbs db_id payload
  dbs="$(api GET "/api/database" | mb_list)"
  db_id="$(echo "${dbs}" | jq -r --arg name "${APP_DB_DISPLAY_NAME}" '.[] | select(.name == $name) | .id' | head -n1)"
  if [ -n "${db_id}" ] && [ "${db_id}" != "null" ]; then
    echo "${db_id}"
    return
  fi
  payload="$(jq -n \
    --arg name "${APP_DB_DISPLAY_NAME}" \
    --arg host "${APP_DB_HOST}" \
    --arg port "${APP_DB_PORT}" \
    --arg dbname "${APP_DB_NAME}" \
    --arg user "${APP_DB_USER}" \
    --arg password "${APP_DB_PASSWORD}" \
    '{
      engine: "postgres",
      name: $name,
      details: {
        host: $host,
        port: ($port | tonumber),
        dbname: $dbname,
        user: $user,
        password: $password,
        ssl: false,
        "tunnel-enabled": false,
        "advanced-options": false
      },
      schedules: {}
    }')"
  api POST "/api/database" "${payload}" | jq -r '.id'
}

delete_collection_contents() {
  local collection_id="$1"
  local items ids id
  items="$(api GET "/api/collection/${collection_id}/items")"
  ids="$(echo "${items}" | mb_list | jq -r '.[] | select(.model == "dashboard") | .id')"
  for id in ${ids}; do api DELETE "/api/dashboard/${id}" >/dev/null || true; done
  ids="$(echo "${items}" | mb_list | jq -r '.[] | select(.model == "card") | .id')"
  for id in ${ids}; do api DELETE "/api/card/${id}" >/dev/null || true; done
}

ensure_collection() {
  local collections id payload
  collections="$(api GET "/api/collection" | mb_list)"
  id="$(echo "${collections}" | jq -r --arg name "${ROOT_COLLECTION_NAME}" '.[] | select(.name == $name) | .id' | head -n1)"
  if [ -n "${id}" ] && [ "${id}" != "null" ]; then
    delete_collection_contents "${id}"
    echo "${id}"
    return
  fi
  payload="$(jq -n --arg name "${ROOT_COLLECTION_NAME}" '{name: $name, description: "Отчёты по работе сервиса интеграции с ЕГИСЗ и поддержке клиник", color: "#2D7FF9"}')"
  api POST "/api/collection" "${payload}" | jq -r '.id'
}

create_card() {
  local card_json="$1"
  local collection_id="$2"
  local payload
  payload="$(jq -n \
    --argjson card "${card_json}" \
    --arg collectionId "${collection_id}" \
    --arg databaseId "${APP_DB_ID}" \
    '{
      name: $card.name,
      description: ($card.description // ""),
      collection_id: ($collectionId | tonumber),
      dataset_query: {
        type: "native",
        native: {query: $card.query, "template-tags": {}},
        database: ($databaseId | tonumber)
      },
      display: ($card.display // "table"),
      visualization_settings: ($card.visualization_settings // {})
    }')"
  api POST "/api/card" "${payload}" | jq -r '.id'
}

create_dashboard() {
  local file="$1"
  local collection_id="$2"
  local dashboard name description payload dashboard_id cards count i card card_id dashcard
  dashboard="$(cat "${file}")"
  name="$(echo "${dashboard}" | jq -r '.name')"
  description="$(echo "${dashboard}" | jq -r '.description // ""')"
  payload="$(jq -n \
    --arg name "${name}" \
    --arg description "${description}" \
    --arg collectionId "${collection_id}" \
    '{name: $name, description: $description, collection_id: ($collectionId | tonumber), width: "full", parameters: [], auto_apply_filters: true}')"
  dashboard_id="$(api POST "/api/dashboard" "${payload}" | jq -r '.id')"
  cards='[]'
  count="$(echo "${dashboard}" | jq '.cards | length')"
  for i in $(seq 0 $((count - 1))); do
    card="$(echo "${dashboard}" | jq -c ".cards[${i}]")"
    card_id="$(create_card "${card}" "${collection_id}")"
    dashcard="$(echo "${card}" | jq -c \
      --argjson id "$((-(i + 1)))" \
      --argjson cardId "${card_id}" \
      '{
        id: $id,
        card_id: $cardId,
        dashboard_tab_id: null,
        size_x: (.size_x // 8),
        size_y: (.size_y // 4),
        row: (.row // 0),
        col: (.col // 0),
        parameter_mappings: [],
        series: [],
        visualization_settings: {}
      }')"
    cards="$(jq -n --argjson arr "${cards}" --argjson dc "${dashcard}" '$arr + [$dc]')"
  done
  if [ "${count}" -gt 0 ]; then
    api PUT "/api/dashboard/${dashboard_id}/cards" "$(jq -n --argjson cards "${cards}" '{ordered_tabs: [], cards: $cards}')" >/dev/null
  fi
  log "created dashboard: ${name}"
}

APP_DB_ID="$(ensure_database)"
api POST "/api/database/${APP_DB_ID}/sync_schema" "{}" >/dev/null || true
COLLECTION_ID="$(ensure_collection)"

for file in "${DASHBOARDS_DIR}"/*.json; do
  [ -f "${file}" ] || continue
  create_dashboard "${file}" "${COLLECTION_ID}"
done

log "database=${APP_DB_ID} collection=${COLLECTION_ID}"
