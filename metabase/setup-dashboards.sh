#!/bin/bash
set -euo pipefail

METABASE_URL="${METABASE_URL:-http://localhost:3000}"
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@egisz.local}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-egisz}"
DB_NAME="${APP_DB_NAME:-dwh_egisz}"
DASHBOARDS_DIR="${METABASE_DASHBOARDS_DIR:-/app/metabase_dashboards}"

log_info() { echo "[dashboards] $1" >&2; }

api_request() {
  local method="$1" path="$2" payload="${3:-}"
  local response
  if [ -z "${payload}" ]; then
    response=$(curl -s -w "\n%{http_code}" -X "$method" "${METABASE_URL}${path}" -H "X-Metabase-Session: $SESSION_TOKEN")
  else
    response=$(curl -s -w "\n%{http_code}" -X "$method" "${METABASE_URL}${path}" -H "Content-Type: application/json" -H "X-Metabase-Session: $SESSION_TOKEN" -d "$payload")
  fi
  code=$(echo "$response" | tail -n1)
  body=$(echo "$response" | sed '$d')
  [[ "$code" =~ ^2 ]] || { log_info "API Error $code on $method $path"; return 1; }
  echo "$body"
}

log_info "Waiting for Metabase..."
until curl -s "${METABASE_URL}/api/health" >/dev/null; do sleep 5; done

SESSION_TOKEN=$(curl -s -X POST "${METABASE_URL}/api/session" -H "Content-Type: application/json" -d "{\"username\":\"$ADMIN_EMAIL\",\"password\":\"$ADMIN_PASSWORD\"}" | jq -r .id)

# Получаем ID базы DWH
APP_DB_ID=$(api_request GET "/api/database" | jq -r --arg db "$DB_NAME" '.data[] | select(.details.dbname == $db) | .id' | head -n1)

# Создаем/находим коллекцию
COL_ID=$(api_request GET "/api/collection" | jq -r '.[] | select(.name == "Интеграция с ЕГИСЗ") | .id' | head -n1)
[ "$COL_ID" == "null" ] || [ -z "$COL_ID" ] && COL_ID=$(api_request POST "/api/collection" "{\"name\":\"Интеграция с ЕГИСЗ\",\"color\":\"#509EE3\"}" | jq -r .id)

log_info "Importing dashboards from $DASHBOARDS_DIR..."
for f in "$DASHBOARDS_DIR"/*.json; do
  [ -f "$f" ] || continue
  log_info "Uploading $(basename "$f")"
  # Удаляем старый ID и подставляем актуальный ID базы и коллекции
  clean_json=$(jq --arg db "$APP_DB_ID" --arg col "$COL_ID" 'del(.id) | .collection_id=($col|tonumber) | .dataset_query.database=($db|tonumber)' "$f")
  api_request POST "/api/dashboard" "$clean_json" > /dev/null
done

log_info "Setup complete."
exit 0
# metabase/setup-dashboards.sh     