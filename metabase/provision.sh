#!/bin/bash
set -euo pipefail

MB_URL="${MB_URL:-http://localhost:3000}"
ADMIN_EMAIL="${METABASE_ADMIN_EMAIL:-admin@egisz.local}"
ADMIN_PASSWORD="${METABASE_ADMIN_PASSWORD:-egisz}"
SITE_NAME="${METABASE_SITE_NAME:-Интеграция с ЕГИСЗ}"
APP_DB_HOST="${APP_DB_HOST:-host.docker.internal}"
APP_DB_PORT="${APP_DB_PORT:-5432}"
APP_DB_NAME="${APP_DB_NAME:-dwh_egisz}"
APP_DB_USER="${APP_DB_USER:-egisz}"
APP_DB_PASSWORD="${APP_DB_PASSWORD:-egisz}"
APP_DB_DISPLAY_NAME="${APP_DB_DISPLAY_NAME:-DWH: Интеграция с ЕГИСЗ}"
DASHBOARDS_DIR="${METABASE_DASHBOARDS_DIR:-/app/metabase_dashboards}"
MANIFEST_STAMP_FILE="${METABASE_DASHBOARDS_MANIFEST_STAMP:-/shared/metabase-dashboards.sha256}"

log() {
  echo "[provision] $*"
}

bundle_sha() {
  find "${DASHBOARDS_DIR}" -maxdepth 1 -name '*.json' -type f 2>/dev/null \
    | LC_ALL=C sort \
    | while IFS= read -r file; do sha256sum "${file}"; done \
    | sha256sum | awk '{print $1}'
}

log "waiting for Metabase API at ${MB_URL}"
until curl --silent --fail "${MB_URL}/api/health" >/dev/null; do
  sleep 5
done

properties="$(curl -sS "${MB_URL}/api/session/properties")"
if [ "$(echo "${properties}" | jq -r '."has-user-setup"')" != "true" ]; then
  setup_token="$(echo "${properties}" | jq -r '."setup-token"')"
  payload="$(jq -n \
    --arg token "${setup_token}" \
    --arg email "${ADMIN_EMAIL}" \
    --arg password "${ADMIN_PASSWORD}" \
    --arg siteName "${SITE_NAME}" \
    --arg dbName "${APP_DB_DISPLAY_NAME}" \
    --arg dbHost "${APP_DB_HOST}" \
    --arg dbPort "${APP_DB_PORT}" \
    --arg dbRealName "${APP_DB_NAME}" \
    --arg dbUser "${APP_DB_USER}" \
    --arg dbPassword "${APP_DB_PASSWORD}" \
    '{
      token: $token,
      user: {first_name: "EGISZ", last_name: "Admin", email: $email, password: $password},
      database: {
        engine: "postgres",
        name: $dbName,
        details: {
          host: $dbHost,
          port: ($dbPort | tonumber),
          dbname: $dbRealName,
          user: $dbUser,
          password: $dbPassword,
          ssl: false,
          "tunnel-enabled": false,
          "advanced-options": false
        }
      },
      prefs: {site_name: $siteName, site_locale: "ru"}
    }')"
  log "bootstrapping admin user and DWH database registration"
  curl -sS --fail -X POST "${MB_URL}/api/setup" -H "Content-Type: application/json" -d "${payload}" >/dev/null
fi

session_token="$(curl -sS -X POST "${MB_URL}/api/session" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"${ADMIN_EMAIL}\",\"password\":\"${ADMIN_PASSWORD}\"}" | jq -r '.id // empty')"

if [ -z "${session_token}" ] || [ "${session_token}" = "null" ]; then
  echo "[provision] failed to authenticate in Metabase" >&2
  exit 1
fi

expected="$(find "${DASHBOARDS_DIR}" -maxdepth 1 -name '*.json' -type f 2>/dev/null | wc -l | tr -d ' ')"
current_sha="$(bundle_sha)"
force="${METABASE_FORCE_PROVISION:-auto}"
if [ "${force}" = "false" ] || [ "${force}" = "0" ]; then
  log "METABASE_FORCE_PROVISION=${force}; skipping dashboard import"
  exit 0
fi
if [ "${force}" = "auto" ] && [ -f "${MANIFEST_STAMP_FILE}" ] && [ "$(cat "${MANIFEST_STAMP_FILE}" | tr -d '[:space:]')" = "${current_sha}" ]; then
  log "dashboard bundle is unchanged; skipping import"
  exit 0
fi

log "waiting for DWH views in ${APP_DB_NAME}"
for attempt in $(seq 1 120); do
  count="$(PGPASSWORD="${APP_DB_PASSWORD}" psql -h "${APP_DB_HOST}" -p "${APP_DB_PORT}" -U "${APP_DB_USER}" -d "${APP_DB_NAME}" -Atc "SELECT COUNT(*) FROM information_schema.views WHERE table_schema='public' AND table_name IN ('v_proxy_exchange_detail','v_proxy_exchange_daily','v_proxy_exchange_service_summary','v_proxy_exchange_error_summary','v_proxy_exchange_latest');" 2>/dev/null | tr -d '[:space:]' || true)"
  if [ "${count:-0}" -ge 5 ] 2>/dev/null; then
    break
  fi
  [ $((attempt % 12)) -eq 0 ] && log "still waiting for DWH views (count=${count:-0})"
  sleep 5
done

if [ "${count:-0}" -lt 5 ] 2>/dev/null; then
  echo "[provision] DWH views are not ready; run etl-sync and restart Metabase or set METABASE_FORCE_PROVISION=true later" >&2
  exit 0
fi

METABASE_URL="${MB_URL}" \
ADMIN_EMAIL="${ADMIN_EMAIL}" \
ADMIN_PASSWORD="${ADMIN_PASSWORD}" \
APP_DB_NAME="${APP_DB_NAME}" \
APP_DB_HOST="${APP_DB_HOST}" \
APP_DB_PORT="${APP_DB_PORT}" \
APP_DB_USER="${APP_DB_USER}" \
APP_DB_PASSWORD="${APP_DB_PASSWORD}" \
APP_DB_DISPLAY_NAME="${APP_DB_DISPLAY_NAME}" \
METABASE_DASHBOARDS_DIR="${DASHBOARDS_DIR}" \
/app/setup-dashboards.sh

mkdir -p "$(dirname "${MANIFEST_STAMP_FILE}")"
printf '%s\n' "${current_sha}" > "${MANIFEST_STAMP_FILE}"
log "recorded dashboard manifest ${current_sha}"
