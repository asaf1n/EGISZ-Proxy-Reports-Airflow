#!/bin/bash
set -euo pipefail

METABASE_URL="${METABASE_URL:-${MB_URL:-http://localhost:3000}}"
ADMIN_EMAIL="${ADMIN_EMAIL:-${METABASE_ADMIN_EMAIL:-admin@egisz.local}}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-${METABASE_ADMIN_PASSWORD:-egisz}}"
# Ключ API — для внешних инстансов без парольной учётки в переменных окружения;
# при заданном ключе сессионный логин и ветка первичной инициализации не используются.
METABASE_API_KEY="${METABASE_API_KEY:-}"
SESSION_TOKEN=""
DASHBOARDS_DIR="${METABASE_DASHBOARDS_DIR:-/app/metabase_dashboards}"
MODELS_DIR="${METABASE_MODELS_DIR:-/app/metabase_models}"
MODEL_REGISTRY_FILE="${METABASE_MODEL_REGISTRY_FILE:-/tmp/metabase-model-registry.json}"
METABASE_FORCE_PROVISION="${METABASE_FORCE_PROVISION:-auto}"
METABASE_SKIP_IMPORT_IF_PRESENT="${METABASE_SKIP_IMPORT_IF_PRESENT:-false}"
METABASE_AUTO_APPLY_FILTERS="${METABASE_AUTO_APPLY_FILTERS:-true}"
# Управление НАСТРОЙКАМИ ВСЕГО ИНСТАНСА (глобальный часовой пояс, локаль, формат валюты/времени,
# результат-кеш, включение public-sharing). На ОБЩЕМ Metabase они меняют поведение чужих
# сервисов, поэтому по умолчанию выключено: импорт трогает только свою коллекцию и своё
# подключение к БД (per-database report-timezone ставится всегда — он скоупится нашей БД).
# Локальный PoC-инстанс наш целиком → up.ps1 включает флаг.
METABASE_MANAGE_INSTANCE_SETTINGS="${METABASE_MANAGE_INSTANCE_SETTINGS:-false}"
DASHBOARD_MANIFEST_FILE="${DASHBOARD_MANIFEST_FILE:-/tmp/metabase-dashboards.sha256}"
COLLECTION_NAME="${METABASE_COLLECTION_NAME:-Интеграция с ЕГИСЗ}"
METABASE_SITE_NAME="${METABASE_SITE_NAME:-Интеграция с ЕГИСЗ}"
# Публичная страница без авторизации для клиентского дашборда (личный кабинет клиники).
CLIENT_DASHBOARD_NAME="${METABASE_CLIENT_DASHBOARD_NAME:-Клиентский дашборд. Мониторинг сервиса интеграции с ЕГИСЗ}"
METABASE_PUBLIC_CLIENT_DASHBOARD="${METABASE_PUBLIC_CLIENT_DASHBOARD:-true}"

APP_DB_HOST="${APP_DB_HOST:-host.docker.internal}"
APP_DB_PORT="${APP_DB_PORT:-5432}"
APP_DB_NAME="${APP_DB_NAME:-dwh_egisz}"
APP_DB_USER="${APP_DB_USER:-postgres}"
APP_DB_PASSWORD="${APP_DB_PASSWORD:-postgres}"
APP_DB_DISPLAY_NAME="${APP_DB_DISPLAY_NAME:-DWH ЕГИСЗ}"
DB_METADATA_FILE=""

# Инклюды лежат рядом со скриптом (deploy-бандл) или в /app (контейнер, где это одно и то же).
SETUP_DASHBOARDS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for _include in "${SETUP_DASHBOARDS_DIR}/include/mb_list.sh" "/app/include/mb_list.sh"; do
  if [ -f "${_include}" ]; then
    # shellcheck disable=SC1090
    source "${_include}"
    break
  fi
done

for _include in "${SETUP_DASHBOARDS_DIR}/sync-models.sh" "/app/sync-models.sh"; do
  if [ -f "${_include}" ]; then
    # shellcheck disable=SC1090
    source "${_include}"
    break
  fi
done
unset _include

log_info() {
  echo "[dashboards] $*" >&2
}

fail() {
  echo "[dashboards] ERROR: $*" >&2
  exit 1
}

# Сериализация: entrypoint запускает provision.sh в фоне, а up.ps1 параллельно
# дёргает этот же скрипт через kubectl exec. Без блокировки два параллельных
# прогона гонятся за archive_stale_collection_cards / create_or_update_card и
# второй падает на 409/конфликте имени карточки.
SETUP_DASHBOARDS_LOCK="${SETUP_DASHBOARDS_LOCK:-/tmp/setup-dashboards.lock}"
if [ "${SETUP_DASHBOARDS_LOCKED:-0}" != "1" ] && command -v flock >/dev/null 2>&1; then
  export SETUP_DASHBOARDS_LOCKED=1
  exec flock "${SETUP_DASHBOARDS_LOCK}" "$0" "$@"
fi

current_dashboard_manifest() {
  if [ -f "${DASHBOARDS_DIR}/.manifest.sha256" ]; then
    cat "${DASHBOARDS_DIR}/.manifest.sha256"
  else
    find "${DASHBOARDS_DIR}" -maxdepth 1 -name '*.json' -type f | LC_ALL=C sort | xargs sha256sum | sha256sum
  fi
}

dashboard_manifest_unchanged() {
  [ -f "${DASHBOARD_MANIFEST_FILE}" ] || return 1
  [ "$(current_dashboard_manifest)" = "$(cat "${DASHBOARD_MANIFEST_FILE}")" ]
}

write_dashboard_manifest() {
  mkdir -p "$(dirname "${DASHBOARD_MANIFEST_FILE}")"
  current_dashboard_manifest > "${DASHBOARD_MANIFEST_FILE}"
}

mb_auth_header() {
  if [ -n "${METABASE_API_KEY}" ]; then
    echo "x-api-key: ${METABASE_API_KEY}"
  else
    echo "X-Metabase-Session: ${SESSION_TOKEN}"
  fi
}

# Ретраи только фазы соединения (--retry-connrefused/timeout): запрос ещё не отправлен,
# поэтому повтор безопасен и для POST; переживает кратковременные обрывы сети до инстанса.
api_request() {
  local method="$1" path="$2" payload="${3:-}"
  local response code body
  if [ -z "${payload}" ]; then
    response=$(curl -sS --retry 5 --retry-delay 3 --retry-connrefused --connect-timeout 20 \
      -w "\n%{http_code}" -X "${method}" "${METABASE_URL}${path}" \
      -H "$(mb_auth_header)")
  else
    response=$(curl -sS --retry 5 --retry-delay 3 --retry-connrefused --connect-timeout 20 \
      -w "\n%{http_code}" -X "${method}" "${METABASE_URL}${path}" \
      -H "Content-Type: application/json" \
      -H "$(mb_auth_header)" \
      -d "${payload}")
  fi
  code=$(echo "${response}" | tail -n1)
  body=$(echo "${response}" | sed '$d')
  [[ "${code}" =~ ^2 ]] || fail "Metabase API ${method} ${path} returned HTTP ${code}: ${body}"
  echo "${body}"
}

# Как api_request, но не валит провижининг при не-2xx: для необязательных дефолтов инстанса,
# которые в разных версиях Metabase бывают read-only или называются иначе.
api_request_optional() {
  local method="$1" path="$2" payload="${3:-}" response code body
  if [ -z "${payload}" ]; then
    response=$(curl -sS --retry 5 --retry-delay 3 --retry-connrefused --connect-timeout 20 \
      -w "\n%{http_code}" -X "${method}" "${METABASE_URL}${path}" \
      -H "$(mb_auth_header)")
  else
    response=$(curl -sS --retry 5 --retry-delay 3 --retry-connrefused --connect-timeout 20 \
      -w "\n%{http_code}" -X "${method}" "${METABASE_URL}${path}" \
      -H "Content-Type: application/json" \
      -H "$(mb_auth_header)" \
      -d "${payload}")
  fi
  code=$(echo "${response}" | tail -n1)
  body=$(echo "${response}" | sed '$d')
  if [[ "${code}" =~ ^2 ]]; then
    echo "${body}"
  else
    log_info "WARN: ${method} ${path} -> HTTP ${code} (необязательная настройка пропущена)"
  fi
}

login() {
  local body setup_token payload
  if [ -n "${METABASE_API_KEY}" ]; then
    # Импортёру нужны права администратора: без них PUT /api/field и архивация упадут позже
    # с невнятными 403 — проверяем сразу.
    body=$(api_request GET "/api/user/current")
    [ "$(echo "${body}" | jq -r '.is_superuser // false')" = "true" ] ||
      fail "METABASE_API_KEY не даёт прав администратора (см. /api/user/current)"
    return
  fi
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
            ssl: false,
            "report-timezone": "Europe/Moscow"
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
      jq -r --arg db "${APP_DB_NAME}" '[.data[]? | select(.details.dbname == $db or .name == $db) | .id][0] // empty'
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
          ssl: false,
          "report-timezone": "Europe/Moscow"
        }
      }'
  )
  APP_DB_ID=$(api_request POST "/api/database" "${payload}" | jq -r '.id')
  [ -n "${APP_DB_ID}" ] && [ "${APP_DB_ID}" != "null" ] || fail "cannot register DWH database '${APP_DB_NAME}'"
}

ensure_app_database_report_timezone() {
  local current_tz payload
  current_tz=$(
    api_request GET "/api/database/${APP_DB_ID}" |
      jq -r '.details["report-timezone"] // empty'
  )
  if [ "${current_tz}" = "Europe/Moscow" ]; then
    return
  fi

  log_info "Setting Metabase report-timezone=Europe/Moscow for database id ${APP_DB_ID}"
  payload=$(
    api_request GET "/api/database/${APP_DB_ID}" |
      jq '.details["report-timezone"] = "Europe/Moscow" | {details: .details, engine: .engine, name: .name}'
  )
  api_request PUT "/api/database/${APP_DB_ID}" "${payload}" >/dev/null
}

# Группировку по суткам на дашбордах задаёт ГЛОБАЛЬНая настройка report-timezone, а не
# per-database деталь выше: при пустой глобальной настройке Metabase раскладывает дни в UTC,
# и сутки МСК «уезжают» на день назад. Пинуем её на Europe/Moscow, чтобы дашборды считали
# границу суток по Москве.
ensure_global_report_timezone() {
  local current_tz
  # Metabase (v0.62+) отдаёт строковые настройки сырым текстом, а не JSON — `jq` на «Europe/Moscow»
  # без кавычек падает с parse error и валит весь импорт. Парсим терпимо к обоим форматам.
  current_tz=$(api_request GET "/api/setting/report-timezone" | tr -d '"' | tr -d '[:space:]')
  if [ "${current_tz}" = "Europe/Moscow" ]; then
    return
  fi
  log_info "Setting global Metabase report-timezone=Europe/Moscow"
  api_request PUT "/api/setting/report-timezone" '{"value":"Europe/Moscow"}' >/dev/null
}

# Дефолты инстанса Metabase (идемпотентно на каждый прогон): язык — русский, формат времени —
# 24 часа сокращённый (HH:mm), валюта — рубль (₽), кеширование результатов — включено.
# Локация (часовой пояс Europe/Moscow) пинится ensure_global_report_timezone. PUT идемпотентен —
# повторная установка того же значения безопасна; ошибки не валят импорт (|| true).
ensure_localization_defaults() {
  log_info "Applying Metabase defaults: locale=ru, time=HH:mm, currency=RUB, query-caching=on"
  api_request_optional PUT "/api/setting/site-locale" '{"value":"ru"}' >/dev/null
  api_request_optional PUT "/api/setting/custom-formatting" \
    '{"value":{"type/Temporal":{"time_style":"HH:mm"},"type/Currency":{"currency":"RUB","currency_style":"symbol"}}}' >/dev/null
  # v0.62: enable-query-caching доступна только на чтение — результат-кеш включаем корневой
  # адаптивной TTL-стратегией через /api/cache (кеш живёт multiplier × время выполнения запроса).
  api_request_optional PUT "/api/cache" \
    '{"model":"root","model_id":0,"strategy":{"type":"ttl","multiplier":10,"min_duration_ms":1000}}' >/dev/null
}

ensure_collection() {
  COL_ID=$(
    api_request GET "/api/collection" |
      jq -r --arg name "${COLLECTION_NAME}" '[.[]? | select(.name == $name) | .id][0] // empty'
  )
  if [ -z "${COL_ID}" ] || [ "${COL_ID}" = "null" ]; then
    local payload
    payload=$(jq -nc --arg name "${COLLECTION_NAME}" '{name: $name, color: "#509EE3"}')
    COL_ID=$(api_request POST "/api/collection" "${payload}" | jq -r '.id')
  fi
  [ -n "${COL_ID}" ] && [ "${COL_ID}" != "null" ] || fail "cannot create or resolve collection '${COLLECTION_NAME}'"
}

required_public_objects() {
  {
    jq -r '.. | strings | scan("public\\.[A-Za-z_][A-Za-z0-9_]*")' "${DASHBOARDS_DIR}"/*.json 2>/dev/null || true
    if [ -d "${MODELS_DIR}" ]; then
      jq -r '.table_ref // empty' "${MODELS_DIR}"/*.json 2>/dev/null || true
    fi
  } | sort -u | sed 's/^public\.//'
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
          SELECT 1 FROM pg_matviews
          WHERE schemaname = 'public' AND matviewname = '${object}'
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
  if declare -F validate_model_fields_in_dwh >/dev/null; then
    validate_model_fields_in_dwh
  fi
}

sync_metabase_schema() {
  log_info "Requesting Metabase schema sync for ${APP_DB_NAME} (database id ${APP_DB_ID})"
  api_request POST "/api/database/${APP_DB_ID}/sync_schema" "{}" >/dev/null
  DB_METADATA_FILE="${DB_METADATA_FILE:-/tmp/metabase-db-metadata.json}"
  api_request GET "/api/database/${APP_DB_ID}/metadata" > "${DB_METADATA_FILE}"
}

required_field_filters() {
  {
    jq -r '
      .. | objects | select(has("metabase-field-filters"))
      | ."metabase-field-filters"[]?
      | select(.table_ref != null)
      | [.table_ref, .field_name]
      | @tsv
    ' "${DASHBOARDS_DIR}"/*.json
    jq -r '
      .. | objects | select(has("metabase-parameter-targets"))
      | ."metabase-parameter-targets"[]?
      | [.model_ref, .field_name]
      | @tsv
    ' "${DASHBOARDS_DIR}"/*.json 2>/dev/null || true
  } | sort -u
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

resolve_filter_table_ref() {
  local ref="$1"
  if [[ "${ref}" == public.* ]]; then
    printf '%s\n' "${ref}"
    return
  fi
  local model_file
  for model_file in "${MODELS_DIR}"/*.json; do
    [ -f "${model_file}" ] || continue
    if [ "$(jq -r '.name' "${model_file}")" = "${ref}" ]; then
      jq -r '.table_ref' "${model_file}"
      return
    fi
  done
  printf '\n'
}

wait_for_metabase_metadata() {
  local attempt table_ref field_name missing sample
  for attempt in $(seq 1 30); do
    api_request GET "/api/database/${APP_DB_ID}/metadata" > "${DB_METADATA_FILE}"
    missing=0
    sample=""
    while IFS=$'\t' read -r table_ref field_name; do
      [ -n "${table_ref}" ] || continue
      if [[ "${table_ref}" != public.* ]]; then
        table_ref="$(resolve_filter_table_ref "${table_ref}")"
      fi
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

existing_dashboard_id() {
  local dashboard_name="$1"
  if declare -F mb_all_dashboards_json >/dev/null; then
    mb_all_dashboards_json "${METABASE_URL}" "$(mb_auth_header)" |
      jq -r --arg name "${dashboard_name}" --argjson col "${COL_ID}" '[.[]? | select(.collection_id == $col and .name == $name) | .id][0] // empty'
  else
    api_request GET "/api/collection/${COL_ID}/items?models=dashboard&limit=1000" |
      jq -r --arg name "${dashboard_name}" '[.data[]? | select(.model == "dashboard" and .name == $name) | .id][0] // empty'
  fi
}

declare -gA CARD_REGISTRY=()

existing_card_id() {
  local card_name="$1"
  api_request GET "/api/card" |
    jq -r --arg name "${card_name}" --argjson col "${COL_ID}" '
      [.[]?
        | select(
            .collection_id == $col
            and .name == $name
            and (.archived | not)
            and ((.type // "question") != "model")
          )
        | .id]
      | if length > 0 then .[-1] else empty end
    '
}

archive_cards_by_name() {
  local card_name="$1"
  while IFS= read -r card_id; do
    [ -n "${card_id}" ] || continue
    log_info "Archiving card ${card_id}: ${card_name}"
    api_request PUT "/api/card/${card_id}" '{"archived":true}' >/dev/null
  done < <(
    api_request GET "/api/card" |
      jq -r --arg name "${card_name}" --argjson col "${COL_ID}" '
        [.[]?
          | select(
              .collection_id == $col
              and .name == $name
              and (.archived | not)
              and ((.type // "question") != "model")
            )
          | .id]
        | .[]
      '
  )
}

expected_card_names() {
  jq -r '.cards[]? | select((.display // "") != "text" and .name != null) | .name' "${DASHBOARDS_DIR}"/*.json | sort -u
}

archive_stale_collection_cards() {
  local expected_file card_id card_name
  expected_file="/tmp/metabase-expected-card-names.txt"
  expected_card_names > "${expected_file}"
  while IFS=$'\t' read -r card_id card_name; do
    [ -n "${card_id}" ] || continue
    if ! grep -Fxq "${card_name}" "${expected_file}"; then
      log_info "Archiving stale card ${card_id}: ${card_name}"
      api_request PUT "/api/card/${card_id}" '{"archived":true}' >/dev/null
    fi
  done < <(
    api_request GET "/api/collection/${COL_ID}/items?models=card&limit=1000" |
      jq -r '.data[]? | select(.model == "card") | [.id, .name] | @tsv'
  )
  rm -f "${expected_file}" >/dev/null 2>&1 || true
}

expected_dashboard_names() {
  jq -r '.name' "${DASHBOARDS_DIR}"/*.json | sort -u
}

# Импорт матчит дашборды по имени и обновляет на месте, но при переименовании дашборда в JSON
# старый (со старым именем) остаётся в коллекции дублем. Архивируем дашборды коллекции, чьих
# имён больше нет в JSON — зеркало archive_stale_collection_cards для дашбордов.
archive_stale_collection_dashboards() {
  local expected_file dashboard_id dashboard_name
  expected_file="/tmp/metabase-expected-dashboard-names.txt"
  expected_dashboard_names > "${expected_file}"
  while IFS=$'\t' read -r dashboard_id dashboard_name; do
    [ -n "${dashboard_id}" ] || continue
    if ! grep -Fxq "${dashboard_name}" "${expected_file}"; then
      log_info "Archiving stale dashboard ${dashboard_id}: ${dashboard_name}"
      api_request PUT "/api/dashboard/${dashboard_id}" '{"archived":true}' >/dev/null
    fi
  done < <(
    api_request GET "/api/collection/${COL_ID}/items?models=dashboard&limit=1000" |
      jq -r '.data[]? | select(.model == "dashboard") | [.id, .name] | @tsv'
  )
  rm -f "${expected_file}" >/dev/null 2>&1 || true
}

dashboard_payload() {
  local file="$1"
  local models_file="${MODEL_REGISTRY_FILE}"
  [ -f "${file}" ] || fail "card definition file missing: ${file}"
  [ -s "${DB_METADATA_FILE}" ] || fail "Metabase DB metadata cache missing: ${DB_METADATA_FILE}"
  [ -f "${models_file}" ] || printf '{}' > "${models_file}"
  jq --argjson db "${APP_DB_ID}" --argjson col "${COL_ID}" \
    --slurpfile meta_file "${DB_METADATA_FILE}" \
    --slurpfile models_file "${models_file}" '
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

    def model_entry($name):
      $models_file[0][$name] // error("Unknown Metabase model: " + $name);

    def resolve_mbql:
      walk(
        if type == "string" and startswith("model:") then
          "card__" + (model_entry(.[6:]).model_id | tostring)
        elif type == "array" and length >= 2 and .[0] == "field" and (.[1] | type) == "string" and (.[1] | test(":")) then
          (.[1] | split(":")) as $parts
          | if ($parts | length) == 2 then
              ["field", model_entry($parts[0]).fields[$parts[1]], .[2]]
            else . end
        else .
        end
      );

    def bind_field_filters:
      if type == "object" and has("dataset_query") and has("metabase-field-filters") then
        reduce (."metabase-field-filters" | to_entries[]) as $filter (.;
          if ($filter.value.model_ref // "") != "" then
            .
          else
            ($filter.value.table_ref // "") as $table_ref
            | ($filter.value.field_name // "") as $field_name
            | (field_id($table_ref; $field_name)) as $field_id
            | if $field_id == null then
                error("Cannot resolve Metabase field id for " + $table_ref + "." + $field_name)
              else
                .dataset_query.native."template-tags"[$filter.key].dimension = ["field", $field_id, null]
              end
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

    def normalize_query_card:
      if type == "object"
        and (.dataset_query.type // "") == "query"
        and (.dataset_query.query | type) == "object" then
        .dataset_query.query = (.dataset_query.query | resolve_mbql)
      else .
      end;

    del(.id)
    | .collection_id = $col
    | walk(set_database | bind_field_filters)
    | normalize_query_card
    | del(."query_tier")
    | del(."source_model")
    | del(."metabase-model-drill-params")
  ' "${file}"
}

card_query_fingerprint() {
  jq -c '
    def native_sql:
      if (.dataset_query.type // "") == "query" then
        .dataset_query.query
      else
        (.dataset_query.native.query // .dataset_query.stages[0].native // "")
      end;
    {display: (.display // ""), query: native_sql}
  '
}

card_dimension_tags_ready() {
  jq -e '
    def native_sql:
      if (.dataset_query.type // "") == "query" then
        .dataset_query.query // ""
      else
        (.dataset_query.native.query // .dataset_query.stages[0].native // "")
      end;
    def tags:
      (.dataset_query.native."template-tags" // .dataset_query.stages[0]."template-tags" // {});
    def has_vars:
      (native_sql | test("\\{\\{"));
    def dims:
      [tags | to_entries[]? | select(.value.type == "dimension")];
    (has_vars | not)
    or (
      (dims | length) > 0
      and ([dims[] | select(.value.dimension == null)] | length) == 0
    )
  '
}

card_native_query_missing() {
  jq -e '
    def native_sql:
      if (.dataset_query.type // "") == "query" then
        .dataset_query.query // ""
      else
        (.dataset_query.native.query // .dataset_query.stages[0].native // "")
      end;
    (native_sql | length) == 0
  '
}

create_or_update_card() {
  local file="$1"
  local payload card_id card_name existing_fp new_fp existing_card raw_payload
  raw_payload="$(dashboard_payload "${file}")" || fail "dashboard_payload failed for ${file}"
  payload="$(printf '%s' "${raw_payload}" | jq '{
        name,
        description,
        collection_id,
        dataset_query,
        display,
        visualization_settings,
        table_id: (.table_id // null)
      }')"
  card_name="$(jq -r '.name' "${file}")"
  new_fp="$(printf '%s' "${payload}" | card_query_fingerprint)"
  card_id="$(existing_card_id "${card_name}")"
  if [ -n "${card_id}" ]; then
    existing_card="$(api_request GET "/api/card/${card_id}")"
    if printf '%s' "${existing_card}" | jq -e '(.query_type // "") == "query"' >/dev/null \
      && printf '%s' "${payload}" | jq -e '(.dataset_query.type // "") == "native"' >/dev/null; then
      log_info "Migrating card from Query Builder to native SQL: ${card_name}"
      api_request PUT "/api/card/${card_id}" "${payload}" >/dev/null
      printf '%s\n' "${card_id}"
      return 0
    fi
    if printf '%s' "${existing_card}" | card_native_query_missing >/dev/null; then
      log_info "Repairing card with missing native query: ${card_name}"
      api_request PUT "/api/card/${card_id}" "${payload}" >/dev/null
      printf '%s\n' "${card_id}"
      return 0
    fi
    if ! printf '%s' "${existing_card}" | card_dimension_tags_ready >/dev/null \
      && printf '%s' "${payload}" | jq -e 'def native_sql: if (.dataset_query.type // "") == "query" then .dataset_query.query // "" else (.dataset_query.native.query // "") end; native_sql | test("\\{\\{")' >/dev/null; then
      log_info "Rebinding broken field filters for card: ${card_name}"
      api_request PUT "/api/card/${card_id}" "${payload}" >/dev/null
      printf '%s\n' "${card_id}"
      return 0
    fi
    existing_fp="$(printf '%s' "${existing_card}" | card_query_fingerprint)"
    if [ "${existing_fp}" = "${new_fp}" ]; then
      if [ "${METABASE_FORCE_PROVISION}" = "always" ] \
        || [ "${METABASE_FORCE_PROVISION}" = "force" ] \
        || [ "${METABASE_FORCE_PROVISION}" = "true" ]; then
        log_info "Force refresh collection card: ${card_name}"
        archive_cards_by_name "${card_name}"
        card_id="$(api_request POST "/api/card" "${payload}" | jq -r '.id')"
        [ -n "${card_id}" ] && [ "${card_id}" != "null" ] || fail "cannot force-recreate card from ${file}"
        printf '%s\n' "${card_id}"
        return 0
      fi
      if printf '%s' "${existing_card}" | card_dimension_tags_ready >/dev/null \
        && printf '%s' "${payload}" | card_dimension_tags_ready >/dev/null; then
        # Shared cards: refresh display + visualization_settings. A later dashboard JSON may omit
        # metabase-field-filters while reusing the same SQL; rewriting dataset_query would
        # drop Metabase field-id bindings and break dashboard filters.
        api_request PUT "/api/card/${card_id}" "$(printf '%s' "${payload}" | jq '{display, visualization_settings, description}')" >/dev/null
        printf '%s\n' "${card_id}"
        return 0
      fi
      log_info "Rebinding field filters for shared card: ${card_name}"
      api_request PUT "/api/card/${card_id}" "${payload}" >/dev/null
      printf '%s\n' "${card_id}"
      return 0
    fi
    # Metabase merges native-query metadata on card updates and can keep stale
    # template tags that are no longer present in dashboard JSON. Recreate the
    # card object while preserving the dashboard object itself.
    archive_cards_by_name "${card_name}"
  fi
  card_id="$(api_request POST "/api/card" "${payload}" | jq -r '.id')"
  [ -n "${card_id}" ] && [ "${card_id}" != "null" ] || fail "cannot create or update card from ${file}"
  printf '%s\n' "${card_id}"
}

sync_all_cards() {
  local file i card_file card_name
  for file in "${DASHBOARDS_DIR}"/*.json; do
    [ -f "${file}" ] || continue
    local num_cards
    num_cards="$(jq '.cards | length' "${file}")"
    for i in $(seq 0 $((num_cards - 1))); do
      card_file="/tmp/metabase-import-card-$(basename "${file}" .json)-${i}.json"
      jq -e -c ".cards[${i}]" "${file}" > "${card_file}" || fail "invalid card ${i} in ${file}"
      if [ "$(jq -r '.display // empty' "${card_file}")" = "text" ]; then
        continue
      fi
      card_name="$(jq -r '.name' "${card_file}")"
      CARD_REGISTRY["${card_name}"]="$(create_or_update_card "${card_file}")"
    done
  done
}

prepare_dashboard_tabs() {
  local file="$1" dashboard_id="$2" tab_map_file="$3"
  local existing_tabs ordered_tabs tab_count existing_count tab_sync_response

  existing_tabs="$(api_request GET "/api/dashboard/${dashboard_id}" | jq -c '.tabs // []')"
  ordered_tabs="$(
    jq -c --argjson existing "${existing_tabs}" '
      [.tabs[]? as $t
        | ($existing | map(select(.name == $t.name)) | .[0].id // null) as $existing_id
        | {
            slug: $t.id,
            position: ($t.position // 0),
            id: (if $existing_id != null then $existing_id else -(($t.position // 0) + 1) end),
            name: $t.name
          }
      ]
      | sort_by(.position)
      | map({id, name})
    ' "${file}"
  )"

  tab_count="$(jq '.tabs | length // 0' "${file}")"
  existing_count="$(jq 'length' <<< "${existing_tabs}")"
  if [ "${tab_count}" -gt 0 ] && {
    jq -e '[.[] | select(.id < 0)] | length > 0' <<< "${ordered_tabs}" >/dev/null 2>&1 ||
    [ "${tab_count}" -ne "${existing_count}" ]
  }; then
    api_request PUT "/api/dashboard/${dashboard_id}/cards" '{"cards":[]}' >/dev/null
    existing_tabs="$(api_request GET "/api/dashboard/${dashboard_id}" | jq -c '.tabs // []')"
    ordered_tabs="$(
      jq -c --argjson existing "${existing_tabs}" '
        [.tabs[]? as $t
          | ($existing | map(select(.name == $t.name)) | .[0].id // null) as $existing_id
          | {
              position: ($t.position // 0),
              id: (if $existing_id != null then $existing_id else -(($t.position // 0) + 1) end),
              name: $t.name
            }
        ]
        | sort_by(.position)
        | map({id, name})
      ' "${file}"
    )"
    tab_sync_response="$(
      api_request PUT "/api/dashboard/${dashboard_id}/cards" \
        "$(jq -n --argjson tabs "${ordered_tabs}" '{tabs: $tabs, cards: []}')"
    )"
    existing_tabs="$(
      jq -c '
        if (.tabs // []) | length > 0 then .tabs
        elif (.ordered_tabs // []) | length > 0 then .ordered_tabs
        else [] end
      ' <<< "${tab_sync_response}"
    )"
    if [ "$(jq 'length' <<< "${existing_tabs}")" -eq 0 ]; then
      existing_tabs="$(api_request GET "/api/dashboard/${dashboard_id}" | jq -c '.tabs // []')"
    fi
  fi

  jq -c --argjson existing "${existing_tabs}" '
    reduce (.tabs[]?) as $t ({};
      . + {($t.id): (
        ($existing | map(select(.name == $t.name)) | .[0].id)
      )}
    )
  ' "${file}" > "${tab_map_file}"

  if [ "${tab_count}" -gt 0 ]; then
    local missing_slug
    missing_slug="$(
      jq -r --argjson map "$(cat "${tab_map_file}")" '
        [.tabs[]?.id | select($map[.] == null)]
        | if length > 0 then .[0] else empty end
      ' "${file}"
    )"
    [ -z "${missing_slug}" ] || fail "cannot resolve Metabase tab id for slug '${missing_slug}' in ${file}"
  fi

  jq -c --argjson existing "${existing_tabs}" '
    [.tabs[]? as $t
      | ($existing | map(select(.name == $t.name)) | .[0].id) as $id
      | if $id == null then empty else {id: $id, name: $t.name} end
    ]
  ' "${file}"
}

model_click_behavior_json() {
  local card_file="$1"
  local dashboard_id="$2"
  local saved_parameters="$3"
  local tab_map_file="$4"
  jq -c \
    --arg dashId "${dashboard_id}" \
    --argjson dashParams "${saved_parameters}" \
    --slurpfile tabMap "${tab_map_file}" \
    --slurpfile models_file "${MODEL_REGISTRY_FILE}" '
    def resolve_param_id($slug):
      ($dashParams[] | select(.slug == $slug) | .id) // empty;

    def model_field_id($model_ref; $field_name):
      ($models_file[0][$model_ref].fields[$field_name]) // empty;

    def model_dimension($model_ref; $field_name):
      (model_field_id($model_ref; $field_name)) as $fid
      | ["dimension", ["field", $fid, {"stage-number": 0}]];

    def model_dimension_key($model_ref; $field_name):
      model_dimension($model_ref; $field_name) | tojson;

    def dashboard_param_slug($base):
      $base + "_filter";

    (."metabase-model-drill-params" // {}) as $drillParams
    | (.click_behavior // null) as $raw
    | if $raw == null or ($raw | type) != "object" or ($raw.type // "") != "link" then
        empty
      elif ($raw.linkType // "") == "question" and ($raw.targetModel // "") != "" then
        (
          $raw
          | del(.targetModel, .targetQuestion, .targetDashboard, .tab)
          | .targetId = ($models_file[0][$raw.targetModel].model_id | tonumber)
          | ($raw.targetModel) as $mref
          | .parameterMapping = (
              reduce (($raw.parameterMapping // {}) | to_entries[]) as $entry ({};
                ($entry.value.target.model_ref // $mref) as $emref
                | ($entry.value.target.field_name // $entry.key) as $fname
                | (model_field_id($emref; $fname) // null) as $fid
                # reduce-safe: пустой select() обнулял бы весь аккумулятор; ветвим через if.
                | if ($fid == null or $fid == "") then .
                  else
                    (["dimension", ["field", $fid, {"stage-number": 0}]]) as $dim
                    | ($dim | tojson) as $dimKey
                    | .[$dimKey] = (
                        $entry.value
                        | .target = (
                            {"type": "dimension", "id": $dimKey, "dimension": $dim}
                            + (if ($entry.value.target.operator // "") != ""
                               then {"operator": $entry.value.target.operator}
                               else {} end)
                          )
                        | .source = (.source + {id: (.source.name // "")})
                      )
                  end
              )
              | reduce ($drillParams | to_entries[]) as $entry (.;
                (resolve_param_id(dashboard_param_slug($entry.key))) as $pid
                | (model_field_id($mref; $entry.value) // null) as $dfid
                # Пропускаем параметр без id ИЛИ если измерение уже задано колонкой —
                # через if, а не select(): select() в reduce обнулял parameterMapping.
                | if ($pid == null or $pid == "" or $dfid == null) then .
                  else
                    (["dimension", ["field", $dfid, {"stage-number": 0}]]) as $dim
                    | ($dim | tojson) as $dimKey
                    | if (.[$dimKey] != null) then .
                      else
                        .[$dimKey] = {
                          "source": {
                            "type": "parameter",
                            "id": $pid,
                            "name": (($dashParams[] | select(.id == $pid) | .name) // $entry.key)
                          },
                          "target": {"type": "dimension", "id": $dimKey, "dimension": $dim}
                        }
                      end
                  end
              )
            )
        )
      else
        empty
      end
    ' "${card_file}"
}

finalize_dashboard_model_drills() {
  local dashboard_id="$1"
  local file="$2"
  local dashboard_slug="$3"
  local tab_map_file="$4"
  local saved_parameters="$5"
  local ordered_tabs_json="$6"
  local num_cards i card_file card_name card_id click_payload cards_payload

  cards_payload="$(api_request GET "/api/dashboard/${dashboard_id}" | jq -c '
    (.dashcards // []) | map({
      id,
      card_id,
      card_name: (.card.name // ""),
      dashboard_tab_id,
      action_id,
      size_x,
      size_y,
      row,
      col,
      parameter_mappings: (.parameter_mappings // []),
      series: (.series // []),
      visualization_settings: (.visualization_settings // {})
    })
  ')"
  num_cards="$(jq '.cards | length' "${file}")"
  for i in $(seq 0 $((num_cards - 1))); do
    card_file="/tmp/metabase-dashcard-${dashboard_slug}-${i}.json"
    jq -e -c ".cards[${i}]" "${file}" > "${card_file}" || continue
    if [ "$(jq -r '.click_behavior.linkType // empty' "${card_file}")" != "question" ]; then
      continue
    fi
    card_name="$(jq -r '.name' "${card_file}")"
    click_payload="$(model_click_behavior_json "${card_file}" "${dashboard_id}" "${saved_parameters}" "${tab_map_file}")"
    if [ -z "${click_payload}" ]; then
      log_info "skip finalize model drill for ${card_name}: empty click payload"
      continue
    fi
    log_info "finalize model drill for ${card_name}"
    cards_payload="$(jq -c \
      --arg cname "${card_name}" \
      --argjson click "${click_payload}" \
      'map(
        if .card_name == $cname then
          .visualization_settings = ((.visualization_settings // {}) + {click_behavior: $click})
        else
          .
        end
      )' <<< "${cards_payload}")"
  done

  api_request PUT "/api/dashboard/${dashboard_id}/cards" \
    "$(jq -n --argjson cards "$(jq -c 'map(del(.card_name))' <<< "${cards_payload}")" --argjson tabs "${ordered_tabs_json}" '{tabs: $tabs, cards: $cards}')" \
    >/dev/null
}

create_or_update_dashboard() {
  local file="$1"
  local payload dashboard_id saved_parameters cards num_cards i
  local dashboard_name tab_map_file ordered_tabs_json dashboard_slug
  local existing_dashcards used_dashcard_ids existing_id
  dashboard_name="$(jq -r '.name' "${file}")"
  dashboard_slug="$(basename "${file}" .json)"
  tab_map_file="/tmp/metabase-tab-map-${dashboard_slug}.json"
  ordered_tabs_json="[]"
  if [ "$(jq '.tabs | length // 0' "${file}")" -gt 0 ]; then
    echo '{}' > "${tab_map_file}"
  else
    echo '{}' > "${tab_map_file}"
  fi

  payload="$(
    dashboard_payload "${file}" |
      jq --arg auto_apply "${METABASE_AUTO_APPLY_FILTERS}" '{
        name,
        description,
        collection_id,
        parameters: (.parameters // []),
        width: (.width // "full"),
        cacheables: [],
        auto_apply_filters: ($auto_apply == "true" or $auto_apply == "1")
      }'
  )"
  dashboard_id="$(existing_dashboard_id "${dashboard_name}")"
  if [ -n "${dashboard_id}" ]; then
    api_request PUT "/api/dashboard/${dashboard_id}" "${payload}" >/dev/null
  else
    dashboard_id="$(api_request POST "/api/dashboard" "${payload}" | jq -r '.id')"
    # POST ignores width in Metabase v0.60 — apply via PUT after creation
    api_request PUT "/api/dashboard/${dashboard_id}" "${payload}" >/dev/null
  fi
  [ -n "${dashboard_id}" ] && [ "${dashboard_id}" != "null" ] || fail "cannot create or update dashboard from ${file}"

  if [ "$(jq '.tabs | length // 0' "${file}")" -gt 0 ]; then
    ordered_tabs_json="$(prepare_dashboard_tabs "${file}" "${dashboard_id}" "${tab_map_file}")"
  fi

  saved_parameters="$(api_request GET "/api/dashboard/${dashboard_id}" | jq -c '.parameters // []')"
  existing_dashcards="$(api_request GET "/api/dashboard/${dashboard_id}" | jq -c '.dashcards // []')"
  cards="[]"
  # Одна карточка размещается на нескольких вкладках: без учёта вкладки и уже
  # занятых id одно и то же существующее id дашкарты попадает в payload дважды,
  # и PUT /api/dashboard/{id}/cards отвечает 400 «id уникальны».
  used_dashcard_ids="[]"
  num_cards="$(jq '.cards | length' "${file}")"
  if [ "${num_cards}" -gt 0 ]; then
    for i in $(seq 0 $((num_cards - 1))); do
      local card_file card_id card_id_json viz_settings size_x size_y row col mappings dashcard_id dashcard tab_slug dashboard_tab_id_json
      card_file="/tmp/metabase-dashcard-${dashboard_slug}-${i}.json"
      jq -e -c ".cards[${i}]" "${file}" > "${card_file}" || fail "invalid dashcard ${i} in ${file}"

      if [ "$(jq -r '.display // empty' "${card_file}")" = "text" ]; then
        card_id_json="null"
        viz_settings="$(jq -c '{
          virtual_card: {name: null, display: "text", dataset_query: {}, visualization_settings: {}},
          text: (.text // "")
        }' "${card_file}")"
        # Текстовые карточки подставляют {{slug}} из значения dashboard-параметра с тем же
        # slug — связь идёт через text-tag mapping (card_id: null). Параметр приходит по URL.
        mappings="$(
          jq -c --argjson dashParams "${saved_parameters}" '
            [ (.text // "") | scan("\\{\\{[[:space:]]*([A-Za-z0-9_]+)[[:space:]]*\\}\\}") | .[0] ]
            | unique
            | map(. as $tag
                | ($dashParams[] | select(.slug == $tag) | .id) as $pid
                | select($pid != null)
                | {parameter_id: $pid, card_id: null, target: ["text-tag", $tag]})
          ' "${card_file}"
        )"
      else
        card_name="$(jq -r '.name' "${card_file}")"
        card_id="${CARD_REGISTRY[${card_name}]:-}"
        [ -n "${card_id}" ] || fail "card is missing from registry after sync: ${card_name}"
        card_id_json="${card_id}"
        viz_settings="$(
          jq -c \
            --arg dashId "${dashboard_id}" \
            --argjson dashParams "${saved_parameters}" \
            --slurpfile tabMap "${tab_map_file}" \
            --slurpfile models_file "${MODEL_REGISTRY_FILE}" '
            def resolve_param_id($slug):
              ($dashParams[] | select(.slug == $slug) | .id) // empty;

            def model_field_id($model_ref; $field_name):
              ($models_file[0][$model_ref].fields[$field_name]) // empty;

            def model_dimension($model_ref; $field_name):
              (model_field_id($model_ref; $field_name)) as $fid
              | ["dimension", ["field", $fid, {"stage-number": 0}]];

            def model_dimension_key($model_ref; $field_name):
              model_dimension($model_ref; $field_name) | tojson;

            def dashboard_param_slug($base):
              $base + "_filter";

            (."metabase-model-drill-params" // {}) as $drillParams
            | (.click_behavior // null) as $raw
            | if $raw == null or ($raw | type) != "object" or ($raw.type // "") != "link" then
                {}
              elif ($raw.linkType // "") == "question" and ($raw.targetModel // "") != "" then
                # Дрилл в модель собирается ОДНИМ местом — finalize_dashboard_model_drills
                # (через model_click_behavior_json), поверх уже выставленных настроек
                # dashcard. Здесь не дублируем сборку click_behavior.
                {}
              else
                {
                  click_behavior: (
                    $raw
                    | del(.targetDashboard, .tab)
                    | .targetId = ($dashId | tonumber)
                    | (if ($raw.tab // "") != "" then .tabId = ($tabMap[0][$raw.tab] // null) else . end)
                    | .parameterMapping = (
                        reduce (($raw.parameterMapping // {}) | to_entries[]) as $entry ({};
                          (resolve_param_id($entry.key) // $entry.value.target.id // empty) as $pid
                          | select($pid != "")
                          | .[$pid] = (
                              $entry.value
                              | .target = {"type": "parameter", "id": $pid}
                              | .source = (.source + {id: (.source.name // "")})
                            )
                        )
                      )
                  )
                }
              end
            ' "${card_file}"
        )"
        mappings="$(
          jq -c --argjson cardIndex "${i}" --argjson dashParams "${saved_parameters}" --argjson cardDbId "${card_id}" \
            --slurpfile models_file "${MODEL_REGISTRY_FILE}" '
            def param_base($slug):
              if (($slug // "") | endswith("_filter")) then
                ($slug | sub("_filter$"; ""))
              else
                ($slug // "")
              end;

            def tag_candidates($base):
              [$base];

            def resolve_tag($slug; $tagKeys):
              tag_candidates(param_base($slug))
              | map(select(. as $t | ($tagKeys | index($t)) != null))
              | .[0] // empty;

            def model_field_id($model_ref; $field_name):
              ($models_file[0][$model_ref].fields[$field_name]) // empty;

            (.cards[$cardIndex].dataset_query.native["template-tags"] // {}) as $tags
            | ($tags | keys) as $tagKeys
            | (.cards[$cardIndex]."metabase-parameter-targets" // {}) as $qbTargets
            | (
                [
                  $dashParams[] as $param
                  | resolve_tag($param.slug; $tagKeys) as $tagName
                  | select($tagName != null and $tagName != "")
                  | ($tags[$tagName].type // "") as $tagType
                  | {
                      parameter_id: $param.id,
                      card_id: $cardDbId,
                      target: (
                        if $tagType == "dimension" then
                          ["dimension", ["template-tag", $tagName]]
                        else
                          ["variable", ["template-tag", $tagName]]
                        end
                      )
                    }
                ]
                + [
                  $dashParams[] as $param
                  | param_base($param.slug) as $base
                  | ($qbTargets[$base] // $qbTargets[param_base($param.slug)] // empty) as $target
                  | select($target != null and ($target.model_ref // "") != "")
                  | (model_field_id($target.model_ref; $target.field_name)) as $fieldId
                  | select($fieldId != null and $fieldId != "")
                  | {
                      parameter_id: $param.id,
                      card_id: $cardDbId,
                      target: ["dimension", ["field", $fieldId, {"stage-number": 0}]]
                    }
                ]
              )
            | group_by(.parameter_id)
            | map(
                (map(select(.target[1][0] == "field")) | if length > 0 then .[0] else empty end)
                // .[0]
              )
          ' "${file}"
        )"
      fi

      size_x="$(jq -r ".cards[${i}].sizeX // .cards[${i}].size_x // 4" "${file}")"
      size_y="$(jq -r ".cards[${i}].sizeY // .cards[${i}].size_y // 4" "${file}")"
      row="$(jq -r ".cards[${i}].row // 0" "${file}")"
      col="$(jq -r ".cards[${i}].col // 0" "${file}")"

      tab_slug="$(jq -r '.tab // empty' "${card_file}")"
      dashboard_tab_id_json="null"
      if [ -n "${tab_slug}" ]; then
        dashboard_tab_id_json="$(jq -r --arg tab "${tab_slug}" '.[$tab] // null' "${tab_map_file}")"
        [ "${dashboard_tab_id_json}" != "null" ] || fail "unknown dashboard tab '${tab_slug}' in ${file}"
      fi

      dashcard_id=$((-(i + 1)))
      if [ "${card_id_json}" != "null" ]; then
        existing_id="$(jq -r \
          --argjson cid "${card_id_json}" \
          --argjson tabId "${dashboard_tab_id_json}" \
          --argjson used "${used_dashcard_ids}" \
          '[.[] | select(.card_id == $cid) | select(.id | IN($used[]) | not)] as $free
           | (([$free[] | select((.dashboard_tab_id // null) == $tabId)] + $free) | .[0].id) // empty' \
          <<< "${existing_dashcards}")"
        if [ -n "${existing_id}" ]; then
          dashcard_id="${existing_id}"
          used_dashcard_ids="$(jq -c --argjson id "${existing_id}" '. + [$id]' <<< "${used_dashcard_ids}")"
        fi
      else
        existing_id="$(jq -r \
          --argjson row "${row}" \
          --argjson col "${col}" \
          --argjson tabId "${dashboard_tab_id_json}" \
          --argjson used "${used_dashcard_ids}" \
          '[.[] | select(.card_id == null and .row == $row and .col == $col and (.dashboard_tab_id // null) == $tabId) | select(.id | IN($used[]) | not) | .id][0] // empty' \
          <<< "${existing_dashcards}")"
        if [ -n "${existing_id}" ]; then
          dashcard_id="${existing_id}"
          used_dashcard_ids="$(jq -c --argjson id "${existing_id}" '. + [$id]' <<< "${used_dashcard_ids}")"
        fi
      fi

      dashcard="$(
        jq -n \
          --argjson dashcardId "${dashcard_id}" \
          --argjson cardId "${card_id_json}" \
          --argjson sizeX "${size_x}" \
          --argjson sizeY "${size_y}" \
          --argjson row "${row}" \
          --argjson col "${col}" \
          --argjson mappings "${mappings}" \
          --argjson vizSettings "${viz_settings}" \
          --argjson dashboardTabId "${dashboard_tab_id_json}" \
          '{
            id: $dashcardId,
            card_id: $cardId,
            dashboard_tab_id: $dashboardTabId,
            action_id: null,
            size_x: $sizeX,
            size_y: $sizeY,
            row: $row,
            col: $col,
            parameter_mappings: $mappings,
            series: [],
            visualization_settings: $vizSettings
          }'
      )"
      cards="$(jq -n --argjson existing "${cards}" --argjson card "${dashcard}" '$existing + [$card]')"
    done

    if [ "$(jq 'length' <<< "${ordered_tabs_json}")" -gt 0 ]; then
      ordered_tabs_json="$(api_request GET "/api/dashboard/${dashboard_id}" | jq -c '[.tabs[]? | {id, name}]')"
      [ "$(jq 'length' <<< "${ordered_tabs_json}")" -gt 0 ] || fail "dashboard ${dashboard_id} has no Metabase tabs after tab sync"
    fi

    api_request PUT "/api/dashboard/${dashboard_id}/cards" "$(jq -n --argjson cards "${cards}" --argjson tabs "${ordered_tabs_json}" '{tabs: $tabs, cards: $cards}')" >/dev/null
    finalize_dashboard_model_drills \
      "${dashboard_id}" \
      "${file}" \
      "${dashboard_slug}" \
      "${tab_map_file}" \
      "${saved_parameters}" \
      "${ordered_tabs_json}"
    rm -f "${tab_map_file}" >/dev/null 2>&1 || true
  fi

  printf '%s\n' "${dashboard_id}"
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

verify_dashboard_cards() {
  local file expected dashboard_id actual
  for file in "${DASHBOARDS_DIR}"/*.json; do
    expected="$(jq '.cards | length' "${file}")"
    [ "${expected}" -gt 0 ] || continue
    dashboard_id="$(existing_dashboard_id "$(jq -r '.name' "${file}")")"
    [ -n "${dashboard_id}" ] || fail "dashboard is missing after import: ${file}"
    actual="$(api_request GET "/api/dashboard/${dashboard_id}" | jq '(.dashcards // .ordered_cards // []) | length')"
    [ "${actual}" -eq "${expected}" ] || fail "dashboard ${dashboard_id} has ${actual}/${expected} card(s)"
  done
}

dashboards_present_with_cards() {
  local file expected dashboard_id actual
  for file in "${DASHBOARDS_DIR}"/*.json; do
    [ -f "${file}" ] || continue
    expected="$(jq '.cards | length' "${file}")"
    dashboard_id="$(existing_dashboard_id "$(jq -r '.name' "${file}")")"
    [ -n "${dashboard_id}" ] || return 1
    actual="$(api_request GET "/api/dashboard/${dashboard_id}" | jq '(.dashcards // .ordered_cards // []) | length')"
    [ "${actual}" -eq "${expected}" ] || return 1
  done
}

maybe_skip_dashboard_import() {
  case "${METABASE_FORCE_PROVISION}" in
    true|always|force)
      log_info "Forced dashboard provisioning is enabled."
      return
      ;;
    false|never|skip)
      log_info "Dashboard provisioning disabled by METABASE_FORCE_PROVISION=${METABASE_FORCE_PROVISION}."
      log_info "Setup complete: dashboard provisioning skipped."
      exit 0
      ;;
    auto)
      if dashboards_present_with_cards; then
        if dashboard_manifest_unchanged; then
          log_info "Dashboard manifest is unchanged and existing dashboards are complete; skipping import."
          log_info "Setup complete: collection '${COLLECTION_NAME}' is up to date."
          exit 0
        else
          log_info "Dashboard manifest changed; refreshing existing dashboards and cards."
          return
        fi
      fi
      ;;
    *)
      fail "Unsupported METABASE_FORCE_PROVISION=${METABASE_FORCE_PROVISION}; use auto, always, or never"
      ;;
  esac
}

# Публичная страница без авторизации для клиентского дашборда. Идемпотентно: включаем
# глобальный public-sharing и переиспользуем существующий public_uuid (живёт в app-БД
# Metabase). На публичной ссылке фильтр «JID Клиники» открыт и переопределяется через URL.
ensure_public_client_dashboard() {
  [ "${METABASE_PUBLIC_CLIENT_DASHBOARD}" = "true" ] || return 0
  local dashboard_id public_uuid
  # enable-public-sharing — глобальный флаг инстанса. На общем Metabase не включаем сами:
  # если он уже включён владельцем — публичная ссылка на наш дашборд всё равно создастся,
  # иначе шаг деградирует до лога «не удалось получить ссылку».
  if [ "${METABASE_MANAGE_INSTANCE_SETTINGS}" = "true" ]; then
    api_request PUT "/api/setting/enable-public-sharing" '{"value":true}' >/dev/null || true
  fi
  dashboard_id="$(existing_dashboard_id "${CLIENT_DASHBOARD_NAME}")"
  if [ -z "${dashboard_id}" ]; then
    log_info "Public sharing: client dashboard not provisioned yet; deferring public link."
    return 0
  fi
  public_uuid="$(api_request GET "/api/dashboard/${dashboard_id}" | jq -r '.public_uuid // empty')"
  if [ -z "${public_uuid}" ]; then
    public_uuid="$(api_request POST "/api/dashboard/${dashboard_id}/public_link" '{}' | jq -r '.uuid // empty')"
  fi
  if [ -n "${public_uuid}" ]; then
    log_info "Public client dashboard: ${METABASE_URL}/public/dashboard/${public_uuid}"
  else
    log_info "Public sharing: could not obtain public link for dashboard ${dashboard_id}."
  fi
}

log_info "Waiting for Metabase at ${METABASE_URL}"
until curl -sS --fail "${METABASE_URL}/api/health" >/dev/null; do
  sleep 5
done

login
resolve_or_create_app_database_id
ensure_app_database_report_timezone
# Глобальные настройки инстанса — только при явном opt-in (см. METABASE_MANAGE_INSTANCE_SETTINGS):
# на общем Metabase они бы переопределили часовой пояс/локаль/формат для чужих сервисов.
if [ "${METABASE_MANAGE_INSTANCE_SETTINGS}" = "true" ]; then
  ensure_global_report_timezone
  ensure_localization_defaults
else
  log_info "METABASE_MANAGE_INSTANCE_SETTINGS=false: пропускаю глобальные настройки инстанса (общий Metabase)."
fi
ensure_collection
# До любого fast-path/skip: если дашборд уже есть — обеспечиваем и логируем публичную ссылку.
ensure_public_client_dashboard
if [ "${METABASE_FORCE_PROVISION}" = "auto" ] \
  && [ "${METABASE_SKIP_IMPORT_IF_PRESENT}" = "true" ] \
  && dashboards_present_with_cards; then
  log_info "Fast path: dashboards already deployed for current manifest."
  log_info "Setup complete: collection '${COLLECTION_NAME}' is up to date."
  exit 0
fi
if [ "${METABASE_FORCE_PROVISION}" = "auto" ] && dashboards_present_with_cards && dashboard_manifest_unchanged; then
  log_info "Dashboard manifest is unchanged and dashboards are complete; skipping import."
  log_info "Setup complete: collection '${COLLECTION_NAME}' is up to date."
  exit 0
fi
validate_dwh_contract
sync_metabase_schema
if declare -F sync_all_models >/dev/null; then
  sync_all_models
else
  printf '{}' > "${MODEL_REGISTRY_FILE}"
fi
wait_for_metabase_metadata
maybe_skip_dashboard_import

log_info "Importing dashboards to collection '${COLLECTION_NAME}' from ${DASHBOARDS_DIR}"
api_request GET "/api/database/${APP_DB_ID}/metadata" > "${DB_METADATA_FILE}"
if declare -F build_model_registry >/dev/null; then
  build_model_registry
fi
archive_stale_collection_cards
sync_all_cards
for file in "${DASHBOARDS_DIR}"/*.json; do
  [ -f "${file}" ] || continue
  dashboard_name=$(jq -r '.name' "${file}")
  [ -n "${dashboard_name}" ] && [ "${dashboard_name}" != "null" ] || fail "dashboard file has no name: ${file}"
  create_or_update_dashboard "${file}" >/dev/null
  log_info "Imported ${dashboard_name}"
done

archive_stale_collection_dashboards

verify_collection_contents
verify_dashboard_cards
write_dashboard_manifest
# Первый импорт: дашборд только что создан — публикуем публичную ссылку.
ensure_public_client_dashboard
log_info "Setup complete: collection '${COLLECTION_NAME}' contains $(ls "${DASHBOARDS_DIR}"/*.json | wc -l | tr -d ' ') dashboard(s)."
