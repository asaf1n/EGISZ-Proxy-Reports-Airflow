#!/bin/bash
# Единственный путь поставки отчётов EGISZ в Metabase: все дашборды и native-карточки из
# DASHBOARDS_DIR/*.json (см. README «Metabase и дашборды», AGENTS «Metabase»). Пустой инстанс после первой настройки
# админа + готовой схемы Postgres — этот скрипт создаёт весь набор. Повторный запуск: очистка целевой коллекции и импорт заново.
set -euo pipefail

_script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=include/mb_list.sh
. "${_script_dir}/include/mb_list.sh"

METABASE_URL="${METABASE_URL:-http://localhost:3000}"
# Fallback, если переменные не заданы (в поде k8s задаются из Secret metabase-admin).
ADMIN_EMAIL="${ADMIN_EMAIL:-admin@egisz.local}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-egisz}"
DB_NAME="${DB_NAME:-egisz_reports}"
DB_USER="${DB_USER:-egisz}"
DB_PASSWORD="${DB_PASSWORD:-egisz}"
DB_DISPLAY_NAME="${DB_DISPLAY_NAME:-EGISZ Corp DWH}"
PGHOST="${PGHOST:-postgres}"
PGPORT="${PGPORT:-5432}"
# Каталог с JSON дашбордов (в образе /app/metabase_dashboards; локально можно смонтировать репозиторий).
DASHBOARDS_DIR="${METABASE_DASHBOARDS_DIR:-/app/metabase_dashboards}"
# true: при открытии дашборда сразу применять выбранные фильтры — без обязательного «Применить» в UI.
# Отключить (тяжёлый старт / нагрузка на PG рядом с ETL): METABASE_AUTO_APPLY_FILTERS=false.
MB_AUTO_APPLY_FILTERS="${METABASE_AUTO_APPLY_FILTERS:-true}"

ROOT_COLLECTION_NAME="EGISZ Corp Monitoring"

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
  else
    if [[ "${method}" == "POST" && ( "${path}" == "/api/card" || "${path}" == "/api/dashboard" ) ]]; then
      echo "Metabase API ${method} ${path} successful with HTTP ${HTTP_CODE} OK" >&2
    fi
    if [[ "${method}" == "PUT" && "${path}" =~ ^/api/dashboard/[0-9]+/cards$ ]]; then
      echo "Metabase API ${method} ${path} successful with HTTP ${HTTP_CODE} OK" >&2
    fi
  fi

  printf '%s' "${RESPONSE_BODY}"
}

# Metabase 0.48–0.60: иногда PUT /api/dashboard/:id/cards не сохраняет parameter_mappings (фильтры в шапке есть,
# карточки показывают «выберите столбец»). Тогда пересобираем связи с серверных template-tags и шлём PUT целого дашборда.
# GET /api/dashboard/:id иногда отдаёт dashcards только с card_id без вложенного .card — jq-ремонт не сработает без подгрузки.
enrich_dashboard_json_with_cards() {
  local j="$1"
  local cid merged

  for cid in $(echo "${j}" | jq -r '
    [(.dashcards // .ordered_cards // [])[]?
      | select((.card | type) != "object")
      | .card_id
    ] | map(select(. != null)) | unique | .[]?
  '); do
    [ -z "${cid}" ] || [ "${cid}" = "null" ] && continue
    merged="$(api_request GET "/api/card/${cid}")"
    j="$(echo "${j}" | jq --argjson card "${merged}" --argjson cid "${cid}" '
      if (.dashcards | type) == "array" then
        .dashcards |= map(
          if (.card_id == $cid) and ((.card | type) != "object") then . + { card: $card } else . end
        )
      else . end
      | if (.ordered_cards | type) == "array" then
          .ordered_cards |= map(
            if (.card_id == $cid) and ((.card | type) != "object") then . + { card: $card } else . end
          )
        else . end
    ')"
  done

  printf '%s' "${j}"
}

repair_dashboard_parameter_links_if_needed() {
  local dashboard_id="$1"
  local dash_json maps_total params_len repaired

  dash_json="$(api_request GET "/api/dashboard/${dashboard_id}")"
  maps_total="$(echo "${dash_json}" | jq '([ (.dashcards // .ordered_cards // [])[]? | (.parameter_mappings // []) | length ] | add) // 0')"
  params_len="$(echo "${dash_json}" | jq '(.parameters // []) | length')"
  if [ "${params_len}" -eq 0 ] || [ "${maps_total}" -gt 0 ]; then
    return 0
  fi

  log_info "Repairing parameter_mappings on dashboard id=${dashboard_id} (had params=${params_len}, mappings=0)…"
  dash_json="$(enrich_dashboard_json_with_cards "${dash_json}")"
  repaired="$(echo "${dash_json}" | jq '
    (.parameters // []) as $params
    | if (.dashcards | type) == "array" then
        (.dashcards) as $dcs
        | .dashcards = (
            $dcs
            | map(
                . as $dc
                | if ($dc.card | type) != "object"
                     or ($dc.card.dataset_query | type) != "object"
                     or ($dc.card.dataset_query.native | type) != "object" then
                    $dc
                  elif (($dc.card.dataset_query.native["template-tags"] // {}) | type) != "object" then
                    $dc
                  else
                    ($dc.card.dataset_query.native["template-tags"]) as $tags
                    | ($tags | keys) as $tagKeys
                    | $dc + {
                        "parameter_mappings": [
                          $params[] as $p
                          | ($p.slug // "") as $raw
                          | (if $raw == "" then empty elif ($raw | endswith("_filter")) then ($raw | sub("_filter$"; "")) else $raw end) as $tn
                          | select(($tagKeys | index($tn)) != null)
                          | ($tags[$tn].type // "") as $ttype
                          | {
                              parameter_id: $p.id,
                              card_id: $dc.card_id,
                              target: (
                                if $ttype == "dimension" then
                                  ["dimension", ["template-tag", $tn]]
                                else
                                  ["variable", ["template-tag", $tn]]
                                end
                              )
                            }
                        ]
                      }
                  end
              )
          )
      elif (.ordered_cards | type) == "array" then
        (.ordered_cards) as $dcs
        | .ordered_cards = (
            $dcs
            | map(
                . as $dc
                | if ($dc.card | type) != "object"
                     or ($dc.card.dataset_query | type) != "object"
                     or ($dc.card.dataset_query.native | type) != "object" then
                    $dc
                  elif (($dc.card.dataset_query.native["template-tags"] // {}) | type) != "object" then
                    $dc
                  else
                    ($dc.card.dataset_query.native["template-tags"]) as $tags
                    | ($tags | keys) as $tagKeys
                    | $dc + {
                        "parameter_mappings": [
                          $params[] as $p
                          | ($p.slug // "") as $raw
                          | (if $raw == "" then empty elif ($raw | endswith("_filter")) then ($raw | sub("_filter$"; "")) else $raw end) as $tn
                          | select(($tagKeys | index($tn)) != null)
                          | ($tags[$tn].type // "") as $ttype
                          | {
                              parameter_id: $p.id,
                              card_id: $dc.card_id,
                              target: (
                                if $ttype == "dimension" then
                                  ["dimension", ["template-tag", $tn]]
                                else
                                  ["variable", ["template-tag", $tn]]
                                end
                              )
                            }
                        ]
                      }
                  end
              )
          )
      else . end
  ')"

  if ! api_request PUT "/api/dashboard/${dashboard_id}" "${repaired}" >/dev/null; then
    log_info "WARN: repair PUT /api/dashboard/${dashboard_id} failed (check Metabase logs)"
  fi
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

delete_collection_tree() {
  local collection_id="$1"
  local children_json child_ids dashboard_ids card_ids

  children_json="$(api_request GET "/api/collection/${collection_id}/items")"

  child_ids="$(echo "${children_json}" | mb_list | jq -r '.[]? | select(.model == "collection") | .id')"
  for child_id in ${child_ids}; do
    delete_collection_tree "${child_id}"
  done

  dashboard_ids="$(echo "${children_json}" | mb_list | jq -r '.[]? | select(.model == "dashboard") | .id')"
  for dashboard_id in ${dashboard_ids}; do
    api_request DELETE "/api/dashboard/${dashboard_id}" >/dev/null
  done

  card_ids="$(echo "${children_json}" | mb_list | jq -r '.[]? | select(.model == "card") | .id')"
  for card_id in ${card_ids}; do
    api_request DELETE "/api/card/${card_id}" >/dev/null
  done

  api_request PUT "/api/collection/${collection_id}" '{"archived":true}' >/dev/null
  api_request DELETE "/api/collection/${collection_id}" >/dev/null
}

delete_demo_content() {
  local collections_raw collections_json dashboards_raw dashboards_json databases_json example_ids dashboard_ids sample_ids

  collections_raw="$(api_request GET "/api/collection")"
  collections_json="$(echo "${collections_raw}" | mb_list)"
  example_ids="$(echo "${collections_json}" | jq -r '.[] | select(.name == "Examples") | .id')"
  for example_id in ${example_ids}; do
    delete_collection_tree "${example_id}"
  done

  dashboards_raw="$(api_request GET "/api/dashboard")"
  dashboards_json="$(echo "${dashboards_raw}" | mb_list)"
  dashboard_ids="$(echo "${dashboards_json}" | jq -r '.[] | select(.name == "E-commerce Insights") | .id')"
  for dashboard_id in ${dashboard_ids}; do
    api_request DELETE "/api/dashboard/${dashboard_id}" >/dev/null
  done

  databases_json="$(api_request GET "/api/database")"
  sample_ids="$(echo "${databases_json}" | jq -r '.data[]? | select(.is_sample == true or .name == "Sample Database") | .id')"
  for sample_id in ${sample_ids}; do
    api_request DELETE "/api/database/${sample_id}" >/dev/null
  done
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

get_database_metadata() {
  if [ -z "${APP_DB_METADATA_JSON:-}" ]; then
    APP_DB_METADATA_JSON="$(api_request GET "/api/database/${APP_DB_ID}/metadata?include_hidden=true")"
  fi

  printf '%s' "${APP_DB_METADATA_JSON}"
}

resolve_table_id() {
  local table_ref="$1"

  if [ -z "${table_ref}" ] || [ "${table_ref}" = "null" ]; then
    return 0
  fi

  local table_id
  table_id="$(
    get_database_metadata | jq -r --arg tableRef "${table_ref}" '
      [
        .tables[]?
        | select(
            .name == $tableRef
            or ((.schema // "public") + "." + .name) == $tableRef
          )
      ]
      | sort_by(.id)
      | last
      | .id // empty
    '
  )"

  if [ -z "${table_id}" ] || [ "${table_id}" = "null" ]; then
    echo "Failed to resolve Metabase table ID for ${table_ref}" >&2
    exit 1
  fi

  printf '%s' "${table_id}"
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
      '{name: $name, description: $description, color: $color, parent_id: ($parentId | tonumber)}') "
  else
    payload="$(jq -n \
      --arg name "${name}" \
      --arg description "${description}" \
      --arg color "${color}" \
      '{name: $name, description: $description, color: $color}')"
  fi

  api_request POST "/api/collection" "${payload}" | jq -r '.id'
}

create_card() {
  local file_json="$1"
  local collection_id="$2"
  
  if ! parsed_json="$(cat "$file_json" | jq -r .)"; then
    echo "Failed to parse $file_json. Exiting." >&2
    exit 1
  fi
  
  local name="$(echo "$parsed_json" | jq -r '.name')"
  local description="$(echo "$parsed_json" | jq -r '.description')"
  local query="$(echo "$parsed_json" | jq -r '.dataset_query.native.query')"
  local display="$(echo "$parsed_json" | jq -r '.display')"
  local meta_file="/tmp/mb_db_metadata.${APP_DB_ID:-0}.json"
  if [ ! -s "$meta_file" ]; then
    # Кэшируем метаданные витрины в файл — иначе передача через --argjson "$meta_json"
    # может упереться в Linux ARG_MAX (~128KB) на крупной витрине.
    get_database_metadata > "$meta_file"
  fi
  local template_tags
  template_tags="$(
    echo "$parsed_json" | jq -c --slurpfile metaArr "$meta_file" '
      # Только явное сопоставление из metabase-field-filters у карточки: имя или display_name поля в метаданных Metabase.
      # Без совпадения provisioning не падает: остаётся тип из JSON (dimension / text); привязку столбца можно задать в UI.
      def resolve_field_id_simple($meta; $tr; $fn):
        [
          $meta.tables[]?
          | select(.name == $tr or ((.schema // "public") + "." + .name) == $tr)
          | .fields[]?
          | select(.name == $fn or .display_name == $fn)
          | .id
        ] | first // null;
      ($metaArr[0]) as $meta
      | . as $card
      | ($card.dataset_query.native["template-tags"] // {}) as $tags
      | ($card["metabase-field-filters"] // {}) as $ff
      | (
          if ($ff | length) == 0 then
            $tags
          else
            reduce ($tags | keys_unsorted[]) as $k ($tags;
              ($tags[$k]) as $tv
              | if (($tv.type // "") == "dimension") and (($ff[$k] // null) != null) then
                  resolve_field_id_simple($meta; $ff[$k].table_ref; $ff[$k].field_name) as $fid
                  | if $fid != null then
                      .[$k] = ($tv + { dimension: ["field", $fid, null] })
                    else
                      .
                    end
                else
                  .
                end
            )
          end
        )
    '
  )"
  local table_ref="$(echo "$parsed_json" | jq -r '.table_ref // empty')"
  local table_id=""
  local visualization_settings
  visualization_settings="$(echo "$parsed_json" | jq -c \
    --arg display "$display" \
    --arg query "$query" '
    (.visualization_settings // {}) as $vs
    | $vs
    | .table = (.table // {})
    | .table.columns = (
        if ($display == "table")
           and ($query | test("v_egisz_transactions_enriched_ui|stg_channel_errors"))
           and ($query | test("v_rpt_documents_no_response_ui") | not) then
          (.table.columns // []) as $cols
          | if ($cols | length) == 0 then
              [ { "name": "Связанное сообщение", "enabled": false } ]
            else
              ($cols | map(if (.name // "") == "Связанное сообщение" then . + { "enabled": false } else . end))
            end
        else
          ((.table.columns // {}) + {
            "[\"name\",\"clinic_id\"]": { "display_as": null },
            "[\"name\",\"service_id\"]": { "display_as": null },
            "[\"name\",\"transaction_id\"]": { "display_as": null }
          })
        end
      )
  ')"

  if [ -n "${table_ref}" ]; then
    table_id="$(resolve_table_id "${table_ref}")"
  fi

  # Большие JSON (особенно visualization_settings у таблиц) могут выбить Linux ARG_MAX, если передавать их через
  # `jq --arg ...` (командная строка). Пишем их во временные файлы и читаем через --rawfile.
  local tmp_tags="/tmp/mb_template_tags.$$.$RANDOM.json"
  local tmp_vs="/tmp/mb_visualization_settings.$$.$RANDOM.json"
  printf '%s' "${template_tags}" > "${tmp_tags}"
  printf '%s' "${visualization_settings}" > "${tmp_vs}"

  local payload
  payload="$(jq -n \
    --slurpfile card "${file_json}" \
    --rawfile templateTags "${tmp_tags}" \
    --rawfile visualizationSettings "${tmp_vs}" \
    --arg collectionId "${collection_id}" \
    --arg databaseId "${APP_DB_ID}" \
    --arg tableId "${table_id}" \
    '($card[0] // {}) as $c
     | {
        name: ($c.name // ""),
        description: ($c.description // ""),
        collection_id: ($collectionId | tonumber),
        dataset_query: {
          type: "native",
          native: {
            query: ($c.dataset_query.native.query // ""),
            "template-tags": ($templateTags | fromjson)
          },
          database: ($databaseId | tonumber)
        },
        table_id: (if ($tableId | length) > 0 then ($tableId | tonumber) else null end),
        display: ($c.display // ""),
        visualization_settings: ($visualizationSettings | fromjson)
      }')"

  rm -f "${tmp_tags}" "${tmp_vs}" >/dev/null 2>&1 || true

  api_request POST "/api/card" "${payload}" | jq -r '.id'
}

create_dashboard() {
  local file_json="$1"
  local collection_id="$2"
  local mb_auto_apply="false"
  case "${MB_AUTO_APPLY_FILTERS}" in
    true|1|yes|on) mb_auto_apply="true" ;;
  esac

  if ! parsed_json="$(cat "$file_json" | jq -r .)"; then
    echo "Failed to parse $file_json. Exiting." >&2
    exit 1
  fi
  
  local name="$(echo "$parsed_json" | jq -r '.name')"
  local description="$(echo "$parsed_json" | jq -r '.description')"
  local parameters_json="$(echo "$parsed_json" | jq -c '.parameters // []')"
  local dash_width="$(echo "$parsed_json" | jq -r '.width // "full"')"

  local dashboard_payload dashboard_id
  dashboard_payload="$(jq -n \
    --arg name "${name}" \
    --arg description "${description}" \
    --arg collectionId "${collection_id}" \
    --arg parameters "${parameters_json}" \
    --arg width "${dash_width}" \
    --argjson autoApply "${mb_auto_apply}" \
    '{
      name: $name,
      description: $description,
      collection_id: ($collectionId | tonumber),
      width: $width,
      cacheables: [],
      parameters: ($parameters | fromjson),
      auto_apply_filters: $autoApply
    }')"

  dashboard_id="$(api_request POST "/api/dashboard" "${dashboard_payload}" | jq -r '.id')"
  if [ -z "${dashboard_id}" ] || [ "${dashboard_id}" = "null" ]; then
    echo "Failed to create dashboard ${name}" >&2
    exit 1
  fi

  # Id параметров фильтра после POST могут отличаться от полей в JSON — привязки к карточкам строим по ответу GET.
  local dash_saved resolved_parameters_json
  dash_saved="$(api_request GET "/api/dashboard/${dashboard_id}")"
  resolved_parameters_json="$(echo "${dash_saved}" | jq -c '.parameters // []')"

  # Metabase v0.47+ attaches dashboard cards via PUT /api/dashboard/:id/cards.
  local cards="[]"
  local num_cards="$(echo "$parsed_json" | jq '.cards | length')"
  if [ "$num_cards" -gt 0 ]; then
    for i in $(seq 0 $((num_cards - 1))); do
      local card_file="/tmp/card_${i}.json"
      echo "$parsed_json" | jq -c ".cards[$i]" > "$card_file"
      local card_id="$(create_card "$card_file" "$collection_id")"
      
      local sizeX="$(echo "$parsed_json" | jq -r ".cards[$i].sizeX // 4")"
      local sizeY="$(echo "$parsed_json" | jq -r ".cards[$i].sizeY // 4")"
      local row="$(echo "$parsed_json" | jq -r ".cards[$i].row // 0")"
      local col="$(echo "$parsed_json" | jq -r ".cards[$i].col // 0")"
      
      local mappings
      mappings="$(echo "$parsed_json" | jq -c \
        --argjson cardIndex "$i" \
        --argjson dashParams "${resolved_parameters_json}" \
        --argjson cardDbId "${card_id}" '
        (.cards[$cardIndex].dataset_query.native["template-tags"] // {}) as $cardTemplateTags
        | ($cardTemplateTags | keys) as $cardTags
        | [
            $dashParams[] as $param
            | (
                if ($param.slug | endswith("_filter")) then
                  ($param.slug | sub("_filter$"; ""))
                else
                  $param.slug
                end
              ) as $tagName
            | if (($cardTags | index($tagName)) != null) then
                ($cardTemplateTags[$tagName].type // "") as $ttype
                | if $ttype == "dimension" then
                    { parameter_id: $param.id, card_id: $cardDbId, target: ["dimension", ["template-tag", $tagName]] }
                  else
                    { parameter_id: $param.id, card_id: $cardDbId, target: ["variable", ["template-tag", $tagName]] }
                  end
              else
                empty
              end
          ]
      ')"

      # Negative IDs mean "new dashboard card" for the bulk update endpoint.
      local dashcard_id=$((-(i + 1)))
      local dashcard="$(jq -n \
        --arg dashcardId "$dashcard_id" \
        --arg cardId "$card_id" \
        --arg sizeX "$sizeX" \
        --arg sizeY "$sizeY" \
        --arg row "$row" \
        --arg col "$col" \
        --arg mappings "$mappings" \
        '{
          id: ($dashcardId | tonumber),
          card_id: ($cardId | tonumber),
          dashboard_tab_id: null,
          action_id: null,
          size_x: ($sizeX | tonumber),
          size_y: ($sizeY | tonumber),
          row: ($row | tonumber),
          col: ($col | tonumber),
          parameter_mappings: ($mappings | fromjson),
          series: [],
          visualization_settings: {}
        }')"

      cards="$(echo "$cards" | jq --arg dc "$dashcard" '. + [($dc | fromjson)]')"
    done
  fi

  if [ "$num_cards" -gt 0 ]; then
    local cards_payload
    cards_payload="$(jq -n --arg cards "${cards}" '{ordered_tabs: [], cards: ($cards | fromjson)}')"
    api_request PUT "/api/dashboard/${dashboard_id}/cards" "${cards_payload}" >/dev/null

    repair_dashboard_parameter_links_if_needed "${dashboard_id}"

    # Полное тело как после GET (уже с parameter_mappings); auto_apply — см. MB_AUTO_APPLY_FILTERS (по умолчанию true).
    local dash_after fix_payload
    dash_after="$(api_request GET "/api/dashboard/${dashboard_id}")"
    fix_payload="$(echo "${dash_after}" | jq --arg w "${dash_width}" --argjson autoApply "${mb_auto_apply}" '.auto_apply_filters = $autoApply | .width = $w')"
    if ! api_request PUT "/api/dashboard/${dashboard_id}" "${fix_payload}" >/dev/null; then
      log_info "WARN: PUT /api/dashboard/${dashboard_id} after dashcards failed (filters may need Apply in UI)"
    fi
    # PUT целого дашборда иногда сбрасывает связи фильтров — повторная проверка/ремонт.
    repair_dashboard_parameter_links_if_needed "${dashboard_id}"
  fi

  printf '%s' "${dashboard_id}"
}

log_info "Waiting for Metabase at ${METABASE_URL}..."
until curl --silent --fail "${METABASE_URL}/api/health" >/dev/null; do
  sleep 3
done

log_info "METABASE_AUTO_APPLY_FILTERS=${MB_AUTO_APPLY_FILTERS} (true: текущие значения фильтров применяются при открытии; false — только после «Применить» в UI)."

authenticate
delete_demo_content

collections_for_delete="$(api_request GET "/api/collection" | mb_list)"
for collection_id in $(echo "${collections_for_delete}" | jq -r '.[] | select(.name | test("^EGISZ")) | .id' | sort -nr); do
  delete_collection_tree "${collection_id}"
done

authenticate
APP_DB_ID="$(ensure_app_database)"

# После смены DDL в Postgres Metabase не подхватывает поля до sync (раньше sync вызывался только при POST /api/database).
log_info "Metabase: sync_schema for application database id=${APP_DB_ID}…"
api_request POST "/api/database/${APP_DB_ID}/sync_schema" "{}" >/dev/null || true

# Готовность витрины: таблица есть и поля просканированы (не полагаемся на кириллицу в jq внутри контейнера).
log_info "Waiting for DWH view field metadata (v_egisz_transactions_enriched_ui)…"
for _i in $(seq 1 90); do
  APP_DB_METADATA_JSON="" # сбрасываем кэш get_database_metadata; иначе застреваем на первом (неполном) снимке
  _nf="$(get_database_metadata | jq '
    ([.tables[]? | select(.name == "v_egisz_transactions_enriched_ui")] | first | .fields // []) | length
  ')"
  if [ "${_nf:-0}" -ge 15 ] 2>/dev/null; then
    log_info "DWH view fields in metadata (count=${_nf}) — OK"
    break
  fi
  if [ "$_i" -eq 90 ]; then
    echo "[dashboards] ERROR: v_egisz_transactions_enriched_ui: недостаточно полей в metadata за 180s (получено ${_nf:-0}, нужно ≥15). Примените sql/001_schema.sql к egisz_reports; в Metabase: Admin → Databases → Sync database schema." >&2
    exit 1
  fi
  sleep 2
done
APP_DB_METADATA_JSON=""

# Дашборд 06_semd_archive.json привязывает field filter к «Дата обработки» в v_rpt_semd_archive_ui.
# Раньше архив был последним в 05 — без явного ожидания create_card падал на resolve_field_id.
# Сейчас 06 идёт после 05 в *.json: метаданные view должны быть готовы до импорта архива.
log_info "Waiting for DWH view field metadata (v_rpt_semd_archive_ui)…"
for _i in $(seq 1 90); do
  APP_DB_METADATA_JSON=""
  _narch="$(get_database_metadata | jq '
    ([.tables[]? | select(.name == "v_rpt_semd_archive_ui")] | first | .fields // []) | length
  ')"
  if [ "${_narch:-0}" -ge 12 ] 2>/dev/null; then
    log_info "DWH archive view fields in metadata (count=${_narch}) — OK"
    break
  fi
  if [ "$_i" -eq 90 ]; then
    echo "[dashboards] ERROR: v_rpt_semd_archive_ui: недостаточно полей в metadata за 180s (получено ${_narch:-0}, нужно ≥12). Примените sql/001_schema.sql к egisz_reports; в Metabase: Admin → Databases → Sync database schema." >&2
    exit 1
  fi
  sleep 2
done
APP_DB_METADATA_JSON=""

# Карточки с field filter по v_rpt_network_errors_detail_ui («Дата создания документа») — дождаться полей view.
log_info "Waiting for DWH view field metadata (v_rpt_network_errors_detail_ui)…"
for _i in $(seq 1 90); do
  APP_DB_METADATA_JSON=""
  _nnet="$(get_database_metadata | jq '
    ([.tables[]? | select(.name == "v_rpt_network_errors_detail_ui")] | first | .fields // []) | length
  ')"
  if [ "${_nnet:-0}" -ge 12 ] 2>/dev/null; then
    log_info "DWH network errors view fields in metadata (count=${_nnet}) — OK"
    break
  fi
  if [ "$_i" -eq 90 ]; then
    echo "[dashboards] ERROR: v_rpt_network_errors_detail_ui: недостаточно полей в metadata за 180s (получено ${_nnet:-0}, нужно ≥12). Примените sql/001_schema.sql; в Metabase: Sync database schema." >&2
    exit 1
  fi
  sleep 2
done
APP_DB_METADATA_JSON=""

# Дашборды в корне личной коллекции (тот же URL, что «Персональная коллекция …»), иначе вложенная папка не видна на главном экране коллекции.
PERSONAL_ID="$(api_request GET "/api/user/current" | jq -r '.personal_collection_id // empty')"
if [ -n "${PERSONAL_ID}" ] && [ "${PERSONAL_ID}" != "null" ]; then
  log_info "Provisioning into admin personal_collection_id=${PERSONAL_ID} (root of personal collection in UI)"
  ROOT_COLLECTION_ID="${PERSONAL_ID}"
else
  log_info "WARN: personal_collection_id missing from /api/user/current; creating ${ROOT_COLLECTION_NAME} at default root"
  ROOT_COLLECTION_ID="$(create_collection "${ROOT_COLLECTION_NAME}" "EGISZ dashboards collection" "#509EE3")"
fi

# Перед импортом JSON — пустая целевая коллекция (дашборды, карточки, вложенные коллекции).
wipe_corp_root_collection() {
  local coll="${1:-}"
  if [ -z "${coll}" ] || [ "${coll}" = "null" ]; then
    return 0
  fi
  local _pass items list id
  for _pass in 1 2 3; do
    items="$(api_request GET "/api/collection/${coll}/items")"
    list="$(echo "${items}" | mb_list)"
    # Вложенные коллекции удаляем целиком (дерево), иначе в UI остаются лишние сущности.
    while IFS= read -r subcoll; do
      [ -z "${subcoll}" ] && continue
      log_info "Removing nested collection id=${subcoll} (full tree)"
      delete_collection_tree "${subcoll}"
    done < <(echo "${list}" | jq -r '.[] | select(.model == "collection") | .id')
    while IFS= read -r id; do
      [ -z "${id}" ] && continue
      log_info "Removing prior dashboard id=${id}"
      api_request DELETE "/api/dashboard/${id}" >/dev/null || true
    done < <(echo "${list}" | jq -r '.[] | select(.model == "dashboard") | .id')
    items="$(api_request GET "/api/collection/${coll}/items")"
    list="$(echo "${items}" | mb_list)"
    while IFS= read -r id; do
      [ -z "${id}" ] && continue
      log_info "Removing prior saved question (card) id=${id}"
      api_request DELETE "/api/card/${id}" >/dev/null || true
    done < <(echo "${list}" | jq -r '.[] | select(.model == "card") | .id')
  done
  log_info "Root collection ${coll} cleared; importing from ${DASHBOARDS_DIR}"
}

wipe_corp_root_collection "${ROOT_COLLECTION_ID}"

_any_dash_fail=0
for dashboard_file in "${DASHBOARDS_DIR}"/*.json; do
  if [ -f "$dashboard_file" ]; then
    log_info "Provisioning $dashboard_file..."
    # Подоболочка: внутри create_dashboard/create_card при ошибке — exit 1; иначе оборвётся весь скрипт и 02–06 не импортируются.
    if ! ( create_dashboard "$dashboard_file" "$ROOT_COLLECTION_ID" >/dev/null ); then
      log_info "ERROR: provisioning failed for ${dashboard_file} (see Metabase API errors above). Continuing with other JSON files."
      _any_dash_fail=1
    fi
  fi
done
if [ "${_any_dash_fail}" -ne 0 ]; then
  echo "[dashboards] One or more dashboard JSON files failed to import. Fix errors and re-run provisioning (e.g. METABASE_FORCE_PROVISION=true or restart Metabase pod)." >&2
  exit 1
fi

log_info "Database: ${APP_DB_ID}"
log_info "Root collection: ${ROOT_COLLECTION_ID}"
