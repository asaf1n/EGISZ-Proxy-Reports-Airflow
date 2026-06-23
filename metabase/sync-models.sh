#!/bin/bash
# Sync Metabase Models from metabase_models/*.json into the collection.
# Writes MODEL_REGISTRY_FILE consumed by dashboard_payload (QB card resolution).
set -euo pipefail

MODELS_DIR="${METABASE_MODELS_DIR:-/app/metabase_models}"
MODEL_REGISTRY_FILE="${METABASE_MODEL_REGISTRY_FILE:-/tmp/metabase-model-registry.json}"

table_id_for_ref() {
  local table_ref="$1"
  local table_name="${table_ref#public.}"
  jq -r --arg table "${table_name}" '
    [.tables[]?
      | select((.schema // "public") == "public" and .name == $table)
      | .id][0] // empty
  ' "${DB_METADATA_FILE}"
}

field_id_for_ref() {
  local table_ref="$1" field_name="$2"
  local table_name="${table_ref#public.}"
  jq -r --arg table "${table_name}" --arg field "${field_name}" '
    [.tables[]?
      | select((.schema // "public") == "public" and .name == $table)
      | .fields[]?
      | select(.name == $field or .display_name == $field)
      | .id][0] // empty
  ' "${DB_METADATA_FILE}"
}

refresh_db_metadata() {
  api_request POST "/api/database/${APP_DB_ID}/sync_schema" "{}" >/dev/null
  DB_METADATA_FILE="${DB_METADATA_FILE:-/tmp/metabase-db-metadata.json}"
  api_request GET "/api/database/${APP_DB_ID}/metadata" > "${DB_METADATA_FILE}"
}

dwh_column_exists() {
  local view_name="$1" column_name="$2"
  PGPASSWORD="${APP_DB_PASSWORD}" psql \
    -h "${APP_DB_HOST}" \
    -p "${APP_DB_PORT}" \
    -U "${APP_DB_USER}" \
    -d "${APP_DB_NAME}" \
    -AtX \
    -v ON_ERROR_STOP=1 \
    -c "SELECT CASE WHEN EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = '${view_name}'
            AND column_name = '${column_name}'
        ) THEN 'ok' ELSE 'missing' END;"
}

validate_model_fields_in_dwh() {
  local model_file table_ref field_name view_name status missing=()
  for model_file in "${MODELS_DIR}"/*.json; do
    [ -f "${model_file}" ] || continue
    table_ref="$(jq -r '.table_ref' "${model_file}")"
    view_name="${table_ref#public.}"
    while IFS= read -r field_name; do
      [ -n "${field_name}" ] || continue
      status="$(dwh_column_exists "${view_name}" "${field_name}")"
      if [ "${status}" != "ok" ]; then
        missing+=("${table_ref}.${field_name}")
      fi
    done < <(jq -r '
      (.fields // {} | keys[])
      , (.hidden_fields[]? // empty)
    ' "${model_file}")
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    printf '%s\n' "${missing[@]}" >&2
    fail "DWH is missing ${#missing[@]} column(s) required by Metabase models; run db/dwh_init.sql"
  fi
}

existing_model_id() {
  local model_name="$1"
  api_request GET "/api/card" |
    jq -r --arg name "${model_name}" --argjson col "${COL_ID}" '
      [.[]?
        | select(.collection_id == $col and .name == $name and .type == "model" and (.archived | not))
        | .id]
      | if length > 0 then .[-1] else empty end
    '
}

apply_field_metadata() {
  local table_ref="$1" field_name="$2" semantic_type="$3" visibility="${4:-normal}"
  local field_id attempt
  for attempt in 1 2 3 4 5; do
    field_id="$(field_id_for_ref "${table_ref}" "${field_name}")"
    if [ -n "${field_id}" ]; then
      api_request PUT "/api/field/${field_id}" "$(jq -nc \
        --arg sem "${semantic_type}" \
        --arg vis "${visibility}" \
        '{semantic_type: $sem, visibility_type: $vis}')" >/dev/null
      return 0
    fi
    if [ "${attempt}" -lt 5 ]; then
      log_info "Field ${table_ref}.${field_name} not in Metabase metadata; resyncing schema (attempt ${attempt}/5)"
      refresh_db_metadata
      sleep 3
    fi
  done
  fail "field not found for model metadata: ${table_ref}.${field_name} (run db/dwh_init.sql and retry)"
}

create_or_update_model() {
  local model_file="$1"
  local model_name table_ref description table_id model_id payload
  model_name="$(jq -r '.name' "${model_file}")"
  table_ref="$(jq -r '.table_ref' "${model_file}")"
  description="$(jq -r '.description // ""' "${model_file}")"
  table_id="$(table_id_for_ref "${table_ref}")"
  [ -n "${table_id}" ] || fail "Metabase table id not found for ${table_ref}"

  payload="$(jq -nc \
    --argjson db "${APP_DB_ID}" \
    --argjson col "${COL_ID}" \
    --argjson table "${table_id}" \
    --arg name "${model_name}" \
    --arg desc "${description}" \
    '{
      name: $name,
      description: $desc,
      type: "model",
      display: "table",
      visualization_settings: {},
      collection_id: $col,
      dataset_query: {
        database: $db,
        type: "query",
        query: {
          "source-table": $table
        }
      }
    }')"

  model_id="$(existing_model_id "${model_name}")"
  if [ -n "${model_id}" ]; then
    api_request PUT "/api/card/${model_id}" "${payload}" >/dev/null
  else
    model_id="$(api_request POST "/api/card" "${payload}" | jq -r '.id')"
  fi
  [ -n "${model_id}" ] && [ "${model_id}" != "null" ] || fail "cannot create model ${model_name}"

  while IFS=$'\t' read -r field_name semantic_type; do
    [ -n "${field_name}" ] || continue
    apply_field_metadata "${table_ref}" "${field_name}" "${semantic_type}" "normal"
  done < <(jq -r '.fields | to_entries[] | [.key, .value.semantic_type] | @tsv' "${model_file}")

  while IFS= read -r field_name; do
    [ -n "${field_name}" ] || continue
    apply_field_metadata "${table_ref}" "${field_name}" "type/Category" "details-only"
  done < <(jq -r '.hidden_fields[]? // empty' "${model_file}")

  printf '%s\n' "${model_id}"
}

build_model_registry() {
  local model_file model_name table_ref model_id
  local registry='{}'
  for model_file in "${MODELS_DIR}"/*.json; do
    [ -f "${model_file}" ] || continue
    model_name="$(jq -r '.name' "${model_file}")"
    table_ref="$(jq -r '.table_ref' "${model_file}")"
    model_id="$(existing_model_id "${model_name}")"
    [ -n "${model_id}" ] || fail "model missing after sync: ${model_name}"
    registry="$(jq -nc \
      --argjson reg "${registry}" \
      --arg name "${model_name}" \
      --arg table_ref "${table_ref}" \
      --argjson model_id "${model_id}" \
      --slurpfile mf "${model_file}" \
      --slurpfile meta "${DB_METADATA_FILE}" \
      '
      def field_map($table_ref):
        ($table_ref | sub("^public\\."; "")) as $table_name
        | reduce ($mf[0].fields // {} | keys[]) as $fname ({};
            . + {
              $fname: (
                [
                  $meta[0].tables[]?
                  | select((.schema // "public") == "public" and .name == $table_name)
                  | .fields[]?
                  | select(.name == $fname or .display_name == $fname)
                  | .id
                ][0]
              )
            }
          );
      $reg + {
        ($name): {
          model_id: $model_id,
          table_ref: $table_ref,
          fields: field_map($table_ref)
        }
      }
      ')"
  done
  printf '%s' "${registry}" > "${MODEL_REGISTRY_FILE}"
}

sync_all_models() {
  [ -d "${MODELS_DIR}" ] || {
    log_info "No models directory ${MODELS_DIR}; skipping model sync"
    printf '{}' > "${MODEL_REGISTRY_FILE}"
    return 0
  }
  local model_file
  log_info "Syncing Metabase models from ${MODELS_DIR}"
  for model_file in "${MODELS_DIR}"/*.json; do
    [ -f "${model_file}" ] || continue
    create_or_update_model "${model_file}" >/dev/null
    log_info "Model synced: $(jq -r '.name' "${model_file}")"
  done
  build_model_registry
}
