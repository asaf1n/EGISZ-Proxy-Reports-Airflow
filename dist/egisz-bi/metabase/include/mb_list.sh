# shellcheck shell=bash
mb_list() {
  jq -c '
    if type == "array" then .
    elif (.data | type == "array") then .data
    elif (.items | type == "array") then .items
    elif (.data | type == "object") and (.data.items | type == "array") then .data.items
    else [] end
  '
}

mb_all_dashboards_json() {
  local base="${1%/}"
  # Полная строка заголовка авторизации: сессионный токен или ключ API — выбирает вызывающий.
  local auth_header="$2"
  local limit=200
  local offset=0
  local combined='[]'
  while true; do
    local page arr n
    page="$(curl -sS --retry 5 --retry-delay 3 --retry-connrefused --connect-timeout 20 \
      "${base}/api/dashboard?limit=${limit}&offset=${offset}" -H "${auth_header}" || echo '{}')"
    arr="$(echo "${page}" | mb_list)"
    n="$(echo "${arr}" | jq 'length')"
    [ "${n:-0}" -eq 0 ] && break
    combined="$(jq -n --argjson a "${combined}" --argjson b "${arr}" '$a + $b')"
    [ "${n}" -lt "${limit}" ] && break
    offset=$((offset + limit))
    [ "${offset}" -gt 20000 ] && break
  done
  printf '%s\n' "${combined}"
}
