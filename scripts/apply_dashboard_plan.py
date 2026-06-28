#!/usr/bin/env python3
"""Apply Metabase dashboard plan: fixes, renames, QB archive, drill-through."""
from __future__ import annotations

import json
import re
import sys
from copy import deepcopy
from pathlib import Path

# Запускается под PowerShell (cp1251-консоль); печатаем в UTF-8 во избежание падений
# на символах вне cp1251 в именах карточек.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:  # pragma: no cover
    pass

ROOT = Path(__file__).resolve().parents[1]
DASH_01 = ROOT / "metabase_dashboards" / "01_integration_egisz.json"

# Единая палитра по категориям ошибок (~10 групп + «Прочие»). Каждый тип наследует
# цвет своей категории → сунберст и стэк-бар «парных» карточек согласованы по цвету.
CATEGORY_COLORS: dict[str, str] = {
    "Данные пациента": "#4E79A7",
    "Данные медработника": "#59A14F",
    "Ошибки структуры и валидации": "#B07AA1",
    "Ошибки справочника НСИ": "#EDC948",
    "Ошибки регистрации в РЭМД": "#E15759",
    "Ошибки организации / ИС": "#76B7B2",
    "Ошибки получения файла ЭМД": "#FF9DA7",
    "Ошибки ЭП и сертификатов": "#F28E2B",
    "Технические ошибки РЭМД": "#9C755F",
    "Ошибки связи": "#499894",
    "Прочие": "#BAB0AC",
}


def error_type_color_map() -> dict[str, str]:
    """Map each canonical error type → its category color, derived from the seed
    (db/parts/30_error_rules.sql: dim_error_rules + dim_error_type_group) so the
    palette stays a single source of truth and never drifts from the dictionary."""
    sql = (ROOT / "db" / "parts" / "30_error_rules.sql").read_text(encoding="utf-8")
    cats = "|".join(re.escape(c) for c in CATEGORY_COLORS)
    # ...'<type>', '<category>')  — последние две строки в строке VALUES (правило/спец-тип).
    pairs = re.findall(r"'((?:[^']|'')+)',\s*'(" + cats + r")'\s*\)", sql)
    colors = {"Категория ошибки": "#BAB0AC"}
    colors.update(CATEGORY_COLORS)  # сами категории (внутреннее кольцо сунберста)
    for label, cat in pairs:
        colors[label.replace("''", "'")] = CATEGORY_COLORS[cat]
    return colors


def write_json_if_changed(path: Path, data: dict) -> bool:
    text = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return False
    path.write_text(text, encoding="utf-8")
    return True

PARAM_IDS = {
    "jid_filter": "e3c4d5e6-f7a8-4901-c234-56789abcdef0",
    "semd_type_filter": "d2b3c4d5-e6f7-4890-b123-456789abcdef",
    "status_filter": "e3c4d5e6-f7a8-4901-c234-56789abcdef4",
    "error_type_filter": "f1a2b3c4-d5e6-4789-a01b-0123456789c0",
}

MOVE_TO_ERRORS = {
    "Ошибки по типу",
    "Ошибок по СЭМД",
    "Ошибки по клиникам: объём и %",
}

ARCHIVE_FROM_OPERATIONAL = frozenset({"Динамика документов по дням"})

DEFAULT_DWH_PERIOD = "thismonth"

RENAME_01 = {
    "Ошибки по типу": "Топ по типу ошибки",
    "Ошибок по СЭМД": "Топ типов СЭМД по ошибкам",
    "Топ по типу СЭМД": "Топ типов СЭМД по документам",
    "Ошибки по клиникам: объём и %": "Объём ошибок по клиникам",
    "Виды ошибок по категориям": "Топ категорий и типов ошибки",
    "Виды ошибок по типам СЭМД": "Топ типов СЭМД по видам ошибки",
    "Документы по дням": "Динамика документов по дням",
    "Топ клиник в очереди": "Топ клиник в очереди по документам",
    "Очередь по типам СЭМД": "Топ типов СЭМД в очереди",
}

RENAME_OTHER = {
    "05_executive.json": {"Всего документов за период": "Документов за период"},
    "07_client_service.json": {
        "Документов за период": "Документов за период — клиент",
        "Топ типов СЭМД по документам": "Топ типов СЭМД — клиент",
        "Всего распознанных документов": "Документов за период — клиент",
        "Структура документооборота по типам СЭМД": "Топ типов СЭМД — клиент",
        "Топ-10 типов СЭМД": "Топ-10 типов СЭМД по документам",
    },
    "08_client_bianalytic.json": {
        "Документов за период": "Документов за период — BI",
        "СЭМД за период": "Документов за период — BI",
        "Динамика по типам СЭМД (месяцы)": "Динамика документов по типам СЭМД",
        "Топ-10 врачей по объёму СЭМД": "Топ врачей по документам",
    },
}

DRILL_BY_NAME: dict[str, list[tuple[str, str]]] = {
    "Последние операции": [("jid_filter", "JID Клиники"), ("semd_type_filter", "Код СЭМД"), ("status_filter", "Статус")],
    "Статусы за период": [("status_filter", "Статус")],
    "Объём по клиникам": [("jid_filter", "JID Клиники")],
    "Транзакции по дням и статусам": [("status_filter", "Статус")],
    "РЭМД vs связь": [("status_filter", "Статус")],
    "Топ по типу ошибки": [],
    "Топ типов СЭМД по ошибкам": [("semd_type_filter", "СЭМД")],
    "Объём ошибок по клиникам": [("jid_filter", "JID Клиники")],
    "Топ категорий и типов ошибки": [],
    "Топ типов СЭМД по видам ошибки": [("semd_type_filter", "СЭМД")],
    "Успешность по клиникам": [("jid_filter", "JID Клиники")],
    "Успешность по типам СЭМД": [("semd_type_filter", "Код СЭМД")],
    "Топ клиник в очереди по документам": [("jid_filter", "JID Клиники")],
    "Топ типов СЭМД в очереди": [("semd_type_filter", "Код СЭМД")],
    "Объём по СЭМД": [("semd_type_filter", "Код СЭМД")],
}

DOCUMENTS_MODEL_REF = "Документы"
ERROR_BREAKDOWN_MODEL_REF = "Разбивка ошибок"

ModelDrillMapping = tuple[str, str] | tuple[str, str, str]

# Дрилл из строки ведёт в модель «Документы»: тип ошибки — через CONTAINS по полному
# списку error_types (документ с несколькими ошибками не теряется), клиника — точным
# равенством. Общие фильтры дашборда (период/СЭМД/статус) переносятся через
# metabase-model-drill-params ниже.
MODEL_DRILL_BY_NAME: dict[str, list[ModelDrillMapping]] = {
    "Ошибки: тип × клиника": [
        ("error_types", "Тип ошибки", "contains"),
        ("clinic_jid", "JID Клиники"),
    ],
}

# Целевая модель дрилла по карточке (по умолчанию — «Документы»).
MODEL_DRILL_TARGET_BY_NAME: dict[str, str] = {}

MODEL_DRILL_DASHBOARD_PARAMS: dict[str, list[str]] = {
    "Ошибки: тип × клиника": ["dwh_date", "semd_type", "jid", "status"],
}

DOCUMENTS_PARAM_TARGETS = {
    "dwh_date": {"model_ref": "Документы", "field_name": "processed_at"},
    "jid": {"model_ref": "Документы", "field_name": "clinic_jid"},
    "semd_type": {"model_ref": "Документы", "field_name": "semd_code"},
    "status": {"model_ref": "Документы", "field_name": "status_label"},
    "local_uid": {"model_ref": "Документы", "field_name": "semd_local_uid"},
    "relates_to": {"model_ref": "Документы", "field_name": "relates_to_id"},
    "emdr_id": {"model_ref": "Документы", "field_name": "semd_emdr_id"},
    "log_id": {"model_ref": "Документы", "field_name": "logid"},
}

ARCHIVE_TABLE_COLUMNS = [
    {"enabled": True, "name": "Дата обработки"},
    {"enabled": True, "name": "Статус"},
    {"enabled": True, "name": "СЭМД"},
    {"enabled": True, "name": "Клиника"},
    {"enabled": False, "name": "JID Клиники"},
    {"enabled": False, "name": "Наименование клиники"},
    {"enabled": True, "name": "Host Клиники (ГОСТ VPN)"},
    {"enabled": True, "name": "localUid СЭМД"},
    {"enabled": True, "name": "Сводка ошибки"},
    {"enabled": True, "name": "Рег. Номер РЭМД"},
    {"enabled": True, "name": "Связанное сообщение"},
    {"enabled": True, "name": "LOGID"},
    {"enabled": False, "name": "dwh_id"},
    {"enabled": False, "name": "OID Клиники"},
    {"enabled": False, "name": "СЭМД CreateDate"},
    {"enabled": False, "name": "MSGID"},
    {"enabled": False, "name": "День"},
]

LATEST_OPERATIONS_TABLE_COLUMNS = [
    {"enabled": True, "name": "Дата обработки"},
    {"enabled": True, "name": "Статус"},
    {"enabled": True, "name": "Клиника"},
    {"enabled": True, "name": "Host Клиники (ГОСТ VPN)"},
    {"enabled": True, "name": "СЭМД"},
    {"enabled": True, "name": "localUid СЭМД"},
    {"enabled": True, "name": "Рег. Номер РЭМД"},
    {"enabled": True, "name": "Сводка ошибки"},
]

LATEST_OPERATIONS_QUERY_FIELDS = [
    ["field", "Документы:processed_at", None],
    ["field", "Документы:status_label", None],
    ["field", "Документы:clinic_label", None],
    ["field", "Документы:clinic_host", None],
    ["field", "Документы:semd_label", None],
    ["field", "Документы:semd_local_uid", None],
    ["field", "Документы:semd_emdr_id", None],
    ["field", "Документы:error_summary", None],
]

LATEST_OPERATIONS_COLUMN_SETTINGS = {
    '["name","Дата обработки"]': {
        "column_title": "Обработано IPS",
        "date_abbreviate": True,
        "date_style": "D MMMM, YYYY",
        "time_style": "HH:mm",
    },
    '["name","СЭМД"]': {"column_title": "СЭМД", "text_style": "wrap"},
    '["name","Клиника"]': {"column_title": "Клиника"},
    '["name","localUid СЭМД"]': {"column_title": "localUid"},
    '["name","Сводка ошибки"]': {"column_title": "Сводка ошибки", "text_style": "wrap"},
    '["name","Host Клиники (ГОСТ VPN)"]': {"column_title": "Host"},
}

DOCUMENT_FILTERS = (
    "[[AND {{dwh_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] "
    "[[AND {{local_uid}}]] [[AND {{relates_to}}]] [[AND {{emdr_id}}]] "
    "[[AND {{status}}]] [[AND {{log_id}}]]"
)

DOCUMENT_VOLUME_BY_DAY_QUERY = (
    "SELECT arrival_day AS \"Дата\", "
    "COUNT(DISTINCT dwh_id)::bigint AS \"Документов\" "
    "FROM public.rpt_documents "
    "WHERE arrival_day IS NOT NULL "
    f"{DOCUMENT_FILTERS} GROUP BY arrival_day ORDER BY arrival_day ASC"
)

TRANSACTIONS_BY_DAY_STATUS_QUERY = (
    "SELECT processed_day AS \"Дата\", status_label AS \"Статус\", "
    "COUNT(DISTINCT dwh_id)::bigint AS \"Документов\" "
    "FROM public.rpt_documents WHERE 1=1 "
    "[[AND {{dwh_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] "
    "GROUP BY processed_day, status_label, status_sort "
    "ORDER BY processed_day, status_sort"
)

CLIENT_STATUS_BY_DAY_QUERY = (
    "SELECT processed_day AS \"Дата\", status_label AS \"Статус\", "
    "COUNT(DISTINCT dwh_id)::bigint AS \"Документов\" "
    "FROM public.rpt_documents WHERE clinic_jid = {{clinic_jid}} "
    "[[AND {{client_period}}]] [[AND {{client_semd_code_name}}]] "
    "GROUP BY processed_day, status_label, status_sort ORDER BY processed_day, status_sort"
)

CLINIC_VOLUME_QUERY = (
    "WITH filtered AS ( SELECT clinic_jid, clinic_label, dwh_id "
    "FROM public.rpt_documents WHERE 1=1 "
    f"{DOCUMENT_FILTERS} ), "
    "totals AS ( SELECT COUNT(DISTINCT dwh_id)::numeric AS total FROM filtered ), "
    "per_clinic AS ( SELECT clinic_jid::text AS \"JID Клиники\", "
    "clinic_label AS \"Клиника\", COUNT(DISTINCT dwh_id)::bigint AS cnt "
    "FROM filtered GROUP BY clinic_jid, clinic_label ) "
    "SELECT \"JID Клиники\", \"Клиника\", cnt AS \"Документов\", "
    "ROUND(100.0 * cnt / NULLIF((SELECT total FROM totals), 0), 1) AS \"%\" "
    "FROM per_clinic ORDER BY cnt DESC"
)

CLINIC_VOLUME_TABLE_COLUMNS = [
    {"enabled": False, "name": "JID Клиники"},
    {"enabled": True, "name": "Клиника"},
    {"enabled": True, "name": "Документов"},
    {"enabled": True, "name": "%"},
]

CLINIC_ERROR_VOLUME_TOP_N = 8

CLINIC_ERROR_VOLUME_QUERY = (
    "WITH filtered AS ( SELECT clinic_jid, clinic_label, clinic_name, dwh_id, status "
    "FROM public.rpt_documents "
    "WHERE status IN ('success','async_error','network_error') "
    "AND NULLIF(TRIM(clinic_jid::text), '') IS NOT NULL "
    f"{DOCUMENT_FILTERS} ), "
    "per_clinic AS ( SELECT clinic_jid::text AS jid, "
    "COALESCE(NULLIF(BTRIM(clinic_name), ''), 'JID ' || clinic_jid::text) AS lbl, "
    "COUNT(DISTINCT dwh_id)::bigint AS total, "
    "COUNT(DISTINCT dwh_id) FILTER (WHERE status IN ('async_error','network_error'))::bigint AS errs "
    "FROM filtered GROUP BY clinic_jid, clinic_label, clinic_name ), "
    "ranked AS ( SELECT jid, lbl, errs, total, "
    "ROW_NUMBER() OVER (ORDER BY errs DESC, total DESC) AS rn "
    "FROM per_clinic WHERE errs > 0 ), "
    f"bounds AS ( SELECT {CLINIC_ERROR_VOLUME_TOP_N} AS top_n ), "
    "top AS ( SELECT r.jid, r.lbl, r.errs, r.total FROM ranked r, bounds b "
    "WHERE r.rn <= b.top_n ), "
    "tail AS ( SELECT COUNT(r.jid)::int AS cnt, COALESCE(SUM(r.errs), 0)::bigint AS errs, "
    "COALESCE(SUM(r.total), 0)::numeric AS total FROM ranked r, bounds b "
    "WHERE r.rn > b.top_n ) "
    "SELECT * FROM ( "
    'SELECT jid AS "JID Клиники", lbl AS "Клиника", errs AS "Документов", '
    'ROUND(100.0 * errs / NULLIF(total, 0), 1) AS "% ошибок" FROM top '
    "UNION ALL "
    'SELECT NULL::text, '
    "'Прочие (' || t.cnt || ')', t.errs, "
    'ROUND(100.0 * t.errs / NULLIF(t.total, 0), 1) FROM tail t WHERE t.cnt > 0 '
    ') u ORDER BY "Документов" DESC'
)

# «Паттерн ошибки» = match_code + match_pattern правила для канонического типа (interpretation).
# При нескольких правилах на один тип берётся паттерн с наименьшим priority.
ERROR_TYPE_CLINIC_QUERY = (
    "WITH period_docs AS ( SELECT dwh_id, clinic_jid::text AS clinic_jid "
    "FROM public.rpt_documents "
    "WHERE status IN ('success', 'async_error', 'network_error') "
    "AND NULLIF(TRIM(clinic_jid::text), '') IS NOT NULL "
    "[[AND {{dwh_date}}]] [[AND {{jid}}]] [[AND {{semd_type}}]] ), "
    "base AS ( SELECT "
    "COALESCE(NULLIF(TRIM(e.error_type), ''), 'Неизвестная ошибка') AS error_type, "
    "e.clinic_label AS clinic_label, e.clinic_jid::text AS clinic_jid, e.dwh_id "
    "FROM public.rpt_error_breakdown e "
    "INNER JOIN period_docs pd ON pd.dwh_id = e.dwh_id "
    "WHERE COALESCE(NULLIF(TRIM(e.error_type), ''), '') <> '' "
    "[[AND {{error_type}}]] ), "
    "error_clinic AS ( SELECT error_type, clinic_label, clinic_jid, "
    "COUNT(DISTINCT dwh_id)::bigint AS doc_count "
    "FROM base GROUP BY 1, 2, 3 ), "
    "clinic_totals AS ( SELECT clinic_jid, COUNT(DISTINCT dwh_id)::numeric AS total_docs "
    "FROM period_docs GROUP BY clinic_jid ), "
    "rule_patterns AS ( SELECT interpretation AS error_type, "
    "(array_agg(COALESCE(match_code, '') || match_pattern ORDER BY priority))[1] AS error_pattern "
    "FROM public.dim_error_rules WHERE is_active GROUP BY interpretation ) "
    'SELECT ec.error_type AS "Тип ошибки", ec.clinic_label AS "Клиника", '
    'COALESCE(rp.error_pattern, ec.error_type) AS "Паттерн ошибки", '
    'ec.clinic_jid AS "JID Клиники", ec.doc_count AS "Документов", '
    'ROUND(100.0 * ec.doc_count / NULLIF(ct.total_docs, 0), 1) AS "% ошибок" '
    "FROM error_clinic ec "
    "JOIN clinic_totals ct ON ct.clinic_jid = ec.clinic_jid "
    "LEFT JOIN rule_patterns rp ON rp.error_type = ec.error_type "
    "ORDER BY ec.doc_count DESC"
)

HEATMAP_QUERY = (
    "WITH d AS ( "
    "SELECT date_trunc('day', processed_at)::date AS day, "
    "COALESCE(NULLIF(BTRIM(clinic_label), ''), 'JID ' || clinic_jid::text) AS clinic, "
    "COUNT(DISTINCT dwh_id) FILTER (WHERE status IN ('success', 'async_error', 'network_error')) AS cnt, "
    "COUNT(DISTINCT dwh_id) FILTER (WHERE status IN ('async_error', 'network_error')) AS err "
    "FROM public.rpt_documents "
    "WHERE NULLIF(TRIM(clinic_jid::text), '') IS NOT NULL "
    "[[AND {{dwh_date}}]] [[AND {{jid}}]] [[AND {{semd_type}}]] "
    "GROUP BY 1, 2 ) "
    'SELECT day AS "День", clinic AS "Клиника", '
    'ROUND(100.0 * err / NULLIF(cnt, 0), 1) AS "Доля ошибок, %" '
    "FROM d ORDER BY 1, 2"
)

HEATMAP_VIZ = {
    "table.pivot": True,
    "table.pivot_column": "День",
    "table.pivot_row": "Клиника",
    "table.cell_column": "Доля ошибок, %",
    "table.column_formatting": [
        {
            "colors": ["#10B981", "#F59E0B", "#EF4444"],
            "columns": ["Доля ошибок, %"],
            "max_type": "custom",
            "max_value": 25,
            "min_type": "custom",
            "min_value": 0,
            "type": "range",
        }
    ],
    "column_settings": {
        '["name","Доля ошибок, %"]': {
            "decimals": 1,
            "number_separators": ", ",
            "suffix": " %",
        },
        '["name","Клиника"]': {"column_title": "Клиника"},
    },
}

# «%» — доля документов с этим типом от всех документов с ошибками в срезе. Документ с
# несколькими типами учитывается в каждой строке, поэтому сумма долей может быть >100%.
TOP_ERROR_TYPE_QUERY = (
    "WITH base AS ( "
    'SELECT COALESCE(NULLIF(TRIM(error_category), \'\'), \'Прочие\') AS cat, '
    'COALESCE(NULLIF(TRIM(error_type), \'\'), \'Неизвестная ошибка\') AS typ, dwh_id '
    "FROM public.rpt_error_breakdown "
    "WHERE COALESCE(NULLIF(TRIM(error_type), ''), '') <> '' "
    "[[AND {{dwh_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] ), "
    "totals AS ( SELECT COUNT(DISTINCT dwh_id)::numeric AS total FROM base ), "
    "per_type AS ( SELECT cat, typ, COUNT(DISTINCT dwh_id)::bigint AS cnt "
    "FROM base GROUP BY 1, 2 ) "
    'SELECT cat AS "Категория ошибки", typ AS "Тип ошибки", cnt AS "Документов", '
    'ROUND(100.0 * cnt / NULLIF((SELECT total FROM totals), 0), 1) AS "%" '
    "FROM per_type ORDER BY cnt DESC"
)

TOP_SEMD_BY_ERROR_KIND_QUERY = (
    "WITH base AS ( "
    "SELECT COALESCE(NULLIF(TRIM(semd_label), ''), 'Неизвестно') AS t, "
    "COALESCE(NULLIF(TRIM(error_type), ''), 'Неизвестная ошибка') AS k, "
    "dwh_id AS doc FROM public.rpt_error_breakdown "
    "WHERE COALESCE(NULLIF(TRIM(semd_label), ''), '') <> '' "
    "AND COALESCE(NULLIF(TRIM(error_type), ''), '') <> '' "
    "[[AND {{dwh_date}}]] [[AND {{jid}}]] [[AND {{semd_type}}]] ), "
    "totals AS ( SELECT t, COUNT(DISTINCT doc) AS total FROM base GROUP BY t ), "
    "ranked_semd AS ( SELECT t, total, ROW_NUMBER() OVER (ORDER BY total DESC, t) AS rn FROM totals ), "
    "per_pair AS ( "
    "SELECT b.t, b.k, COUNT(DISTINCT b.doc)::bigint AS docs "
    "FROM base b GROUP BY 1, 2 ), "
    "ranked_pair AS ( "
    "SELECT p.t, p.k, p.docs, r.total, r.rn AS semd_rn, "
    "ROW_NUMBER() OVER (PARTITION BY p.t ORDER BY p.docs DESC, p.k) AS type_rn "
    "FROM per_pair p JOIN ranked_semd r ON r.t = p.t ) "
    'SELECT t AS "СЭМД", k AS "Тип ошибки", docs AS "Документов" '
    "FROM ranked_pair WHERE semd_rn <= 15 AND type_rn <= 5 "
    "ORDER BY semd_rn, t, type_rn"
)

TOP_SEMD_BY_ERRORS_QUERY = (
    "WITH per_code AS ( "
    "SELECT semd_label AS label, "
    "COUNT(DISTINCT dwh_id)::bigint AS total, "
    "COUNT(DISTINCT dwh_id) FILTER (WHERE status IN ('async_error','network_error'))::bigint AS errs "
    "FROM public.rpt_documents "
    "WHERE status IN ('success','async_error','network_error') "
    "AND NULLIF(TRIM(semd_label), '') IS NOT NULL "
    f"{DOCUMENT_FILTERS} GROUP BY 1 ), "
    "ranked AS ( SELECT label, total, errs, ROW_NUMBER() OVER (ORDER BY errs DESC) AS rn "
    "FROM per_code WHERE errs > 0 ) "
    'SELECT CASE WHEN rn <= 8 THEN label ELSE \'Прочие\' END AS "СЭМД", '
    'SUM(errs)::bigint AS "Документов", '
    'ROUND(100.0 * SUM(errs) / NULLIF(SUM(total), 0), 1) AS "%" '
    "FROM ranked GROUP BY 1 ORDER BY 2 DESC"
)

ERROR_TYPE_CLINIC_FIELD_FILTERS = {
    "dwh_date": {"table_ref": "public.rpt_documents", "field_name": "processed_at"},
    "jid": {"table_ref": "public.rpt_documents", "field_name": "clinic_jid"},
    "semd_type": {"table_ref": "public.rpt_documents", "field_name": "semd_code"},
    "error_type": {"table_ref": "public.rpt_error_breakdown", "field_name": "error_type"},
}

ERROR_TYPE_CLINIC_TABLE_COLUMNS = [
    {"enabled": True, "name": "Тип ошибки"},
    {"enabled": True, "name": "Клиника"},
    {"enabled": True, "name": "Паттерн ошибки"},
    {"enabled": False, "name": "JID Клиники"},
    {"enabled": True, "name": "Документов"},
    {"enabled": True, "name": "% ошибок"},
]

ERROR_TYPE_CLINIC_COLUMN_WIDTHS = [280, 200, 360, 88, 96, 104]

SUCCESS_CLINIC_COLUMN_WIDTHS = [88, 300, 88, 88]
SUCCESS_SEMD_COLUMN_WIDTHS = [88, 120, 88, 88, 88]

ERROR_TYPE_CLINIC_TEMPLATE_TAGS = {
    "jid": {
        "widget-type": "string/=",
        "display-name": "JID Клиники",
        "id": "fb050601-0601-4601-8601-000000000002",
        "name": "jid",
        "type": "dimension",
    },
    "dwh_date": {
        "widget-type": "date/all-options",
        "display-name": "По дате «Обработано»",
        "id": "fb050601-0601-4601-8601-000000000001",
        "name": "dwh_date",
        "type": "dimension",
    },
    "semd_type": {
        "widget-type": "string/=",
        "display-name": "Код СЭМД",
        "id": "fb050601-0601-4601-8601-000000000003",
        "name": "semd_type",
        "type": "dimension",
    },
    "error_type": {
        "widget-type": "string/=",
        "display-name": "Тип ошибки",
        "id": "fb050601-0601-4601-8601-000000000004",
        "name": "error_type",
        "type": "dimension",
    },
}

QUEUE_TABLE_COLUMNS = [
    {"enabled": True, "name": "Сегмент ожидания"},
    {"enabled": True, "name": "Дней в ожидании"},
    {"enabled": True, "name": "Дата отправки"},
    {"enabled": True, "name": "Клиника"},
    {"enabled": True, "name": "Код СЭМД"},
    {"enabled": True, "name": "Наименование СЭМД"},
    {"enabled": False, "name": "JID Клиники"},
    {"enabled": True, "name": "localUid СЭМД"},
]

COUNT_COLUMN_SETTINGS = {
    '["name","Документов"]': {
        "column_title": "Документов",
        "decimals": 0,
        "number_separators": ", ",
    },
    '["name","%"]': {
        "column_title": "%",
        "decimals": 1,
        "number_separators": ", ",
        "suffix": " %",
    },
}


def fix_sql(query: str) -> str:
    q = query
    q = q.replace("semd_code_name", "semd_label")
    q = re.sub(r"END AS status_label,", 'END AS "Статус",', q)
    q = re.sub(
        r'SELECT DATE\(processed_at\) AS "Дата", status_label,',
        'SELECT DATE(processed_at) AS "Дата", status_label AS "Статус",',
        q,
    )
    q = q.replace('AS "Вид ошибки"', 'AS "Тип ошибки"')
    q = q.replace("AS error_category,", 'AS "Категория ошибки",')
    q = q.replace("AS error_type,", 'AS "Тип ошибки",')
    q = q.replace("AS network_error_type,", 'AS "Тип сетевой ошибки",')
    q = q.replace("AS wait_segment,", 'AS "Сегмент ожидания",')
    q = q.replace("AS processed_day,", 'AS "День",')
    q = q.replace("SELECT b.t AS semd_code,", 'SELECT b.t AS "Код СЭМД",')
    q = q.replace(
        "SELECT COALESCE(NULLIF(TRIM(semd_code), ''), 'Неизвестно') AS semd_code,",
        'SELECT COALESCE(NULLIF(TRIM(semd_code), \'\'), \'Неизвестно\') AS "Код СЭМД",',
    )
    q = q.replace("semd AS semd_code,", 'semd AS "Код СЭМД",')
    q = q.replace("err AS error_type,", 'err AS "Тип ошибки",')
    q = q.replace(
        "COALESCE(NULLIF(TRIM(semd_code), ''), 'Неизвестно') AS semd_code,",
        "COALESCE(NULLIF(TRIM(semd_code), ''), 'Неизвестно') AS \"Код СЭМД\",",
    )
    q = q.replace(
        "SELECT COALESCE(NULLIF(TRIM(error_type), ''), 'Неизвестная ошибка') AS error_type,",
        'SELECT COALESCE(NULLIF(TRIM(error_type), \'\'), \'Неизвестная ошибка\') AS "Тип ошибки",',
    )
    q = q.replace(
        "CASE WHEN rn <= 8 THEN code ELSE 'Прочие' END AS semd_code,",
        'CASE WHEN rn <= 8 THEN code ELSE \'Прочие\' END AS "Код СЭМД",',
    )
    q = q.replace(
        'SELECT semd_code, cnt AS "Документов",',
        'SELECT semd_code AS "Код СЭМД", cnt AS "Документов",',
    )
    q = q.replace('SUM(errs)::bigint AS "Ошибок"', 'SUM(errs)::bigint AS "Документов"')
    q = q.replace('errs AS "Ошибок"', 'errs AS "Документов"')
    q = q.replace('SUM(r.cnt)::bigint AS "Количество"', 'SUM(r.cnt)::bigint AS "Документов"')
    q = re.sub(
        r"AS clinic_jid, COALESCE\(MAX\(NULLIF\(TRIM\(clinic_name",
        'AS "JID Клиники", COALESCE(MAX(NULLIF(TRIM(clinic_name',
        q,
    )
    q = re.sub(
        r"AS clinic_jid, COALESCE\(MAX\(NULLIF\(TRIM\(clinic_name::text\), ''\)\), 'Неизвестно'\) AS \"Клиника\"",
        'AS "JID Клиники", COALESCE(MAX(NULLIF(TRIM(clinic_name::text), \'\')), \'Неизвестно\') AS "Клиника"',
        q,
    )
    q = re.sub(
        r"AS clinic_jid, COALESCE\(MAX\(NULLIF\(TRIM\(clinic_name::text\), ''\)\), 'Неизвестно'\) AS \"Клиника\", COALESCE\(NULLIF\(TRIM\(semd_code\)",
        'AS "JID Клиники", COALESCE(MAX(NULLIF(TRIM(clinic_name::text), \'\')), \'Неизвестно\') AS "Клиника", COALESCE(NULLIF(TRIM(semd_code)',
        q,
    )
    q = q.replace(
        "COALESCE(NULLIF(TRIM(semd_label), ''), NULLIF(TRIM(semd_code), ''), '(неизвестно)') AS \"Тип СЭМД\"",
        "COALESCE(NULLIF(TRIM(semd_code), ''), '(неизвестно)') AS \"Код СЭМД\"",
    )
    q = re.sub(
        r'SELECT processed_at AS "Создано", clinic_name,',
        'SELECT processed_at AS "Создано", clinic_label AS "Клиника",',
        q,
    )
    q = re.sub(
        r"SELECT semd_local_uid, semd_code, semd_name, clinic_jid::text AS clinic_jid, clinic_name,",
        'SELECT semd_local_uid AS "localUid СЭМД", semd_code AS "Код СЭМД", '
        'semd_name AS "Наименование СЭМД", clinic_jid::text AS "JID Клиники", clinic_label AS "Клиника",',
        q,
    )
    q = q.replace(
        "sent_at,",
        'sent_at AS "Дата отправки",',
    )
    q = q.replace(
        "waiting_days,",
        'waiting_days AS "Дней в ожидании",',
    )
    q = q.replace(
        "wait_segment FROM",
        'wait_segment AS "Сегмент ожидания" FROM',
    )
    q = re.sub(
        r"SELECT clinic_jid::text AS clinic_jid, clinic_name,",
        'SELECT clinic_jid::text AS "JID Клиники", clinic_label AS "Клиника",',
        q,
    )
    return q


def fix_detail_quality_sql() -> str:
    # No table alias on rpt_documents: Metabase field filters expand to
    # "rpt_documents".<col>. Mismatch markers (↯) drive per-cell highlighting.
    return (
        "WITH base AS (\n"
        "  SELECT\n"
        "    rpt_documents.processed_at AS \"Дата обработки\",\n"
        "    rpt_documents.status_label AS \"Статус\",\n"
        "    rpt_documents.clinic_label AS \"Клиника\",\n"
        "    rpt_documents.semd_code AS \"Код СЭМД\",\n"
        "    rpt_documents.semd_name AS \"Наименование СЭМД\",\n"
        "    rpt_documents.semd_local_uid AS \"localUid СЭМД\",\n"
        "    rpt_document_lineage.clinic_jid::text AS \"JID Клиники\",\n"
        "    CASE\n"
        "      WHEN NULLIF(btrim(rpt_document_lineage.clinic_oid_xml), '') IS NOT NULL\n"
        "       AND (\n"
        "         (NULLIF(btrim(rpt_document_lineage.clinic_oid_jpersons), '') IS NOT NULL\n"
        "          AND btrim(rpt_document_lineage.clinic_oid_xml) <> btrim(rpt_document_lineage.clinic_oid_jpersons))\n"
        "         OR (NULLIF(btrim(rpt_document_lineage.clinic_oid_license), '') IS NOT NULL\n"
        "          AND btrim(rpt_document_lineage.clinic_oid_xml) <> btrim(rpt_document_lineage.clinic_oid_license))\n"
        "       )\n"
        "      THEN '↯ ' || rpt_document_lineage.clinic_oid_xml\n"
        "      ELSE COALESCE(NULLIF(btrim(rpt_document_lineage.clinic_oid_xml), ''), '—')\n"
        "    END AS \"OID из XML\",\n"
        "    CASE\n"
        "      WHEN NULLIF(btrim(rpt_document_lineage.clinic_oid_jpersons), '') IS NOT NULL\n"
        "       AND (\n"
        "         (NULLIF(btrim(rpt_document_lineage.clinic_oid_xml), '') IS NOT NULL\n"
        "          AND btrim(rpt_document_lineage.clinic_oid_jpersons) <> btrim(rpt_document_lineage.clinic_oid_xml))\n"
        "         OR (NULLIF(btrim(rpt_document_lineage.clinic_oid_license), '') IS NOT NULL\n"
        "          AND btrim(rpt_document_lineage.clinic_oid_jpersons) <> btrim(rpt_document_lineage.clinic_oid_license))\n"
        "       )\n"
        "      THEN '↯ ' || rpt_document_lineage.clinic_oid_jpersons\n"
        "      ELSE COALESCE(NULLIF(btrim(rpt_document_lineage.clinic_oid_jpersons), ''), '—')\n"
        "    END AS \"OID из JPERSONS\",\n"
        "    CASE\n"
        "      WHEN NULLIF(btrim(rpt_document_lineage.clinic_oid_license), '') IS NOT NULL\n"
        "       AND (\n"
        "         (NULLIF(btrim(rpt_document_lineage.clinic_oid_xml), '') IS NOT NULL\n"
        "          AND btrim(rpt_document_lineage.clinic_oid_license) <> btrim(rpt_document_lineage.clinic_oid_xml))\n"
        "         OR (NULLIF(btrim(rpt_document_lineage.clinic_oid_jpersons), '') IS NOT NULL\n"
        "          AND btrim(rpt_document_lineage.clinic_oid_license) <> btrim(rpt_document_lineage.clinic_oid_jpersons))\n"
        "       )\n"
        "      THEN '↯ ' || rpt_document_lineage.clinic_oid_license\n"
        "      ELSE COALESCE(NULLIF(btrim(rpt_document_lineage.clinic_oid_license), ''), '—')\n"
        "    END AS \"OID из лицензий\",\n"
        "    COALESCE(NULLIF(btrim(rpt_document_lineage.clinic_host), ''), '—') AS \"Host Клиники (ГОСТ VPN)\",\n"
        "    COALESCE(NULLIF(btrim(rpt_document_lineage.clinic_jid_resolve_method), ''), '—') AS \"Метод резолва JID\",\n"
        "    rpt_documents.clinic_jid_mismatch AS \"Расхождение источников JID\",\n"
        "    TRIM(BOTH ' · ' FROM CONCAT_WS(' · ',\n"
        "      CASE WHEN NULLIF(BTRIM(rpt_documents.clinic_jid::text), '') IS NULL THEN 'без JID' END,\n"
        "      CASE WHEN rpt_documents.clinic_jid_mismatch = true THEN 'расхождение OID/JID' END,\n"
        "      CASE WHEN NULLIF(BTRIM(rpt_documents.semd_local_uid::text), '') IS NULL THEN 'без localUid' END,\n"
        "      CASE WHEN NULLIF(BTRIM(rpt_documents.semd_code::text), '') IS NULL THEN 'без кода СЭМД' END,\n"
        "      CASE WHEN rpt_documents.status = 'success' AND rpt_documents.processed_at IS NULL THEN 'успех без даты' END\n"
        "    )) AS \"Нарушения\"\n"
        "  FROM public.rpt_documents\n"
        "  INNER JOIN public.rpt_document_lineage\n"
        "    ON rpt_document_lineage.dwh_id = rpt_documents.dwh_id\n"
        "  WHERE rpt_documents.status IN ('success', 'async_error', 'network_error')\n"
        "    [[AND {{dwh_date}}]] [[AND {{jid}}]] [[AND {{semd_type}}]]\n"
        ")\n"
        "SELECT *\n"
        "FROM base\n"
        "WHERE \"Нарушения\" <> ''\n"
        "ORDER BY \"Дата обработки\" DESC NULLS LAST\n"
        "LIMIT 1000"
    )


def strip_chart_keys(viz: dict, display: str) -> None:
    if display != "table":
        return
    for key in list(viz.keys()):
        if key.startswith("graph.") or key.startswith("pie."):
            del viz[key]
        if key == "table.pivot_column" and not viz.get("table.pivot"):
            del viz[key]


def apply_document_volume_by_day(card: dict) -> None:
    card["display"] = "bar"
    card["description"] = (
        "Поступление документов на прокси по дням (first_sent_at или CreateDate из XML, "
        "без sent_at). Фильтр «Период» — по дате поступления (arrival_day), не по дате обработки."
    )
    dq = card.setdefault("dataset_query", {})
    dq["native"]["query"] = DOCUMENT_VOLUME_BY_DAY_QUERY
    tags = dq["native"].setdefault("template-tags", {})
    if "dwh_date" in tags:
        tags["dwh_date"]["display-name"] = "По дате поступления"
    card.setdefault("metabase-field-filters", {})["dwh_date"] = {
        "table_ref": "public.rpt_documents",
        "field_name": "arrival_day",
    }
    viz = card.setdefault("visualization_settings", {})
    viz["graph.dimensions"] = ["Дата"]
    viz["graph.metrics"] = ["Документов"]
    viz["graph.x_axis.scale"] = "timeseries"
    viz["graph.show_values"] = True
    viz["graph.label_value_formatting"] = "compact"
    viz["stackable.stack_type"] = None
    cs = viz.setdefault("column_settings", {})
    cs['["name","Документов"]'] = {
        "column_title": "Документов",
        "decimals": 0,
        "number_separators": ", ",
    }


QUALITY_DETAIL_MISMATCH_MARK = "↯ "
QUALITY_DETAIL_VIOLATION_BG = "#FEE2E2"


def apply_quality_detail(card: dict) -> None:
    card["display"] = "table"
    card["description"] = (
        "Документы с любым нарушением правил сводной таблицы «Контроль качества данных» за период. "
        "Колонка «Нарушения» перечисляет сработавшие проверки; ячейки с нарушениями и расхождениями "
        "источников подсвечены красным. Лимит 1000 строк."
    )
    dq = card.setdefault("dataset_query", {})
    dq["native"]["query"] = fix_detail_quality_sql()
    viz = card.setdefault("visualization_settings", {})
    viz["table.columns"] = [
        {"enabled": True, "name": "Дата обработки"},
        {"enabled": True, "name": "Статус"},
        {"enabled": True, "name": "Нарушения"},
        {"enabled": True, "name": "Клиника"},
        {"enabled": True, "name": "JID Клиники"},
        {"enabled": True, "name": "OID из XML"},
        {"enabled": True, "name": "OID из JPERSONS"},
        {"enabled": True, "name": "OID из лицензий"},
        {"enabled": True, "name": "Host Клиники (ГОСТ VPN)"},
        {"enabled": False, "name": "Метод резолва JID"},
        {"enabled": True, "name": "Код СЭМД"},
        {"enabled": True, "name": "Наименование СЭМД"},
        {"enabled": True, "name": "localUid СЭМД"},
        {"enabled": False, "name": "Расхождение источников JID"},
    ]
    viz["table.column_formatting"] = [
        {
            "color": QUALITY_DETAIL_VIOLATION_BG,
            "columns": ["Нарушения"],
            "operator": "!=",
            "type": "single",
            "value": "",
        },
        {
            "color": QUALITY_DETAIL_VIOLATION_BG,
            "columns": ["JID Клиники"],
            "operator": "is-null",
            "type": "single",
        },
        {
            "color": QUALITY_DETAIL_VIOLATION_BG,
            "columns": ["JID Клиники"],
            "operator": "=",
            "type": "single",
            "value": "",
        },
        {
            "color": QUALITY_DETAIL_VIOLATION_BG,
            "columns": ["localUid СЭМД"],
            "operator": "is-null",
            "type": "single",
        },
        {
            "color": QUALITY_DETAIL_VIOLATION_BG,
            "columns": ["localUid СЭМД"],
            "operator": "=",
            "type": "single",
            "value": "",
        },
        {
            "color": QUALITY_DETAIL_VIOLATION_BG,
            "columns": ["Код СЭМД"],
            "operator": "is-null",
            "type": "single",
        },
        {
            "color": QUALITY_DETAIL_VIOLATION_BG,
            "columns": ["Код СЭМД"],
            "operator": "=",
            "type": "single",
            "value": "",
        },
        {
            "color": QUALITY_DETAIL_VIOLATION_BG,
            "columns": ["OID из XML", "OID из JPERSONS", "OID из лицензий"],
            "operator": "starts-with",
            "type": "single",
            "value": QUALITY_DETAIL_MISMATCH_MARK,
        },
    ]
    cs = viz.setdefault("column_settings", {})
    cs['["name","Дата обработки"]'] = {
        "date_style": "D MMMM, YYYY",
        "time_enabled": "minutes",
    }
    strip_chart_keys(viz, "table")


def apply_transactions_trend(card: dict) -> None:
    card["display"] = "bar"
    card["description"] = (
        "Объём документов по дням и текущему статусу (stacked). "
        "Клик по сегменту — архив с фильтром по статусу."
    )
    dq = card.setdefault("dataset_query", {})
    dq["native"]["query"] = TRANSACTIONS_BY_DAY_STATUS_QUERY
    card["metabase-field-filters"] = {
        "dwh_date": {"table_ref": "public.rpt_documents", "field_name": "processed_at"},
        "semd_type": {"table_ref": "public.rpt_documents", "field_name": "semd_code"},
        "jid": {"table_ref": "public.rpt_documents", "field_name": "clinic_jid"},
    }
    viz = card.setdefault("visualization_settings", {})
    viz["graph.dimensions"] = ["Дата", "Статус"]
    viz["graph.metrics"] = ["Документов"]
    viz["graph.x_axis.title_text"] = "Дата"
    viz["graph.y_axis.title_text"] = "Документов"
    viz["graph.x_axis.axis_enabled"] = "rotate-45"
    viz["graph.x_axis.scale"] = "timeseries"
    viz["graph.show_values"] = True
    viz["graph.label_value_formatting"] = "compact"
    viz["stackable.stack_type"] = "stacked"
    viz.setdefault("series_settings", {}).update(
        {
            "В обработке": {"color": "#509EE3"},
            "Ошибка асинхронного ответа РЭМД": {"color": "#A989C5"},
            "Ошибка связи": {"color": "#F2994A"},
            "Успешно зарегистрирован": {"color": "#84BB4C"},
        }
    )
    cs = viz.setdefault("column_settings", {})
    cs['["name","Документов"]'] = {
        "column_title": "Документов",
        "decimals": 0,
        "number_separators": ", ",
    }


def apply_semd_volume_table(card: dict) -> None:
    """«Объём по СЭМД» — таблица кодов СЭМД по числу документов в срезе, по образцу
    «Объём по клиникам». Колонка «%» — доля от общего числа документов."""
    card["name"] = "Объём по СЭМД"
    card["display"] = "table"
    card["description"] = "Объём документов по кодам СЭМД в срезе. Колонка «%» — доля от общего числа документов."
    viz = card.setdefault("visualization_settings", {})
    for key in list(viz.keys()):
        if key.startswith("graph.") or key.startswith("pie."):
            del viz[key]
    viz["table.columns"] = [
        {"enabled": True, "name": "Код СЭМД"},
        {"enabled": True, "name": "Документов"},
        {"enabled": True, "name": "%"},
    ]
    viz["table.cell_column"] = "Код СЭМД"
    viz["column_settings"] = deepcopy(COUNT_COLUMN_SETTINGS)
    strip_chart_keys(viz, "table")


def apply_clinic_volume(card: dict) -> None:
    card["dataset_query"]["native"]["query"] = CLINIC_VOLUME_QUERY
    viz = card.setdefault("visualization_settings", {})
    viz["table.columns"] = deepcopy(CLINIC_VOLUME_TABLE_COLUMNS)
    viz["table.cell_column"] = "Клиника"
    viz["column_settings"] = deepcopy(COUNT_COLUMN_SETTINGS)
    strip_chart_keys(viz, card.get("display", "table"))


def apply_clinic_error_volume(card: dict) -> None:
    card["description"] = (
        f"Топ-{CLINIC_ERROR_VOLUME_TOP_N} клиник по объёму отказов (async_error + network_error) "
        "и строка «Прочие» с взвешенным % ошибок. Детальная разбивка по видам — вкладка **Анализ ошибок**."
    )
    card["dataset_query"]["native"]["query"] = CLINIC_ERROR_VOLUME_QUERY
    card["display"] = "combo"
    viz = card.setdefault("visualization_settings", {})
    viz["graph.dimensions"] = ["Клиника"]
    viz["graph.metrics"] = ["Документов", "% ошибок"]
    viz["graph.show_values"] = True
    viz["graph.max_categories"] = 20
    viz.pop("graph.x_axis.axis_enabled", None)
    viz.pop("graph.y_axis.auto_split", None)
    viz["series_settings"] = {
        "% ошибок": {"axis": "right", "color": "#F2994A", "display": "bar"},
        "Документов": {
            "axis": "left",
            "color": "#DC2626",
            "display": "line",
            "line.interpolate": "linear",
            "line.size": "M",
            "line.style": "solid",
        },
    }
    cs = viz.setdefault("column_settings", {})
    cs.update(deepcopy(COUNT_COLUMN_SETTINGS))
    cs['["name","% ошибок"]'] = {
        "column_title": "% ошибок",
        "decimals": 1,
        "number_separators": ", ",
        "suffix": " %",
    }


def apply_error_type_clinic(card: dict) -> None:
    card["description"] = (
        "Тип ошибки × клиника: COUNT(DISTINCT «ID») и доля от финализированного "
        "документного универсума клиники. Клик — модель «Разбивка ошибок» (грейн "
        "тип×документ) с точным фильтром по типу ошибки и JID клиники из строки и "
        "фильтрами дашборда."
    )
    card.pop("query_tier", None)
    card.pop("source_model", None)
    card["dataset_query"] = {
        "type": "native",
        "database": 1,
        "native": {
            "query": ERROR_TYPE_CLINIC_QUERY,
            "template-tags": deepcopy(ERROR_TYPE_CLINIC_TEMPLATE_TAGS),
        },
    }
    card["metabase-field-filters"] = deepcopy(ERROR_TYPE_CLINIC_FIELD_FILTERS)
    card.pop("metabase-parameter-targets", None)
    card.pop("metabase-model-drill-params", None)
    viz = card.setdefault("visualization_settings", {})
    viz["table.columns"] = deepcopy(ERROR_TYPE_CLINIC_TABLE_COLUMNS)
    viz["table.column_widths"] = deepcopy(ERROR_TYPE_CLINIC_COLUMN_WIDTHS)
    viz["table.cell_column"] = "Документов"
    cs = {
        '["name","Документов"]': {
            "column_title": "Документов",
            "decimals": 0,
            "number_separators": ", ",
        },
        '["name","% ошибок"]': {
            "column_title": "% ошибок",
            "decimals": 1,
            "number_separators": ", ",
            "suffix": " %",
        },
        '["name","Тип ошибки"]': {"column_title": "Тип ошибки", "text_style": "wrap"},
        '["name","Паттерн ошибки"]': {
            "column_title": "Паттерн ошибки",
            "text_style": "wrap",
        },
    }
    viz["column_settings"] = cs
    strip_chart_keys(viz, "table")


def apply_heatmap(card: dict) -> None:
    card["display"] = "table"
    card["dataset_query"]["native"]["query"] = HEATMAP_QUERY
    viz = card.setdefault("visualization_settings", {})
    viz.clear()
    viz.update(deepcopy(HEATMAP_VIZ))


def apply_top_error_type_table(card: dict) -> None:
    """«Топ по типу ошибки» — табличный рейтинг атомарных видов ошибки (error_type)
    с категорией и долей документов от всех документов с ошибками в срезе."""
    card["display"] = "table"
    card["description"] = (
        "Рейтинг атомарных видов ошибки (`error_type`) по числу документов в срезе. "
        "Колонка «%» — доля документов с этим типом от всех документов с ошибками."
    )
    card["dataset_query"]["native"]["query"] = TOP_ERROR_TYPE_QUERY
    viz = card.setdefault("visualization_settings", {})
    for key in list(viz.keys()):
        if key.startswith("graph.") or key.startswith("pie."):
            del viz[key]
    viz.pop("series_settings", None)
    viz["table.columns"] = [
        {"enabled": True, "name": "Категория ошибки"},
        {"enabled": True, "name": "Тип ошибки"},
        {"enabled": True, "name": "Документов"},
        {"enabled": True, "name": "%"},
    ]
    viz["table.cell_column"] = "Документов"
    cs = deepcopy(COUNT_COLUMN_SETTINGS)
    cs['["name","Тип ошибки"]'] = {"column_title": "Тип ошибки", "text_style": "wrap"}
    viz["column_settings"] = cs
    strip_chart_keys(viz, "table")


def apply_top_category_type_bar(card: dict) -> None:
    """«Топ категорий и типов ошибки» — стэк-бар категория×тип, где КАЖДЫЙ тип окрашен в
    цвет своей категории (стэк категории становится одноцветным). Палитра — из словаря."""
    card["display"] = "row"
    card["description"] = (
        "Категория ошибки (ось) × тип (стэк), документов COUNT(DISTINCT «ID»). "
        "Каждый вид окрашен цветом своей категории."
    )
    viz = card.setdefault("visualization_settings", {})
    viz["graph.dimensions"] = ["Категория ошибки", "Тип ошибки"]
    viz["graph.metrics"] = ["Документов"]
    viz["stackable.stack_type"] = "stacked"
    viz["graph.label_value_formatting"] = "compact"
    viz["graph.y_axis.scale"] = "linear"
    viz["graph.x_axis.title_text"] = ""
    viz["graph.y_axis.title_text"] = ""
    # Цвет серии (типа) = цвет его категории; категории — свои цвета (единая палитра).
    colors = error_type_color_map()
    viz["series_settings"] = {
        name: {"color": color} for name, color in colors.items()
    }
    cs = viz.setdefault("column_settings", {})
    cs['["name","Документов"]'] = {
        "column_title": "Документов",
        "decimals": 0,
        "number_separators": ", ",
    }


def apply_top_semd_by_error_kind(card: dict) -> None:
    card["display"] = "row"
    card["dataset_query"]["native"]["query"] = TOP_SEMD_BY_ERROR_KIND_QUERY
    viz = card.setdefault("visualization_settings", {})
    viz["graph.dimensions"] = ["СЭМД", "Тип ошибки"]
    viz["graph.metrics"] = ["Документов"]
    viz["stackable.stack_type"] = "stacked"
    viz["graph.show_stack_values"] = "total"
    viz["graph.label_value_frequency"] = "all"
    viz["graph.x_axis.scale"] = "ordinal"
    viz["graph.x_axis.axis_enabled"] = "rotate-45"
    cs = viz.setdefault("column_settings", {})
    cs.pop('["name","Код СЭМД"]', None)
    cs['["name","СЭМД"]'] = {"column_title": "СЭМД", "text_style": "wrap"}
    cs['["name","Тип ошибки"]'] = {"column_title": "Вид ошибки"}
    strip_chart_keys(viz, "row")


def apply_top_semd_by_errors(card: dict) -> None:
    card["dataset_query"]["native"]["query"] = TOP_SEMD_BY_ERRORS_QUERY
    viz = card.setdefault("visualization_settings", {})
    dims = viz.get("graph.dimensions") or viz.get("pie.dimension")
    if dims and "Код СЭМД" in dims:
        viz["graph.dimensions"] = [
            "СЭМД" if d == "Код СЭМД" else d for d in dims
        ]
    cs = viz.setdefault("column_settings", {})
    if '["name","Код СЭМД"]' in cs:
        cs['["name","СЭМД"]'] = cs.pop('["name","Код СЭМД"]')


def apply_success_slice_tables(card: dict) -> None:
    viz = card.setdefault("visualization_settings", {})
    name = card.get("name", "")
    if name == "Успешность по клиникам":
        viz["table.column_widths"] = deepcopy(SUCCESS_CLINIC_COLUMN_WIDTHS)
    elif name == "Успешность по типам СЭМД":
        viz["table.column_widths"] = deepcopy(SUCCESS_SEMD_COLUMN_WIDTHS)


def apply_queue_table(card: dict) -> None:
    viz = card.setdefault("visualization_settings", {})
    viz["table.columns"] = deepcopy(QUEUE_TABLE_COLUMNS)
    viz["table.cell_column"] = "Сегмент ожидания"


def apply_latest_operations(card: dict) -> None:
    card["description"] = (
        "До 50 последних документов в периоде; сортировка по дате последней активности "
        "(processed_at, новые сверху). Одна строка — один dwh_id."
    )
    q = card.setdefault("dataset_query", {}).setdefault("query", {})
    q["fields"] = deepcopy(LATEST_OPERATIONS_QUERY_FIELDS)
    q["order-by"] = [["desc", ["field", "Документы:processed_at", None]]]
    q["limit"] = 50
    q["source-table"] = "model:Документы"
    viz = card.setdefault("visualization_settings", {})
    viz.pop("table", None)
    viz["table.columns"] = deepcopy(LATEST_OPERATIONS_TABLE_COLUMNS)
    viz["table.cell_column"] = "Клиника"
    viz["table.column_widths"] = [148, 108, 200, 240, 128, 128, 300]
    cs = viz.setdefault("column_settings", {})
    for key, value in LATEST_OPERATIONS_COLUMN_SETTINGS.items():
        cs[key] = value
    strip_chart_keys(viz, card.get("display", "table"))


def fix_viz(viz: dict, *, display: str = "table") -> None:
    for col in viz.get("table.columns", []) or []:
        if col.get("name") == "JID+Наименование":
            col["name"] = "Клиника"
        elif col.get("name") == "DWH_ID":
            col["name"] = "dwh_id"
            col["enabled"] = False
    for key in ("graph.dimensions", "pie.dimension"):
        dims = viz.get(key)
        if not dims:
            continue
        viz[key] = [_dim(d) for d in dims]
    cs = viz.get("column_settings") or {}
    new_cs = {}
    for k, v in cs.items():
        nk = k.replace("JID+Наименование", "Клиника").replace("DWH_ID", "dwh_id")
        nk = nk.replace("Вид ошибки", "Тип ошибки").replace('"Ошибок"', '"Документов"')
        if isinstance(v, dict) and v.get("column_title") == "%" and v.get("decimals") == 1:
            v = {**v, "suffix": " %"}
        if isinstance(v, dict) and v.get("column_title") == "Ошибок":
            v = {**v, "column_title": "Документов"}
        if isinstance(v, dict) and v.get("column_title") == "Количество":
            v = {**v, "column_title": "Документов"}
        new_cs[nk] = v
    if new_cs:
        viz["column_settings"] = new_cs
    if viz.get("pie.metric") == "Ошибок":
        viz["pie.metric"] = "Документов"
    if viz.get("table.cell_column") == "Ошибок":
        viz["table.cell_column"] = "Документов"
    ss = viz.get("series_settings") or {}
    if "Ошибок" in ss:
        ss["Документов"] = ss.pop("Ошибок")
    metrics = viz.get("graph.metrics")
    if metrics:
        viz["graph.metrics"] = ["Документов" if m == "Ошибок" else m for m in metrics]
    if display == "pie":
        for key in list(viz.keys()):
            if key.startswith("graph."):
                del viz[key]
    elif display in {"bar", "row", "line", "area", "combo", "scatter", "waterfall"}:
        for key in list(viz.keys()):
            if key.startswith("pie."):
                del viz[key]
    strip_chart_keys(viz, display)


def _dim(d: str) -> str:
    mapping = {
        "JID+Наименование": "Клиника",
        "Наименование клиники": "Клиника",
        "clinic_name": "Клиника",
        "semd_code": "Код СЭМД",
        "СЭМД": "СЭМД",
        "Вид ошибки": "Тип ошибки",
        "error_category": "Категория ошибки",
        "network_error_type": "Тип сетевой ошибки",
        "wait_segment": "Сегмент ожидания",
        "status_label": "Статус",
        "processed_day": "День",
    }
    return mapping.get(d, d)


def build_drill(mappings: list[tuple[str, str]]) -> dict:
    pm: dict = {}
    for slug, col in mappings:
        pm[slug] = {
            "source": {"type": "column", "name": col},
            "target": {"type": "parameter", "id": PARAM_IDS[slug]},
        }
    return {
        "type": "link",
        "linkType": "dashboard",
        "targetDashboard": "Интеграция с ЕГИСЗ",
        "tab": "archive",
        "parameterMapping": pm,
    }


def build_model_drill(
    model_ref: str,
    mappings: list[ModelDrillMapping],
) -> dict:
    pm: dict = {}
    for item in mappings:
        field_name, col = item[0], item[1]
        operator = item[2] if len(item) > 2 else None
        target: dict = {
            "type": "dimension",
            "model_ref": model_ref,
            "field_name": field_name,
        }
        if operator:
            target["operator"] = operator
        pm[field_name] = {
            "source": {"type": "column", "name": col},
            "target": target,
        }
    return {
        "type": "link",
        "linkType": "question",
        "targetModel": model_ref,
        "parameterMapping": pm,
    }


def convert_archive_card(card: dict) -> None:
    card["description"] = (
        "Список документов за выбранный период и фильтры. "
        "Одна строка — один документ (dwh_id). До 1000 последних по дате обработки."
    )
    card["query_tier"] = "query_builder"
    card["source_model"] = "Документы"
    card["dataset_query"] = {
        "type": "query",
        "database": 1,
        "query": {
            "source-table": "model:Документы",
            "limit": 1000,
            "order-by": [["desc", ["field", "Документы:processed_at", None]]],
        },
    }
    card["metabase-parameter-targets"] = deepcopy(DOCUMENTS_PARAM_TARGETS)
    card.pop("metabase-field-filters", None)
    viz = card.setdefault("visualization_settings", {})
    viz["table.columns"] = deepcopy(ARCHIVE_TABLE_COLUMNS)
    cs = viz.setdefault("column_settings", {})
    cs['["name","dwh_id"]'] = {"column_title": "dwh_id"}


def ensure_dashboard_parameters(dash: dict) -> None:
    params = dash.setdefault("parameters", [])
    if not any(p.get("slug") == "error_type_filter" for p in params):
        params.append(
            {
                "id": PARAM_IDS["error_type_filter"],
                "name": "Тип ошибки",
                "slug": "error_type_filter",
                "type": "string/=",
                "sectionId": "string",
            }
        )


def apply_01(dash: dict) -> None:
    ensure_dashboard_parameters(dash)
    dash["description"] = (
        "Единый эксплуатационный дашборд обмена с ЕГИСЗ: оперативный контроль, "
        "динамика сервиса, очередь без callback, аналитика ошибок и архив документов."
    )
    for param in dash.get("parameters", []):
        if param.get("slug") == "dwh_date_filter":
            param["default"] = DEFAULT_DWH_PERIOD
    cards = dash["cards"]
    filtered: list[dict] = []

    for card in cards:
        if card.get("display") == "text":
            filtered.append(card)
            continue
        name = card.get("name", "")
        if name in ARCHIVE_FROM_OPERATIONAL and card.get("tab") == "operational":
            card["tab"] = "archive"
        if name in MOVE_TO_ERRORS and card.get("tab") == "operational":
            card["tab"] = "errors"
        if name == "Топ по типу СЭМД" and card.get("tab") == "errors":
            continue
        if name in RENAME_01:
            card["name"] = RENAME_01[name]
        name = card.get("name", "")

        dq = card.get("dataset_query", {})
        if dq.get("type") == "native":
            if name != "Детализация контроля качества":
                dq["native"]["query"] = fix_sql(dq["native"]["query"])
        elif dq.get("type") == "query":
            query = dq.get("query", {})
            if name == "Статусы за период":
                query["aggregation"] = [
                    [
                        "aggregation-options",
                        ["distinct", ["field", "Документы:dwh_id", None]],
                        {"name": "Документов", "display-name": "Документов"},
                    ]
                ]
            if name == "Ошибки: тип × клиника":
                pass

        if name == "Топ типов СЭМД по видам ошибки":
            apply_top_semd_by_error_kind(card)

        if name == "Архив СЭМД":
            convert_archive_card(card)

        if name == "Объём по клиникам" and dq.get("type") == "native":
            apply_clinic_volume(card)
        elif name == "Объём ошибок по клиникам" and dq.get("type") == "native":
            apply_clinic_error_volume(card)
        elif name == "Ошибки: тип × клиника":
            apply_error_type_clinic(card)
        elif name == "Тепловая карта: клиника × день":
            apply_heatmap(card)
        elif name == "Топ по типу ошибки":
            apply_top_error_type_table(card)
        elif name == "Топ категорий и типов ошибки":
            apply_top_category_type_bar(card)
        elif name == "Топ типов СЭМД по ошибкам":
            apply_top_semd_by_errors(card)
        elif name in ("Успешность по клиникам", "Успешность по типам СЭМД"):
            apply_success_slice_tables(card)
        elif name == "Очередь без ответа":
            apply_queue_table(card)
        elif name == "Транзакции по дням и статусам" and dq.get("type") == "native":
            apply_transactions_trend(card)
        elif name == "Динамика документов по дням" and dq.get("type") == "native":
            apply_document_volume_by_day(card)
        elif name in ("Топ типов СЭМД по документам", "Объём по СЭМД") and card.get("tab") == "operational":
            apply_semd_volume_table(card)
            name = card["name"]
        elif name == "Детализация контроля качества":
            apply_quality_detail(card)

        fix_viz(card.setdefault("visualization_settings", {}), display=card.get("display", "table"))

        if name == "Последние операции":
            apply_latest_operations(card)

        if name in MODEL_DRILL_BY_NAME:
            card["click_behavior"] = build_model_drill(
                MODEL_DRILL_TARGET_BY_NAME.get(name, DOCUMENTS_MODEL_REF),
                MODEL_DRILL_BY_NAME[name],
            )
            params = MODEL_DRILL_DASHBOARD_PARAMS.get(name)
            if params:
                card["metabase-model-drill-params"] = {
                    key: DOCUMENTS_PARAM_TARGETS[key]["field_name"] for key in params
                }
        elif name in DRILL_BY_NAME and DRILL_BY_NAME[name]:
            card["click_behavior"] = build_drill(DRILL_BY_NAME[name])
        elif name in DRILL_BY_NAME:
            card.pop("click_behavior", None)

        filtered.append(card)

    dash["cards"] = filtered
    restore_archive_top_semd(dash)


def apply_renames(path: Path, mapping: dict[str, str]) -> bool:
    if not path.exists():
        return False
    dash = json.loads(path.read_text(encoding="utf-8"))
    for card in dash.get("cards", []):
        if card.get("name") in mapping:
            card["name"] = mapping[card["name"]]
        dq = card.get("dataset_query", {})
        if dq.get("type") == "native":
            dq["native"]["query"] = fix_sql(dq["native"]["query"])
        fix_viz(card.get("visualization_settings") or {}, display=card.get("display", "table"))
        if card.get("name") == "Очередь оттока: JID с нулём успехов":
            cs = card.setdefault("visualization_settings", {}).setdefault("column_settings", {})
            cs['["name","Клиника"]'] = {"column_title": "Клиника"}
        if card.get("name") == "Журнал документов с ошибками регистрации":
            cs = card.setdefault("visualization_settings", {}).setdefault("column_settings", {})
            cs['["name","Сводка ошибки"]'] = {"column_title": "Сводка ошибки", "text_style": "wrap"}
            dq = card.get("dataset_query", {})
            if dq.get("type") == "native":
                q = dq["native"]["query"]
                q = q.replace("error_summary AS \"Сводка ошибки\"", 'error_summary AS "Сводка ошибки"')
                if "error_summary" not in q:
                    q = q.replace('error_type AS "Тип ошибки"', 'error_summary AS "Сводка ошибки"')
                    q = q.replace("error_text AS error_text", 'error_summary AS "Сводка ошибки"')
                    q = q.replace('error_text AS "Текст ошибки"', 'error_summary AS "Сводка ошибки"')
                dq["native"]["query"] = q
    return write_json_if_changed(path, dash)


def restore_archive_top_semd(dash: dict) -> None:
    if any(
        c.get("name") in ("Топ типов СЭМД по документам", "Объём по СЭМД")
        for c in dash["cards"]
    ):
        return
    query = (
        "WITH base AS ( SELECT semd_code, COUNT(DISTINCT dwh_id)::bigint AS cnt "
        "FROM public.rpt_documents WHERE 1=1 [[AND {{dwh_date}}]] [[AND {{semd_type}}]] "
        "[[AND {{jid}}]] [[AND {{local_uid}}]] [[AND {{relates_to}}]] [[AND {{emdr_id}}]] "
        "[[AND {{status}}]] [[AND {{log_id}}]] GROUP BY 1 ), "
        "totals AS (SELECT COALESCE(SUM(cnt), 0)::numeric AS total FROM base) "
        'SELECT semd_code AS "Код СЭМД", cnt AS "Документов", '
        'ROUND(100.0 * cnt / NULLIF((SELECT total FROM totals), 0), 1) AS "%" '
        "FROM base ORDER BY cnt DESC"
    )
    ff = {
        k: {"table_ref": "public.rpt_documents", "field_name": v}
        for k, v in {
            "jid": "clinic_jid", "dwh_date": "processed_at", "semd_type": "semd_code",
            "local_uid": "semd_local_uid", "relates_to": "relates_to_id",
            "emdr_id": "semd_emdr_id", "status": "status_label", "log_id": "logid",
        }.items()
    }
    card = {
        "name": "Топ типов СЭМД по документам",
        "description": "Топ кодов СЭМД по числу документов в срезе. Колонка «%» — доля от общего числа документов.",
        "dataset_query": {
            "type": "native",
            "native": {
                "query": query,
                "template-tags": {
                    "jid": {"widget-type": "string/=", "display-name": "JID Клиники",
                            "id": "f6a00003-0003-4003-8003-000000000002", "name": "jid", "type": "dimension"},
                    "dwh_date": {"widget-type": "date/all-options", "display-name": "По дате обработки",
                                 "id": "f6a00003-0003-4003-8003-000000000001", "name": "dwh_date", "type": "dimension"},
                    "semd_type": {"widget-type": "string/=", "display-name": "Код СЭМД",
                                  "id": "f6a00003-0003-4003-8003-000000000003", "name": "semd_type", "type": "dimension"},
                    "local_uid": {"widget-type": "string/=", "display-name": "localUid СЭМД",
                                  "id": "f6a00003-0003-4003-8003-000000000004", "name": "local_uid", "type": "dimension"},
                    "relates_to": {"widget-type": "string/=", "display-name": "Связанное сообщение",
                                   "id": "f6a00003-0003-4003-8003-000000000005", "name": "relates_to", "type": "dimension"},
                    "emdr_id": {"widget-type": "string/=", "display-name": "Рег. Номер РЭМД",
                                "id": "f6a00003-0003-4003-8003-000000000006", "name": "emdr_id", "type": "dimension"},
                    "status": {"widget-type": "string/=", "display-name": "Статус",
                               "id": "f6a00003-0003-4003-8003-000000000007", "name": "status", "type": "dimension"},
                    "log_id": {"widget-type": "string/=", "display-name": "LOGID",
                               "id": "f6a00003-0003-4003-8003-000000000008", "name": "log_id", "type": "dimension"},
                },
            },
            "database": 1,
        },
        "display": "table",
        "visualization_settings": {
            "table.columns": [
                {"enabled": True, "name": "Код СЭМД"},
                {"enabled": True, "name": "Документов"},
                {"enabled": True, "name": "%"},
            ],
            "column_settings": {
                '["name","%"]': {"column_title": "%", "decimals": 1, "number_separators": ", ", "suffix": " %"},
                '["name","Документов"]': {"column_title": "Документов", "decimals": 0, "number_separators": ", "},
            },
        },
        "sizeX": 12,
        "sizeY": 6,
        "row": 3,
        "col": 5,
        "tab": "archive",
        "metabase-field-filters": ff,
        "click_behavior": build_drill([("semd_type_filter", "Код СЭМД")]),
    }
    idx = next(i for i, c in enumerate(dash["cards"]) if c.get("name") == "Всего клиник" and c.get("tab") == "archive")
    dash["cards"].insert(idx + 1, card)


def fix_client_sql(query: str) -> str:
    query = query.replace("[[AND {{client_semd_code_name}}]]", "[[AND {{client_document_type}}]]")
    query = query.replace("{{clinic_jid}}", "{{client_jid}}")
    return query.replace("clinic_jid = {{client_jid}}", "clinic_jid::text = {{client_jid}}")


def apply_client_dashboards(path: Path) -> bool:
    if not path.exists():
        return False
    dash = json.loads(path.read_text(encoding="utf-8"))
    for card in dash.get("cards", []):
        filters = card.get("metabase-field-filters") or {}
        doc_type = filters.get("client_document_type")
        if isinstance(doc_type, dict) and doc_type.get("field_name") == "semd_code_name":
            doc_type["field_name"] = "semd_label"
        if card.get("name") == "Динамика статусов по дням" and card.get("dataset_query", {}).get("type") == "native":
            card["dataset_query"]["native"]["query"] = CLIENT_STATUS_BY_DAY_QUERY
            card["description"] = (
                "Stacked bar: Успешно зарегистрирован / Ошибка асинхронного ответа РЭМД / "
                "Ошибка связи / В обработке по дням (текущий статус документа)."
            )
        dq = card.get("dataset_query", {})
        if dq.get("type") == "native":
            dq["native"]["query"] = fix_client_sql(fix_sql(dq["native"]["query"]))
        fix_viz(card.get("visualization_settings") or {}, display=card.get("display", "table"))
    return write_json_if_changed(path, dash)


def main() -> None:
    dash = json.loads(DASH_01.read_text(encoding="utf-8"))
    apply_01(dash)
    if write_json_if_changed(DASH_01, dash):
        print(f"Updated {DASH_01}")

    for fname, mapping in RENAME_OTHER.items():
        path = ROOT / "metabase_dashboards" / fname
        if apply_renames(path, mapping):
            print(f"Updated {path}")

    for client_file in ("07_client_service.json", "08_client_bianalytic.json"):
        client_path = ROOT / "metabase_dashboards" / client_file
        if apply_client_dashboards(client_path):
            print(f"Updated metabase_dashboards/{client_file}")

    archive_path = ROOT / "metabase_dashboards" / "06_semd_archive.json"
    if archive_path.exists():
        archive_path.unlink()
        print(f"Deleted {archive_path}")


if __name__ == "__main__":
    main()
