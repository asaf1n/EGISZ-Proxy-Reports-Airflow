#!/usr/bin/env python3
"""Apply Metabase dashboard plan: fixes, renames, QB archive, drill-through."""
from __future__ import annotations

import json
import re
import sys
from copy import deepcopy
from contextlib import suppress
from pathlib import Path

# Запускается под PowerShell (cp1251-консоль); печатаем в UTF-8 во избежание падений
# на символах вне cp1251 в именах карточек.
with suppress(Exception):  # pragma: no cover
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

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
    "Ошибки ИЭМК": "#8CD17D",
    "Ошибки ФРЛЛО": "#D7B5A6",
    "Прочие": "#BAB0AC",
}


def _nsi_error_label(description: str) -> str:
    """Наименование типа из описания ФНСИ — та же нормализация, что в сиде правил:
    плейсхолдеры значений в квадратных скобках снимаются, пробелы схлопываются."""
    return re.sub(r"\s{2,}", " ", re.sub(r"\s*\[[^\]]*\]", "", description)).strip()


def error_type_color_map() -> dict[str, str]:
    """Map each canonical error type → its category color.

    Источник — те же сиды, что наполняют словарь: справочник ФНСИ в db/01_schema.sql
    (код → описание) и таблица правил в db/02_functions.sql (код → категория, при
    необходимости — своя формулировка). Палитра остаётся производной от словаря
    и не может разойтись с ним.
    """
    schema_sql = (ROOT / "db" / "01_schema.sql").read_text(encoding="utf-8")
    rules_sql = (ROOT / "db" / "02_functions.sql").read_text(encoding="utf-8")
    cats = "|".join(re.escape(c) for c in CATEGORY_COLORS)

    colors = {"Категория ошибки": "#BAB0AC"}
    colors.update(CATEGORY_COLORS)  # сами категории (внутреннее кольцо сунберста)

    # ('CODE', <id>, '<описание>', '<контур>') — сид dim_nsi_error_code.
    nsi = {
        code: descr.replace("''", "'")
        for code, descr in re.findall(r"\('([A-Z0-9_.]+)',\s*\d+,\s*'((?:[^']|'')+)',\s*'", schema_sql)
    }
    # ('CODE', '<категория>', NULL|'<формулировка>') — курируемая часть яруса 2.
    for code, cat, label in re.findall(
        r"\('([A-Z0-9_.]+)',\s*'(" + cats + r")',\s*(NULL|'(?:[^']|'')+')\)", rules_sql
    ):
        text = label[1:-1].replace("''", "'") if label != "NULL" else _nsi_error_label(nsi.get(code, ""))
        if text:
            colors[text] = CATEGORY_COLORS[cat]
    # ...'<тип>', '<категория>') — литеральные правила текстовых ярусов и контура ИЭМК.
    for label, cat in re.findall(r"'((?:[^']|'')+)',\s*'(" + cats + r")'\s*\)", rules_sql):
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

# Карточки, снятые с дашборда целиком (не переименованные). «Ступени обработки» описывала
# только backlog: документов с ответом в ней нет по построению, поэтому нижние шаги всегда
# 100% и воронка читалась как «зарегистрированы за 5 минут» — ровно наоборот. Её заменяет
# воронка процесса по всему отправленному корпусу.
RETIRED_CARD_NAMES = frozenset({
    "Ступени обработки",
    "Сегменты ожидания",
    # После вывода утилизированных из аналитики общий итог совпал с «В обработке»,
    # а доля утилизированных считалась от того же корпуса — обе плитки сняты.
    "Отправлено без ответа",
    "Доля без ответа, %",
})

# Модели, выведенные из обращения: импорт находит модель по имени, поэтому переименование
# оставляет прежнюю в коллекции дублем — она продолжает ссылаться на снесённый объект DWH
# и открывается ошибкой. Карты переименований для моделей нет, список ведётся явно.
RETIRED_MODEL_NAMES = frozenset({"Очередь без ответа"})

# Дашборды, выведенные из обращения (имя менялось или дашборд снят целиком).
RETIRED_DASHBOARD_NAMES = frozenset({"Архив СЭМД"})

DEFAULT_DWH_PERIOD = "thismonth"

# Топ-срезы вкладки «Отправленные» разложены по ступеням обработки: важно не только
# сколько документов ждёт ответа, но и как долго. Ранжирование — по терминальной
# ступени справочника, порог в имени карточки не зашит.
SENT_STATE_NO_RESPONSE_LABEL = "Ответ не получен (утилизирован)"
SENT_TABLE_NAME_PENDING = "Документы в обработке"
SENT_TABLE_NAME_NO_RESPONSE = f"Документы: {SENT_STATE_NO_RESPONSE_LABEL.lower()}"
SENT_UNDELIVERED_TO_CLINIC_NAME = "Недоставленные в клинику (ошибка связи шлюз-МО)"
SENT_UNDELIVERED_TO_CLINIC_DETAIL_NAME = (
    "Документы: недоставленные в клинику (ошибка связи шлюз-МО)"
)

SENT_STUCK_TOP_CLINICS_NAME = "Ожидание ответа по клиникам и ступеням"
SENT_STUCK_TOP_SEMD_NAME = "Ожидание ответа по типам СЭМД и ступеням"

# Переименования, ещё НЕ применённые на целевых контурах. Карта не архив: как только
# прогон импорта прошёл везде, запись отсюда убирается — прежнее имя уже не встретится,
# а копить цепочки «имя → имя → имя» значит хранить историю в рабочем коде.
# Снятые с дашборда карточки живут не здесь, а в RETIRED_CARD_NAMES.
RENAME_01 = {
    "Без ответа": SENT_STATE_NO_RESPONSE_LABEL,
    "Документы без ответа": SENT_TABLE_NAME_NO_RESPONSE,
    "Объём документов по дням": "Динамика документов по дням",
}

# Переименования по остальным дашбордам — так же только не применённые. Пусто: всё,
# что было, уже разошлось по контурам.
RENAME_OTHER: dict[str, dict[str, str]] = {}

# Канонические цвета состояний документа. «В обработке» — светло-синий (документ ещё в
# работе), «Без ответа» — светло-серый (ответа уже не ожидается; выводится только на
# вкладке «Отправленные», в общих распределениях не участвует).
STATUS_DETAIL_COLORS: dict[str, dict[str, str]] = {
    "Успешно зарегистрирован": {"color": "#84BB4C"},
    "Ошибка асинхронного ответа РЭМД": {"color": "#A989C5"},
    "Ошибка связи": {"color": "#F2994A"},
    "В обработке": {"color": "#A6C8E8"},
    "Без ответа": {"color": "#C8CDD5"},
}

DOCUMENTS_MODEL_REF = "Документы"
ERROR_BREAKDOWN_MODEL_REF = "Разбивка ошибок"
SENT_MODEL_REF = "Отправленные"

# Карточки без дрилла (агрегаты-рейтинги без естественного грейна для строки).
DRILL_BY_NAME: dict[str, list[tuple[str, str]]] = {
    "Топ по типу ошибки": [],
    "Топ категорий и типов ошибки": [],
}

ModelDrillMapping = tuple[str, str] | tuple[str, str, str]

# Дрилл из строки ведёт СРАЗУ в модель (не на вкладку «Архив»): строка передаёт свой грейн
# точным равенством + активные фильтры дашборда через metabase-model-drill-params. Тип ошибки —
# через CONTAINS по полному списку error_types (документ с несколькими ошибками не теряется).
MODEL_DRILL_BY_NAME: dict[str, list[ModelDrillMapping]] = {
    "Последние операции": [("clinic_label", "Клиника")],
    "Статусы за период": [("status_detail_label", "Статус")],
    "Транзакции по дням и статусам": [("status_detail_label", "Статус")],
    "РЭМД vs связь": [("status_detail_label", "Статус")],
    "Объём по клиникам": [("clinic_jid", "JID Клиники")],
    "Успешность по клиникам": [("clinic_jid", "JID Клиники")],
    "Объём ошибок по клиникам": [("clinic_jid", "JID Клиники")],
    "Топ типов СЭМД по документам": [("semd_label", "СЭМД")],
    "Успешность по типам СЭМД": [("semd_label", "СЭМД")],
    "Топ типов СЭМД по ошибкам": [("semd_label", "СЭМД")],
    "Топ типов СЭМД по видам ошибки": [("semd_code", "СЭМД")],
    # Сегмент полосы — это пара «объект × ступень», поэтому в модель уносится и ступень:
    # без неё клик по хвосту лестницы открывал бы весь корпус клиники.
    SENT_STUCK_TOP_CLINICS_NAME: [
        ("clinic_jid", "JID Клиники"),
        ("pending_segment_label", "Ступень обработки"),
    ],
    SENT_STUCK_TOP_SEMD_NAME: [
        ("semd_code", "Код СЭМД"),
        ("pending_segment_label", "Ступень обработки"),
    ],
    "Ошибки: тип × клиника": [
        ("error_types", "Тип ошибки", "contains"),
        ("clinic_jid", "JID Клиники"),
    ],
}

# Целевая модель дрилла по карточке (по умолчанию — «Документы»).
MODEL_DRILL_TARGET_BY_NAME: dict[str, str] = {
    "Топ типов СЭМД по ошибкам": ERROR_BREAKDOWN_MODEL_REF,
    "Топ типов СЭМД по видам ошибки": ERROR_BREAKDOWN_MODEL_REF,
    SENT_STUCK_TOP_CLINICS_NAME: SENT_MODEL_REF,
    SENT_STUCK_TOP_SEMD_NAME: SENT_MODEL_REF,
}

# Активные фильтры дашборда, переносимые в модель (без измерения-грейна самой строки).
MODEL_DRILL_DASHBOARD_PARAMS: dict[str, list[str]] = {
    "Последние операции": ["ips_date", "semd_type", "status"],
    "Статусы за период": ["ips_date", "semd_type", "jid"],
    "Транзакции по дням и статусам": ["ips_date", "semd_type", "jid"],
    "РЭМД vs связь": ["ips_date", "semd_type", "jid"],
    "Объём по клиникам": ["ips_date", "semd_type", "status"],
    "Успешность по клиникам": ["ips_date", "semd_type", "status"],
    "Объём ошибок по клиникам": ["ips_date", "semd_type", "status"],
    "Топ типов СЭМД по документам": ["ips_date", "jid", "status"],
    "Успешность по типам СЭМД": ["ips_date", "jid", "status"],
    "Топ типов СЭМД по ошибкам": ["ips_date", "jid"],
    "Топ типов СЭМД по видам ошибки": ["ips_date", "jid"],
    # pending_segment не переносится: ступень теперь измерение самой строки (см. MODEL_DRILL_BY_NAME).
    SENT_STUCK_TOP_CLINICS_NAME: ["ips_date", "semd_type"],
    SENT_STUCK_TOP_SEMD_NAME: ["ips_date", "jid"],
    "Ошибки: тип × клиника": ["ips_date", "semd_type", "jid", "status"],
}

# Поля модели для переноса дашборд-фильтра (на грейне модели): JID/СЭМД — по label.
MODEL_PARAM_FIELDS: dict[str, dict[str, str]] = {
    DOCUMENTS_MODEL_REF: {
        "ips_date": "ips_date", "semd_type": "semd_label",
        "jid": "clinic_label", "status": "status_detail_label",
    },
    ERROR_BREAKDOWN_MODEL_REF: {
        "ips_date": "ips_date", "semd_type": "semd_label", "jid": "clinic_label",
    },
    SENT_MODEL_REF: {
        "ips_date": "first_sent_at", "semd_type": "semd_label",
        "jid": "clinic_label", "pending_segment": "pending_segment",
    },
}

DOCUMENTS_PARAM_TARGETS = {
    "ips_date": {"model_ref": "Документы", "field_name": "ips_date"},
    "jid": {"model_ref": "Документы", "field_name": "clinic_label"},
    "semd_type": {"model_ref": "Документы", "field_name": "semd_label"},
    "status": {"model_ref": "Документы", "field_name": "status_detail_label"},
    "local_uid": {"model_ref": "Документы", "field_name": "semd_local_uid"},
    "relates_to": {"model_ref": "Документы", "field_name": "relates_to_msgid"},
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
    {"enabled": True, "name": "Типы ошибки"},
    {"enabled": True, "name": "Рег. Номер РЭМД"},
    {"enabled": True, "name": "Связанное сообщение"},
    {"enabled": True, "name": "LOGID"},
    {"enabled": False, "name": "dwh_id"},
    {"enabled": False, "name": "OID Клиники"},
    {"enabled": False, "name": "СЭМД CreateDate"},
    {"enabled": False, "name": "MSGID"},
    {"enabled": False, "name": "День"},
]

# Порядок колонок и ширины сведены с живого прода (bi.sdsys.ru): «Типы ошибки» подняты
# к идентификаторам документа, «Host Клиники» уведён в конец.
LATEST_OPERATIONS_TABLE_COLUMNS = [
    {"enabled": True, "name": "Дата обработки"},
    {"enabled": True, "name": "Статус"},
    {"enabled": True, "name": "Клиника"},
    {"enabled": True, "name": "СЭМД"},
    {"enabled": True, "name": "Типы ошибки"},
    {"enabled": True, "name": "localUid СЭМД"},
    {"enabled": True, "name": "Рег. Номер РЭМД"},
    {"enabled": True, "name": "Host Клиники (ГОСТ VPN)"},
]

LATEST_OPERATIONS_QUERY_FIELDS = [
    ["field", "Документы:processed_at", None],
    ["field", "Документы:status_detail_label", None],
    ["field", "Документы:clinic_label", None],
    ["field", "Документы:clinic_host", None],
    ["field", "Документы:semd_label", None],
    ["field", "Документы:semd_local_uid", None],
    ["field", "Документы:semd_emdr_id", None],
    ["field", "Документы:error_types", None],
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
    '["name","Типы ошибки"]': {"column_title": "Типы ошибки", "text_style": "wrap"},
    '["name","Host Клиники (ГОСТ VPN)"]': {"column_title": "Host"},
}

DOCUMENT_FILTERS = (
    "[[AND {{dwh_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] "
    "[[AND {{local_uid}}]] [[AND {{relates_to}}]] [[AND {{emdr_id}}]] "
    "[[AND {{status}}]] [[AND {{log_id}}]]"
)

DOCUMENT_VOLUME_BY_DAY_QUERY = (
    "SELECT first_sent_at::date AS \"Дата\", "
    "COUNT(DISTINCT dwh_id)::bigint AS \"Документов\" "
    "FROM public.rpt_documents "
    "WHERE first_sent_at::date IS NOT NULL "
    "[[AND {{ips_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] "
    "[[AND {{local_uid}}]] [[AND {{relates_to}}]] [[AND {{emdr_id}}]] "
    "[[AND {{status}}]] [[AND {{log_id}}]] "
    "GROUP BY first_sent_at::date ORDER BY first_sent_at::date ASC"
)

# Распределение по статусам: три исхода плюс «В обработке». «Без ответа» исключено —
# ответа по этим документам уже не ожидается, они разбираются на вкладке «Отправленные»
# (см. README §«Учёт отправленных»).
TRANSACTIONS_BY_DAY_STATUS_QUERY = (
    "SELECT processed_day AS \"Дата\", status_detail_label AS \"Статус\", "
    "COUNT(DISTINCT dwh_id)::bigint AS \"Документов\" "
    "FROM public.rpt_documents WHERE status_detail <> 'no_response' "
    "[[AND {{dwh_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] "
    "GROUP BY processed_day, status_detail_label, status_detail_sort "
    "ORDER BY processed_day, status_detail_sort"
)

# Стэк долей состояний за день (сумма = 100%): три исхода плюс «В обработке». Знаменатель —
# тот же отображаемый корпус, поэтому доли складываются в 100. «Без ответа» исключено:
# ответа по ним уже не ожидается, разбор — на вкладке «Отправленные».
# Счётный ряд «Всего» на второй оси визуально спорит с процентными рядами, поэтому
# объём вынесен в отдельные карточки.
CLIENT_STATUS_BY_DAY_QUERY = (
    "SELECT ips_date::date AS \"Дата\", "
    "ROUND(100.0 * COUNT(DISTINCT dwh_id) FILTER (WHERE status = 'success') "
    "/ NULLIF(COUNT(DISTINCT dwh_id), 0), 1) AS \"Успешно, %\", "
    "ROUND(100.0 * COUNT(DISTINCT dwh_id) FILTER (WHERE status = 'async_error') "
    "/ NULLIF(COUNT(DISTINCT dwh_id), 0), 1) AS \"Async ошибки, %\", "
    "ROUND(100.0 * COUNT(DISTINCT dwh_id) FILTER (WHERE status = 'network_error') "
    "/ NULLIF(COUNT(DISTINCT dwh_id), 0), 1) AS \"Сетевые ошибки, %\", "
    "ROUND(100.0 * COUNT(DISTINCT dwh_id) FILTER (WHERE status_detail = 'pending') "
    "/ NULLIF(COUNT(DISTINCT dwh_id), 0), 1) AS \"В обработке, %\" "
    "FROM public.rpt_documents "
    "WHERE 1=1 [[AND {{clinic_label}}]] [[AND clinic_jid::text = {{client_jid}}]] "
    "AND status_detail <> 'no_response' [[AND {{ips_date}}]] [[AND {{client_document_type}}]] "
    "GROUP BY ips_date::date ORDER BY ips_date::date"
)

# Отказы по часам — доля от корпуса с ответом РЭМД за час (успех+ошибка). Это метрика
# отношения, а не распределение: «В обработке» — ещё не исход и в знаменатель не входит,
# поэтому отсечка идёт по status, а не по status_detail (см. README §«Учёт отправленных»).
SERVICE_REFUSALS_BY_HOUR_QUERY = (
    "SELECT date_trunc('hour', ips_date) AS \"Час\", "
    "ROUND(100.0 * COUNT(DISTINCT dwh_id) FILTER (WHERE status = 'network_error') "
    "/ NULLIF(COUNT(DISTINCT dwh_id), 0), 1) AS \"Ошибка связи, %\", "
    "ROUND(100.0 * COUNT(DISTINCT dwh_id) FILTER (WHERE status = 'async_error') "
    "/ NULLIF(COUNT(DISTINCT dwh_id), 0), 1) AS \"Ошибка асинхронного ответа РЭМД, %\" "
    "FROM public.rpt_documents WHERE status <> 'sent' "
    "[[AND {{ips_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] "
    "GROUP BY 1 ORDER BY 1"
)

LATEST_OPERATIONS_QUERY = (
    "SELECT ips_date AS \"Дата обработки\", status_detail_label AS \"Статус\", "
    "clinic_label AS \"Клиника\", clinic_host AS \"Host Клиники (ГОСТ VPN)\", "
    "semd_label AS \"СЭМД\", semd_local_uid AS \"localUid СЭМД\", "
    "semd_emdr_id AS \"Рег. Номер РЭМД\", error_types AS \"Типы ошибки\" "
    "FROM public.rpt_documents WHERE 1=1 "
    "[[AND {{ips_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] [[AND {{status}}]] "
    "ORDER BY ips_date DESC LIMIT 50"
)

STATUS_PERIOD_QUERY = (
    "SELECT status_detail_label AS \"Статус\", COUNT(DISTINCT dwh_id)::bigint AS \"Документов\" "
    "FROM public.rpt_documents WHERE status_detail <> 'no_response' "
    "[[AND {{ips_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] "
    "GROUP BY status_detail_label, status_detail_sort ORDER BY status_detail_sort"
)

DOCUMENTS_FILTER_TEMPLATE_TAGS = {
    "ips_date": {
        "widget-type": "date/all-options",
        "display-name": "По дате «Обработано»",
        "id": "f2000099-0099-4099-8099-000000000001",
        "name": "ips_date",
        "type": "dimension",
    },
    "semd_type": {
        "widget-type": "string/=",
        "display-name": "Код СЭМД",
        "id": "f2000099-0099-4099-8099-000000000010",
        "name": "semd_type",
        "required": False,
        "type": "dimension",
    },
    "jid": {
        "widget-type": "string/=",
        "display-name": "JID Клиники",
        "id": "f2000099-0099-4099-8099-000000000011",
        "name": "jid",
        "required": False,
        "type": "dimension",
    },
    "status": {
        "widget-type": "string/=",
        "display-name": "Статус",
        "id": "f2000099-0099-4099-8099-000000000012",
        "name": "status",
        "required": False,
        "type": "dimension",
    },
}

DOCUMENTS_FILTER_FIELD_FILTERS = {
    "ips_date": {"table_ref": "public.rpt_documents", "field_name": "ips_date"},
    "semd_type": {"table_ref": "public.rpt_documents", "field_name": "semd_label"},
    "jid": {"table_ref": "public.rpt_documents", "field_name": "clinic_label"},
    "status": {"table_ref": "public.rpt_documents", "field_name": "status_detail_label"},
}

CLIENT_JID_PARAM_ID = "07c00000-0000-4000-8000-000000000003"
CLIENT_CLINIC_PARAM_ID = "07c00000-0000-4000-8000-000000000005"

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

# «Код отказа» — код, с которым пришёл отказ: по нему обращаются в СТП ЕГИСЗ и ищут проверку
# в методической документации. Берётся из витрины (там он уже разложен по типу), поэтому
# обращения к словарю правил не нужно. У типа, распознанного по формулировке отказа, своей
# мнемоники в классификаторе нет — витрина отдаёт зонтичную (VALIDATION_ERROR /
# RUNTIME_ERROR), под которой отказ пришёл.
#
# Рядом выводится справочник кода: регистрационный путь отвечает мнемоникой ФНСИ 305, контур
# ИЭМК — errorCode IHE XDS, шлюз — синтетическим кодом. Без указания справочника коды разных
# контуров в одной колонке неразличимы, а у ИЭМК и шлюза она была бы просто пустой.
ERROR_TYPE_CLINIC_QUERY = (
    "WITH period_docs AS ( SELECT dwh_id, clinic_jid::text AS clinic_jid "
    "FROM public.rpt_documents "
    "WHERE status IN ('success', 'async_error', 'network_error') "
    "AND NULLIF(TRIM(clinic_jid::text), '') IS NOT NULL "
    "[[AND {{dwh_date}}]] [[AND {{jid}}]] [[AND {{semd_type}}]] ), "
    # Таблицу с field-фильтром нельзя алиасить: Metabase разворачивает {{error_type}}
    # в полное имя public.rpt_error_breakdown.error_type, и алиас ломает ссылку.
    "base AS ( SELECT "
    "COALESCE(NULLIF(TRIM(rpt_error_breakdown.error_type), ''), 'Неизвестная ошибка') AS error_type, "
    "COALESCE(NULLIF(TRIM(rpt_error_breakdown.nsi_error_code), ''), "
    "NULLIF(TRIM(rpt_error_breakdown.error_code), ''), '—') AS error_code, "
    "COALESCE(NULLIF(TRIM(rpt_error_breakdown.code_namespace), ''), '—') AS code_namespace, "
    "rpt_error_breakdown.clinic_label AS clinic_label, "
    "rpt_error_breakdown.clinic_jid::text AS clinic_jid, rpt_error_breakdown.dwh_id "
    "FROM public.rpt_error_breakdown "
    "INNER JOIN period_docs pd ON pd.dwh_id = rpt_error_breakdown.dwh_id "
    "WHERE COALESCE(NULLIF(TRIM(rpt_error_breakdown.error_type), ''), '') <> '' "
    "[[AND {{error_type}}]] ), "
    "error_clinic AS ( SELECT error_type, error_code, code_namespace, clinic_label, clinic_jid, "
    "COUNT(DISTINCT dwh_id)::bigint AS doc_count "
    "FROM base GROUP BY 1, 2, 3, 4, 5 ), "
    "clinic_totals AS ( SELECT clinic_jid, COUNT(DISTINCT dwh_id)::numeric AS total_docs "
    "FROM period_docs GROUP BY clinic_jid ) "
    'SELECT ec.error_type AS "Тип ошибки", ec.clinic_label AS "Клиника", '
    'ec.error_code AS "Код отказа", ec.code_namespace AS "Справочник", '
    'ec.clinic_jid AS "JID Клиники", ec.doc_count AS "Документов", '
    'ROUND(100.0 * ec.doc_count / NULLIF(ct.total_docs, 0), 1) AS "% ошибок" '
    "FROM error_clinic ec "
    "JOIN clinic_totals ct ON ct.clinic_jid = ec.clinic_jid "
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
    # Плоский градиент от минимума к максимуму столбца: min_type/max_type = null →
    # шкала растягивается по фактическому диапазону данных, а не по фиксированным 0..25.
    "table.column_formatting": [
        {
            "colors": ["#10B981", "#F59E0B", "#EF4444"],
            "columns": ["Доля ошибок, %"],
            "max_type": None,
            "min_type": None,
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
# Два знаменателя на разных грейнах: «% ошибок» — доля среди ошибочных документов
# (грейн rpt_error_breakdown), «% обработанных» — доля среди всех документов с ответом
# РЭМД (успех+ошибка, грейн rpt_documents). Поэтому база — period_docs из rpt_documents,
# к которой джойнится rpt_error_breakdown (без алиаса: field-фильтры разворачиваются в
# полное имя таблицы, см. ERROR_TYPE_CLINIC_QUERY). Фильтры срез — на грейне документа.
TOP_ERROR_TYPE_QUERY = (
    "WITH period_docs AS ( "
    "SELECT dwh_id FROM public.rpt_documents "
    "WHERE status IN ('success', 'async_error', 'network_error') "
    "[[AND {{ips_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] ), "
    "eb AS ( SELECT "
    "COALESCE(NULLIF(TRIM(rpt_error_breakdown.error_category), ''), 'Прочие') AS cat, "
    "COALESCE(NULLIF(TRIM(rpt_error_breakdown.error_type), ''), 'Неизвестная ошибка') AS typ, "
    "COALESCE(NULLIF(TRIM(rpt_error_breakdown.responsibility), ''), 'смешанная') AS resp, "
    "rpt_error_breakdown.is_retryable AS retryable, rpt_error_breakdown.dwh_id AS dwh_id "
    "FROM public.rpt_error_breakdown "
    "INNER JOIN period_docs pd ON pd.dwh_id = rpt_error_breakdown.dwh_id "
    "WHERE COALESCE(NULLIF(TRIM(rpt_error_breakdown.error_type), ''), '') <> '' ), "
    "totals AS ( SELECT "
    "(SELECT COUNT(DISTINCT dwh_id) FROM eb)::numeric AS total_err, "
    "(SELECT COUNT(DISTINCT dwh_id) FROM period_docs)::numeric AS total_final ), "
    "per_type AS ( SELECT cat, typ, resp, retryable, COUNT(DISTINCT dwh_id)::bigint AS cnt "
    "FROM eb GROUP BY 1, 2, 3, 4 ) "
    'SELECT cat AS "Категория ошибки", typ AS "Тип ошибки", '
    'resp AS "Зона ответственности", '
    'CASE WHEN retryable THEN \'да\' ELSE \'нет\' END AS "Устраняется повтором", '
    'cnt AS "Документов", '
    'ROUND(100.0 * cnt / NULLIF((SELECT total_err FROM totals), 0), 1) AS "% ошибок", '
    'ROUND(100.0 * cnt / NULLIF((SELECT total_final FROM totals), 0), 1) AS "% обработанных" '
    "FROM per_type ORDER BY cnt DESC"
)

TOP_SEMD_BY_ERROR_KIND_QUERY = (
    "WITH base AS ( "
    "SELECT COALESCE(NULLIF(TRIM(semd_code), ''), 'Неизвестно') AS t, "
    "COALESCE(NULLIF(TRIM(error_type), ''), 'Неизвестная ошибка') AS k, "
    "dwh_id AS doc FROM public.rpt_error_breakdown "
    "WHERE COALESCE(NULLIF(TRIM(semd_code), ''), '') <> '' "
    "AND COALESCE(NULLIF(TRIM(error_type), ''), '') <> '' "
    "[[AND {{ips_date}}]] [[AND {{jid}}]] [[AND {{semd_type}}]] ), "
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
    {"enabled": True, "name": "Код отказа"},
    {"enabled": True, "name": "Справочник"},
    {"enabled": False, "name": "JID Клиники"},
    {"enabled": True, "name": "Документов"},
    {"enabled": True, "name": "% ошибок"},
]

ERROR_TYPE_CLINIC_COLUMN_WIDTHS = [360, 200, 280, 120, 88, 96, 104]

SUCCESS_CLINIC_COLUMN_WIDTHS = [88, 300, 88, 88]
SUCCESS_SEMD_COLUMN_WIDTHS = [88, 120, 88, 88, 88]
CLINIC_VOLUME_COLUMN_WIDTHS = [186]
TOP_SEMD_BY_ERRORS_COLUMN_WIDTHS = [396, 86]
TOP_ERROR_TYPE_COLUMN_WIDTHS = [350, 96, 96, 128]
TOP_ERROR_TYPE_TABLE_COLUMNS = [
    {"enabled": True, "name": "Тип ошибки"},
    {"enabled": True, "name": "Документов"},
    {"enabled": True, "name": "% ошибок"},
    {"enabled": True, "name": "% обработанных"},
    {"enabled": True, "name": "Категория ошибки"},
    {"enabled": True, "name": "Зона ответственности"},
    {"enabled": True, "name": "Устраняется повтором"},
]
TOP_ERROR_TYPE_COLUMN_FORMATTING = [
    {
        "columns": ["% ошибок"],
        "type": "range",
        "colors": [
            "hsla(89, 48%, 40%, 1)",
            "transparent",
            "hsla(358, 71%, 62%, 1)",
        ],
        "min_type": "custom",
        "max_type": None,
        "min_value": -100,
        "max_value": 100,
    },
    # Доля от всех обработанных обычно мала (0..~15 %): сплошной градиент зелёный→красный
    # на фиксированной шкале 0..15, чтобы заливка не «схлопывалась» в один тон на редких типах.
    {
        "columns": ["% обработанных"],
        "type": "range",
        "colors": ["#84BB4C", "#FBBF24", "#DC2626"],
        "min_type": "custom",
        "max_type": "custom",
        "min_value": 0,
        "max_value": 15,
    },
]

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

SENT_TABLE_COLUMNS = [
    {"enabled": True, "name": "Состояние отправки"},
    {"enabled": True, "name": "Ступень обработки"},
    {"enabled": True, "name": "Суток с отправки"},
    {"enabled": True, "name": "Подач в ЕГИСЗ"},
    {"enabled": True, "name": "Дата отправки"},
    {"enabled": True, "name": "Клиника"},
    {"enabled": True, "name": "Код СЭМД"},
    {"enabled": True, "name": "Наименование СЭМД"},
    {"enabled": False, "name": "JID Клиники"},
    {"enabled": True, "name": "localUid СЭМД"},
]

# Число подач берётся из реестра шлюза: повторная отправка не меняет localUid, поэтому
# счётчик показывает, сколько раз документ уже отправляли до текущего момента.
# Без алиаса таблицы: фильтры-поля Metabase разворачиваются в "public"."rpt_documents_sent".<col>.
# Разбор идёт двумя таблицами по состоянию отправки: «В обработке» — рабочая очередь,
# по ней ответ ещё ждут; «Без ответа» — терминальное состояние, там уже разбор инцидента.
# Смешивать их в одном списке значит прятать вторую под первой: свежие отправки
# вытесняют застрявшие, а сортировка одна на обе.


def _sent_table_query(state: str, order: str) -> str:
    return (
        'SELECT semd_local_uid AS "localUid СЭМД", semd_code AS "Код СЭМД", '
        'semd_name AS "Наименование СЭМД", clinic_jid::text AS "JID Клиники", '
        'clinic_label AS "Клиника", first_sent_at AS "Дата отправки", '
        'pending_days AS "Суток с отправки", attempt_count AS "Подач в ЕГИСЗ", '
        'sent_state_label AS "Состояние отправки", '
        'pending_segment_label AS "Ступень обработки" '
        "FROM public.rpt_documents_sent "
        f"WHERE sent_state = '{state}' "
        "[[AND {{ips_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] "
        "[[AND {{local_uid}}]] [[AND {{pending_segment}}]] "
        f"ORDER BY {order} LIMIT 200"
    )


# «В обработке» читают от свежих: интерес — что происходит сейчас. «Без ответа» —
# от самых старых: там разбирают давно застрявшее.
SENT_TABLE_PENDING_QUERY = _sent_table_query("pending", "first_sent_at DESC NULLS LAST")
SENT_TABLE_NO_RESPONSE_QUERY = _sent_table_query("no_response", "first_sent_at ASC NULLS LAST")

# Вкладка «Отправленные» целиком строится на rpt_documents_sent: состояние отправки и
# ступень обработки приходят из справочников (dim_sent_state, dim_pending_segments),
# поэтому карточки не содержат ни порогов, ни подписей — только отбор по состоянию.
SENT_FILTERS = (
    "[[AND {{ips_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] "
    "[[AND {{local_uid}}]] [[AND {{pending_segment}}]]"
)

SENT_FILTER_TEMPLATE_TAGS = {
    "ips_date": {
        "widget-type": "date/all-options",
        "display-name": "По дате «Обработано»",
        "id": "f2000099-0099-4099-8099-000000000001",
        "name": "ips_date",
        "type": "dimension",
    },
    "semd_type": {
        "widget-type": "string/=",
        "display-name": "Код СЭМД",
        "id": "f2000099-0099-4099-8099-000000000010",
        "name": "semd_type",
        "required": False,
        "type": "dimension",
    },
    "jid": {
        "widget-type": "string/=",
        "display-name": "JID Клиники",
        "id": "f2000099-0099-4099-8099-000000000011",
        "name": "jid",
        "required": False,
        "type": "dimension",
    },
    "local_uid": {
        "widget-type": "string/=",
        "display-name": "localUid СЭМД",
        "id": "f2000099-0099-4099-8099-000000000013",
        "name": "local_uid",
        "required": False,
        "type": "dimension",
    },
    "pending_segment": {
        "widget-type": "string/=",
        "display-name": "Ступень обработки",
        "id": "f2000099-0099-4099-8099-000000000014",
        "name": "pending_segment",
        "required": False,
        "type": "dimension",
    },
}

SENT_FIELD_FILTERS = {
    "ips_date": {"table_ref": "public.rpt_documents_sent", "field_name": "first_sent_at"},
    "semd_type": {"table_ref": "public.rpt_documents_sent", "field_name": "semd_label"},
    "jid": {"table_ref": "public.rpt_documents_sent", "field_name": "clinic_label"},
    "local_uid": {"table_ref": "public.rpt_documents_sent", "field_name": "semd_local_uid"},
    "pending_segment": {
        "table_ref": "public.rpt_documents_sent",
        "field_name": "pending_segment_label",
    },
}


def _sent_scalar_query(where: str) -> str:
    return (
        'SELECT COUNT(DISTINCT semd_local_uid)::bigint AS "Документов" '
        "FROM public.rpt_documents_sent "
        f"WHERE {where} {SENT_FILTERS}"
    )


SENT_PENDING_QUERY = _sent_scalar_query("sent_state = 'pending'")
SENT_NO_RESPONSE_QUERY = _sent_scalar_query("sent_state = 'no_response'")

# Ранжирование срезов — по числу документов в состоянии «Без ответа»: это терминальная
# ступень справочника, ответа по ним уже не ждут. Порог в карточке не объявляется —
# ужесточение лестницы делается правкой dim_pending_segments и подхватывается само.
SENT_STALE_RANK = (
    "COUNT(DISTINCT semd_local_uid) FILTER ("
    "WHERE pending_segment_sort = ("
    "SELECT MAX(sort_order) FROM public.dim_pending_segments WHERE NOT is_no_response"
    ")) AS stale_docs, "
    "COUNT(DISTINCT semd_local_uid) AS total_docs"
)

# Срез клиник разложен по ступеням обработки: одна полоса на клинику, сегменты — возраст
# ожидания. Разрез отвечает на вопрос поддержки не «у кого много отправлено», а «у кого
# ожидание перевалило за лестницу»; прежний порог «дольше суток» этого не показывал —
# под него попадало 90% корпуса.
SENT_STUCK_TOP_CLINICS_QUERY = (
    "WITH sent AS ( SELECT clinic_jid, clinic_label, semd_local_uid, sent_state, "
    "pending_segment_label, pending_segment_sort "
    "FROM public.rpt_documents_sent "
    # Утилизированные выведены из аналитики: ответа по ним не будет, и в срезе
    # ожидания они изображали бы очередь, которой нет.
    f"WHERE sent_state = 'pending' {SENT_FILTERS} ), "
    f"ranked AS ( SELECT clinic_jid, clinic_label, {SENT_STALE_RANK} "
    "FROM sent GROUP BY 1, 2 ORDER BY stale_docs DESC, total_docs DESC LIMIT 15 ) "
    'SELECT r.clinic_jid::text AS "JID Клиники", r.clinic_label AS "Клиника", '
    's.pending_segment_label AS "Ступень обработки", '
    'COUNT(DISTINCT s.semd_local_uid)::bigint AS "Документов" '
    "FROM ranked r JOIN sent s ON s.clinic_jid = r.clinic_jid "
    "GROUP BY 1, 2, 3, s.pending_segment_sort, r.stale_docs, r.total_docs "
    "ORDER BY r.stale_docs DESC, r.total_docs DESC, s.pending_segment_sort"
)

# Воронка процесса живёт на полном корпусе (rpt_documents), поэтому фильтры вкладки
# переносятся только те, что есть на этом грейне: ступень ожидания и localUid относятся
# к срезу ожидающих и к процессу регистрации неприменимы.
DOCUMENTS_FUNNEL_FILTERS = "[[AND {{ips_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]]"

SENT_REGISTRATION_FIELD_FILTERS = {
    "ips_date": {"table_ref": "public.rpt_documents", "field_name": "ips_date"},
    "semd_type": {"table_ref": "public.rpt_documents", "field_name": "semd_label"},
    "jid": {"table_ref": "public.rpt_documents", "field_name": "clinic_label"},
}

SENT_REGISTRATION_FUNNEL_NAME = "Скорость регистрации в РЭМД"
SENT_PENDING_FUNNEL_NAME = "В обработке на конец периода"

# Срез очереди на момент, а не «сейчас»: якорь — правая граница выбранного периода,
# то есть последняя активность, попавшая в фильтр. Документ считается ожидающим на этот
# момент, если он к нему уже отправлен и ответа на него ещё не было: либо ответа нет
# вовсе, либо он пришёл позже якоря. Возраст ожидания тоже отсчитывается от якоря —
# иначе смена периода двигала бы ступени, оставляя состав корпуса прежним.
SENT_PENDING_FUNNEL_QUERY = (
    "WITH anchor AS ( SELECT MAX(ips_date) AS ts FROM public.rpt_documents "
    "WHERE 1=1 [[AND {{ips_date}}]] ), "
    "pending AS ( SELECT d.dwh_id, "
    "EXTRACT(EPOCH FROM (a.ts - d.first_sent_at)) / 60.0 AS age_minutes "
    "FROM public.rpt_documents d CROSS JOIN anchor a "
    "WHERE a.ts IS NOT NULL AND d.first_sent_at IS NOT NULL AND d.first_sent_at <= a.ts "
    "AND ( d.delivery_seconds IS NULL "
    "OR d.first_sent_at + make_interval(secs => d.delivery_seconds) > a.ts ) "
    # Утилизированные выведены из аналитики целиком: на якорную дату они формально ещё
    # ждали, но ответа по ним не будет, и в очереди они изображали бы работу, которой нет.
    "AND COALESCE(d.sent_state, '') <> 'no_response' "
    "[[AND {{semd_type}}]] [[AND {{jid}}]] ) "
    "SELECT -1000 AS step_sort, 'В обработке' AS \"Этап\", "
    'COUNT(DISTINCT dwh_id)::bigint AS "Документов" FROM pending '
    "UNION ALL "
    "SELECT g.sort_order::int, 'ждали > ' || regexp_replace(g.label, '^до ', ''), "
    "COUNT(DISTINCT p.dwh_id) FILTER (WHERE p.age_minutes > g.max_age_minutes)::bigint "
    "FROM public.dim_pending_segments g CROSS JOIN pending p "
    "WHERE NOT g.is_no_response "
    "GROUP BY g.sort_order, g.label "
    "ORDER BY 1"
)

# Воронка процесса: весь отправленный корпус, а не только ожидающие. Шаг — сколько
# документов УЛОЖИЛИСЬ в срок, а лестница идёт от самого мягкого срока к самому жёсткому.
# Поэтому ряд убывает (воронка сужается), а метка «< 5 мин» описывает ровно то число,
# что под ней стоит. Обратный порядок — «ждали дольше N» — давал те же данные, но метка
# считалась от противного и читалась как доля успевших.
#
# delivery_seconds считается по журналу (ответ минус отправка) и пуст у ожидающих —
# в срок они не уложились ни на одной границе и в шаги не попадают.
SENT_REGISTRATION_FUNNEL_QUERY = (
    # Корпус воронки — документы, по которым ответ УЖЕ пришёл: только у них есть срок
    # регистрации. Ожидающие и терминальное «Без ответа» отсекаются самим условием
    # delivery_seconds IS NOT NULL: срока у них нет, и в знаменателе они притворялись бы
    # медленными, хотя первые ещё в очереди, а вторым ответа не будет вовсе.
    "WITH sent AS ( SELECT dwh_id, delivery_seconds "
    "FROM public.rpt_documents "
    "WHERE first_sent_at IS NOT NULL AND delivery_seconds IS NOT NULL "
    f"{DOCUMENTS_FUNNEL_FILTERS} ) "
    # База обязана идти первым шагом: ступени нумеруются отрицательно (мягкий срок →
    # жёсткий), поэтому нулевой сорт-ключ увёл бы её в конец воронки. Убрать базу нельзя:
    # часть документов регистрируется дольше самой мягкой ступени, и без неё первый шаг
    # молча стал бы стопроцентным, спрятав их.
    "SELECT -1000 AS step_sort, 'Получен ответ' AS \"Этап\", "
    'COUNT(DISTINCT dwh_id)::bigint AS "Документов" FROM sent '
    "UNION ALL "
    # Метка ступени из справочника («до 6 часов») сжимается до знака сравнения: восемь
    # подписей во всю ширину сетки иначе обрезаются до неразличимого.
    "SELECT -g.sort_order::int, regexp_replace(g.label, '^до ', '< '), "
    "COUNT(DISTINCT s.dwh_id) FILTER ("
    "WHERE s.delivery_seconds IS NOT NULL AND s.delivery_seconds <= g.max_age_minutes * 60"
    ")::bigint "
    "FROM public.dim_pending_segments g CROSS JOIN sent s "
    "WHERE NOT g.is_no_response "
    "GROUP BY g.sort_order, g.label "
    "ORDER BY 1"
)

SENT_STUCK_TOP_SEMD_QUERY = (
    "WITH sent AS ( SELECT semd_code, semd_local_uid, sent_state, "
    "pending_segment_label, pending_segment_sort "
    "FROM public.rpt_documents_sent "
    f"WHERE sent_state = 'pending' {SENT_FILTERS} ), "
    "typed AS ( SELECT COALESCE(NULLIF(TRIM(semd_code), ''), '(неизвестно)') AS semd_code, "
    "semd_local_uid, sent_state, pending_segment_label, pending_segment_sort FROM sent ), "
    f"ranked AS ( SELECT semd_code, {SENT_STALE_RANK} "
    "FROM typed GROUP BY 1 ORDER BY stale_docs DESC, total_docs DESC LIMIT 20 ) "
    'SELECT r.semd_code AS "Код СЭМД", '
    't.pending_segment_label AS "Ступень обработки", '
    'COUNT(DISTINCT t.semd_local_uid)::bigint AS "Документов" '
    "FROM ranked r JOIN typed t ON t.semd_code = r.semd_code "
    "GROUP BY 1, 2, t.pending_segment_sort, r.stale_docs, r.total_docs "
    "ORDER BY r.stale_docs DESC, r.total_docs DESC, t.pending_segment_sort"
)

SENT_TAB_QUERIES: dict[str, str] = {
    "В обработке": SENT_PENDING_QUERY,
    SENT_STATE_NO_RESPONSE_LABEL: SENT_NO_RESPONSE_QUERY,
    SENT_TABLE_NAME_PENDING: SENT_TABLE_PENDING_QUERY,
    SENT_TABLE_NAME_NO_RESPONSE: SENT_TABLE_NO_RESPONSE_QUERY,
    SENT_STUCK_TOP_CLINICS_NAME: SENT_STUCK_TOP_CLINICS_QUERY,
    SENT_STUCK_TOP_SEMD_NAME: SENT_STUCK_TOP_SEMD_QUERY,
    SENT_REGISTRATION_FUNNEL_NAME: SENT_REGISTRATION_FUNNEL_QUERY,
    SENT_PENDING_FUNNEL_NAME: SENT_PENDING_FUNNEL_QUERY,
}

SENT_TAB_DESCRIPTIONS: dict[str, str] = {
    "В обработке": "Отправленные в пределах ожидаемого времени ответа (ступени до последней, dim_pending_segments).",
    SENT_STATE_NO_RESPONSE_LABEL: "Отправленные, по которым ожидаемое время истекло: ответа не будет. Из остальных карточек такие документы исключены и подлежат очистке.",
    SENT_TABLE_NAME_PENDING: "Отправленные, по которым ответ ещё ожидается; новые сверху. Ступень обработки, число подач.",
    SENT_TABLE_NAME_NO_RESPONSE: "Отправленные, по которым ожидаемое время истекло и ответа уже не будет; самые давние сверху.",
    SENT_STUCK_TOP_CLINICS_NAME: "Отправленные без ответа по клиникам, разложенные по ступеням ожидания; клиники — по числу документов «Без ответа».",
    SENT_STUCK_TOP_SEMD_NAME: "Отправленные без ответа по типам СЭМД, разложенные по ступеням ожидания; типы — по числу документов «Без ответа».",
    SENT_REGISTRATION_FUNNEL_NAME: (
        "Документы, по которым ответ уже получен: сколько из них уложилось в срок. "
        "Сроки ужесточаются слева направо, поэтому воронка сужается. Разрыв между базой "
        "и первой ступенью — регистрации дольше самой мягкой границы справочника. "
        "Ожидающие и «Без ответа» в корпус не входят: срока регистрации у них нет."
    ),
    SENT_PENDING_FUNNEL_NAME: (
        "Очередь ожидания на правую границу выбранного периода: документ отправлен "
        "до неё, а ответ либо не пришёл вовсе, либо пришёл позже. Возраст ожидания "
        "отсчитывается от той же границы, поэтому срез не зависит от момента просмотра."
    ),
}

UNDELIVERED_TO_CLINIC_FILTER_TAGS = {
    key: deepcopy(SENT_FILTER_TEMPLATE_TAGS[key])
    for key in ("ips_date", "semd_type", "jid", "local_uid")
}
UNDELIVERED_TO_CLINIC_FIELD_FILTERS = {
    "ips_date": {"table_ref": "public.rpt_documents", "field_name": "ips_date"},
    "semd_type": {"table_ref": "public.rpt_documents", "field_name": "semd_label"},
    "jid": {"table_ref": "public.rpt_documents", "field_name": "clinic_label"},
    "local_uid": {"table_ref": "public.rpt_documents", "field_name": "semd_local_uid"},
}
UNDELIVERED_TO_CLINIC_LATEST_ERRORS = (
    "WITH latest_errors AS ( "
    "SELECT DISTINCT ON (tx.dwh_id) "
    "tx.dwh_id, tx.logid, tx.log_date AS error_at, tx.message AS error_text "
    "FROM public.transactions tx "
    "JOIN public.documents d ON d.dwh_id = tx.dwh_id "
    "WHERE tx.status = 'network_error' "
    "AND d.result_logid IS NOT NULL "
    "AND tx.logid > d.result_logid "
    "ORDER BY tx.dwh_id, tx.logid DESC, tx.log_date DESC NULLS LAST ) "
)
UNDELIVERED_TO_CLINIC_QUERY = (
    UNDELIVERED_TO_CLINIC_LATEST_ERRORS
    + 'SELECT COUNT(DISTINCT public.rpt_documents.dwh_id)::bigint AS "Документов" '
    "FROM latest_errors "
    "JOIN public.rpt_documents ON public.rpt_documents.dwh_id = latest_errors.dwh_id "
    "JOIN public.documents d ON d.dwh_id = latest_errors.dwh_id "
    "WHERE public.rpt_documents.status IN ('success', 'async_error') "
    "AND d.result_logid IS NOT NULL "
    "[[AND {{ips_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] [[AND {{local_uid}}]]"
)
UNDELIVERED_TO_CLINIC_DETAIL_QUERY = (
    UNDELIVERED_TO_CLINIC_LATEST_ERRORS
    + 'SELECT public.rpt_documents.semd_local_uid AS "localUid СЭМД", '
    'public.rpt_documents.semd_code AS "Код СЭМД", '
    'public.rpt_documents.semd_name AS "Наименование СЭМД", '
    'public.rpt_documents.clinic_jid::text AS "JID Клиники", '
    'public.rpt_documents.clinic_label AS "Клиника", '
    'public.rpt_documents.first_sent_at AS "Дата отправки", '
    'public.rpt_documents.ips_date AS "Дата ответа ЕГИСЗ", '
    'public.rpt_documents.status_detail_label AS "Результат ЕГИСЗ", '
    'd.result_logid::text AS "LOGID ответа ЕГИСЗ", '
    'latest_errors.logid::text AS "LOGID ошибки доставки", '
    'latest_errors.error_at AS "Дата ошибки доставки", '
    'LEFT(COALESCE(latest_errors.error_text, \'\'), 180) AS "Текст ошибки доставки" '
    "FROM latest_errors "
    "JOIN public.rpt_documents ON public.rpt_documents.dwh_id = latest_errors.dwh_id "
    "JOIN public.documents d ON d.dwh_id = latest_errors.dwh_id "
    "WHERE public.rpt_documents.status IN ('success', 'async_error') "
    "AND d.result_logid IS NOT NULL "
    "[[AND {{ips_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] [[AND {{local_uid}}]] "
    "ORDER BY latest_errors.error_at DESC NULLS LAST, latest_errors.logid DESC "
    "LIMIT 200"
)


def apply_undelivered_to_clinic(card: dict) -> None:
    detail = card.get("name") == SENT_UNDELIVERED_TO_CLINIC_DETAIL_NAME
    card["display"] = "table" if detail else "scalar"
    card["description"] = (
        "Сбой доставки результата в клинику после финального ответа ЕГИСЗ. "
        "Источник — разобранные transactions; raw-слой после обработки не требуется."
    )
    card.pop("query_tier", None)
    card.pop("source_model", None)
    card["dataset_query"] = {
        "type": "native",
        "database": 1,
        "native": {
            "query": UNDELIVERED_TO_CLINIC_DETAIL_QUERY if detail else UNDELIVERED_TO_CLINIC_QUERY,
            "template-tags": deepcopy(UNDELIVERED_TO_CLINIC_FILTER_TAGS),
        },
    }
    card["metabase-field-filters"] = deepcopy(UNDELIVERED_TO_CLINIC_FIELD_FILTERS)
    viz = card.setdefault("visualization_settings", {})
    if detail:
        viz["table.columns"] = [
            {"enabled": True, "name": "localUid СЭМД"},
            {"enabled": True, "name": "Код СЭМД"},
            {"enabled": True, "name": "Наименование СЭМД"},
            {"enabled": False, "name": "JID Клиники"},
            {"enabled": True, "name": "Клиника"},
            {"enabled": True, "name": "Дата отправки"},
            {"enabled": True, "name": "Дата ответа ЕГИСЗ"},
            {"enabled": True, "name": "Результат ЕГИСЗ"},
            {"enabled": True, "name": "LOGID ответа ЕГИСЗ"},
            {"enabled": True, "name": "LOGID ошибки доставки"},
            {"enabled": True, "name": "Дата ошибки доставки"},
            {"enabled": True, "name": "Текст ошибки доставки"},
        ]

# «Всего» — весь срез клиники, включая отправленные без ответа; «% успеха» считается
# от корпуса с ответом, поэтому отправленные не размывают долю. Прежняя единая колонка
# «Отправлено» разделена на два состояния: они требуют разных действий поддержки.
CLINIC_SUCCESS_QUERY = (
    "SELECT COALESCE(NULLIF(TRIM(clinic_jid::text), ''), 'Неизвестно') AS \"JID Клиники\", "
    "COALESCE(MAX(NULLIF(TRIM(clinic_name::text), '')), 'Неизвестно') AS \"Клиника\", "
    'COUNT(DISTINCT dwh_id)::bigint AS "Всего", '
    "COUNT(DISTINCT dwh_id) FILTER (WHERE status='success')::bigint AS \"Успешно\", "
    "COUNT(DISTINCT dwh_id) FILTER (WHERE status='async_error')::bigint AS \"Отказ РЭМД\", "
    "COUNT(DISTINCT dwh_id) FILTER (WHERE status_detail='pending')::bigint AS \"В обработке\", "
    "COUNT(DISTINCT dwh_id) FILTER (WHERE status_detail='no_response')::bigint AS \"Без ответа\", "
    "ROUND(100.0 * COUNT(DISTINCT dwh_id) FILTER (WHERE status='success') "
    "/ NULLIF(COUNT(DISTINCT dwh_id) FILTER "
    "(WHERE status IN ('success', 'async_error', 'network_error')), 0), 1) AS \"% успеха\" "
    "FROM public.rpt_documents "
    "WHERE COALESCE(NULLIF(TRIM(clinic_jid::text), ''), '') <> '' "
    "[[AND {{ips_date}}]] [[AND {{jid}}]] [[AND {{semd_type}}]] [[AND {{status}}]] "
    "GROUP BY 1 HAVING COUNT(DISTINCT dwh_id) > 0 ORDER BY 3 DESC"
)

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
    q = q.replace("result_msgid AS message_id", "msgid AS message_id")
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
    q = q.replace("AS pending_segment,", 'AS "Ступень обработки",')
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
        "    rpt_documents.status_detail_label AS \"Статус\",\n"
        "    rpt_documents.clinic_label AS \"Клиника\",\n"
        "    rpt_documents.semd_code AS \"Код СЭМД\",\n"
        "    rpt_documents.semd_name AS \"Наименование СЭМД\",\n"
        "    rpt_documents.semd_local_uid AS \"localUid СЭМД\",\n"
        "    rpt_document_lineage.clinic_jid::text AS \"JID Клиники\",\n"
        "    CASE\n"
        "      WHEN rpt_documents.clinic_oid_unknown\n"
        "      THEN '↯ ' || rpt_document_lineage.clinic_oid_xml\n"
        "      ELSE COALESCE(NULLIF(btrim(rpt_document_lineage.clinic_oid_xml), ''), '—')\n"
        "    END AS \"OID из обмена\",\n"
        "    COALESCE(NULLIF(BTRIM(rpt_document_lineage.clinic_jid_by_oid::text), ''), '—') AS \"ЮЛ по реестру OID\",\n"
        "    COALESCE(NULLIF(btrim(rpt_document_lineage.clinic_host), ''), '—') AS \"Host Клиники (ГОСТ VPN)\",\n"
        "    COALESCE(NULLIF(btrim(rpt_document_lineage.clinic_jid_resolve_method), ''), '—') AS \"Метод резолва JID\",\n"
        "    TRIM(BOTH ' · ' FROM CONCAT_WS(' · ',\n"
        "      CASE WHEN NULLIF(BTRIM(rpt_documents.clinic_jid::text), '') IS NULL THEN 'без JID' END,\n"
        "      CASE WHEN rpt_documents.clinic_oid_unknown = true THEN 'OID вне реестра' END,\n"
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
        "Поступление документов на прокси по дням первой отправки (first_sent_at). "
        "Фильтр «Период» — по дате поступления, не по «Обработано IPS» (ips_date)."
    )
    dq = card.setdefault("dataset_query", {})
    dq["native"]["query"] = DOCUMENT_VOLUME_BY_DAY_QUERY
    tags = dq["native"].setdefault("template-tags", {})
    tags.pop("dwh_date", None)
    if "ips_date" in tags:
        tags["ips_date"]["display-name"] = "По дате поступления"
    card["metabase-field-filters"] = {
        "ips_date": {"table_ref": "public.rpt_documents", "field_name": "first_sent_at"},
        "semd_type": {"table_ref": "public.rpt_documents", "field_name": "semd_label"},
        "jid": {"table_ref": "public.rpt_documents", "field_name": "clinic_label"},
        "local_uid": {"table_ref": "public.rpt_documents", "field_name": "semd_local_uid"},
        "relates_to": {"table_ref": "public.rpt_documents", "field_name": "relates_to_msgid"},
        "emdr_id": {"table_ref": "public.rpt_documents", "field_name": "semd_emdr_id"},
        "status": {"table_ref": "public.rpt_documents", "field_name": "status_detail_label"},
        "log_id": {"table_ref": "public.rpt_documents", "field_name": "logid"},
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
        "Колонка «Нарушения» перечисляет сработавшие проверки; «ЮЛ по реестру OID» показывает, к какой "
        "клинике реестр относит OID из обмена. Лимит 1000 строк."
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
        {"enabled": True, "name": "OID из обмена"},
        {"enabled": True, "name": "ЮЛ по реестру OID"},
        {"enabled": True, "name": "Host Клиники (ГОСТ VPN)"},
        {"enabled": False, "name": "Метод резолва JID"},
        {"enabled": True, "name": "Код СЭМД"},
        {"enabled": True, "name": "Наименование СЭМД"},
        {"enabled": True, "name": "localUid СЭМД"},
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
            "columns": ["OID из обмена"],
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
        "Документы по дням и состоянию: три исхода РЭМД плюс «В обработке» (stacked). "
        "«Без ответа» — на вкладке «Отправленные». Клик по сегменту — архив с фильтром по статусу."
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
    series = viz.setdefault("series_settings", {})
    # «Без ответа» из запроса не приходит — серия с этим ключом осталась бы мёртвой.
    for stale in ("Отправлено", "Без ответа"):
        series.pop(stale, None)
    series.update(
        {k: deepcopy(v) for k, v in STATUS_DETAIL_COLORS.items() if k != "Без ответа"}
    )
    # graph.series_order может нести серии прежней раскладки статусов.
    if isinstance(viz.get("graph.series_order"), list):
        viz["graph.series_order"] = [
            s for s in viz["graph.series_order"]
            if not (isinstance(s, dict) and s.get("key") in ("Отправлено", "Без ответа"))
        ]
    cs = viz.setdefault("column_settings", {})
    cs['["name","Документов"]'] = {
        "column_title": "Документов",
        "decimals": 0,
        "number_separators": ", ",
    }


def apply_refusals_hourly(card: dict) -> None:
    """«Отказы по часам» — error-rate: доли отказов связи и асинхронного ответа от
    документов с ответом РЭМД за час; знаменатель одного грейна с сериями."""
    card["display"] = "line"
    card["description"] = (
        "Почасовые доли отказов связи и асинхронного ответа РЭМД от всех документов "
        "с ответом РЭМД за час (%). Отправленные без ответа в знаменатель не входят. "
        "Ось — «Дата обработки» (`rpt_documents`), период — фильтр «Обработано IPS»."
    )
    card["dataset_query"]["native"]["query"] = SERVICE_REFUSALS_BY_HOUR_QUERY
    viz = card.setdefault("visualization_settings", {})
    viz["graph.dimensions"] = ["Час"]
    viz["graph.metrics"] = ["Ошибка связи, %", "Ошибка асинхронного ответа РЭМД, %"]
    viz["graph.x_axis.scale"] = "timeseries"
    viz["graph.x_axis.title_text"] = "Час"
    viz["graph.y_axis.title_text"] = "% ошибок"
    viz["graph.show_values"] = False
    viz["series_settings"] = {
        "Ошибка связи, %": {"display": "line", "axis": "left", "color": "#F2994A"},
        "Ошибка асинхронного ответа РЭМД, %": {
            "display": "line",
            "axis": "left",
            "color": "#A989C5",
        },
    }
    viz["column_settings"] = {
        '["name","Ошибка связи, %"]': {
            "decimals": 1,
            "number_separators": ", ",
            "suffix": " %",
        },
        '["name","Ошибка асинхронного ответа РЭМД, %"]': {
            "decimals": 1,
            "number_separators": ", ",
            "suffix": " %",
        },
        '["name","Час"]': {"date_style": "D MMMM, YYYY", "time_style": "HH:mm"},
    }


def apply_semd_volume_table(card: dict) -> None:
    """Таблица кодов СЭМД по числу документов в срезе (вкладка «Архив СЭМД»)."""
    card["name"] = "Топ типов СЭМД по документам"
    card["display"] = "table"
    card["description"] = "Объём документов по кодам СЭМД в срезе. Колонка «%» — доля от общего числа документов."
    viz = card.setdefault("visualization_settings", {})
    for key in list(viz.keys()):
        if key.startswith("graph.") or key.startswith("pie."):
            del viz[key]
    viz["table.columns"] = [
        {"enabled": True, "name": "СЭМД"},
        {"enabled": True, "name": "Документов"},
        {"enabled": True, "name": "%"},
    ]
    viz["table.cell_column"] = "СЭМД"
    viz["column_settings"] = deepcopy(COUNT_COLUMN_SETTINGS)
    strip_chart_keys(viz, "table")


def apply_clinic_volume(card: dict) -> None:
    card["dataset_query"]["native"]["query"] = CLINIC_VOLUME_QUERY
    viz = card.setdefault("visualization_settings", {})
    viz["table.columns"] = deepcopy(CLINIC_VOLUME_TABLE_COLUMNS)
    viz["table.cell_column"] = "Клиника"
    viz["table.column_widths"] = deepcopy(CLINIC_VOLUME_COLUMN_WIDTHS)
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
        '["name","Код отказа"]': {"column_title": "Код отказа", "text_style": "wrap"},
        '["name","Справочник"]': {"column_title": "Справочник", "text_style": "wrap"},
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
        "«% ошибок» — доля документов с этим типом от всех документов с ошибками; "
        "«% всего» — доля от всех документов с ответом РЭМД (успех + ошибка)."
    )
    card["dataset_query"]["native"]["query"] = TOP_ERROR_TYPE_QUERY
    # Знаменатель «обработанных» живёт на грейне документа → фильтры среза привязаны к
    # rpt_documents (не к rpt_error_breakdown), иначе предикат в period_docs не развернётся.
    card["metabase-field-filters"] = {
        "ips_date": {"table_ref": "public.rpt_documents", "field_name": "ips_date"},
        "jid": {"table_ref": "public.rpt_documents", "field_name": "clinic_label"},
        "semd_type": {"table_ref": "public.rpt_documents", "field_name": "semd_label"},
    }
    viz = card.setdefault("visualization_settings", {})
    for key in list(viz.keys()):
        if key.startswith("graph.") or key.startswith("pie."):
            del viz[key]
    viz.pop("series_settings", None)
    viz["table.columns"] = deepcopy(TOP_ERROR_TYPE_TABLE_COLUMNS)
    viz["table.cell_column"] = "Документов"
    viz["table.pivot_column"] = "Документов"
    viz["table.column_widths"] = deepcopy(TOP_ERROR_TYPE_COLUMN_WIDTHS)
    viz["table.column_formatting"] = deepcopy(TOP_ERROR_TYPE_COLUMN_FORMATTING)
    viz["version"] = 2
    cs = deepcopy(COUNT_COLUMN_SETTINGS)
    cs['["name","Тип ошибки"]'] = {"column_title": "Тип ошибки", "text_style": "wrap"}
    # SQL-алиас столбца — «% обработанных», но в шапке показываем короткое «% всего».
    for col, title in (("% ошибок", "% ошибок"), ("% обработанных", "% всего")):
        cs[f'["name","{col}"]'] = {
            "column_title": title,
            "decimals": 1,
            "number_separators": ", ",
            "suffix": " %",
        }
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
    card["metabase-field-filters"] = {
        "ips_date": {"table_ref": "public.rpt_error_breakdown", "field_name": "ips_date"},
        "jid": {"table_ref": "public.rpt_error_breakdown", "field_name": "clinic_label"},
        "semd_type": {"table_ref": "public.rpt_error_breakdown", "field_name": "semd_code"},
    }
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
    viz["table.column_widths"] = deepcopy(TOP_SEMD_BY_ERRORS_COLUMN_WIDTHS)
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
        native = card.get("dataset_query", {}).get("native")
        if native:
            native["query"] = CLINIC_SUCCESS_QUERY
    elif name == "Успешность по типам СЭМД":
        viz["table.column_widths"] = deepcopy(SUCCESS_SEMD_COLUMN_WIDTHS)


def apply_sent_tab(dash: dict) -> None:
    """Вкладка «Отправленные»: единый источник (rpt_documents_sent), единый блок фильтров.

    KPI-плитки заменяют прежние пороговые счётчики («Зависших > N дней»): пороги теперь
    живут в dim_pending_segments, а карточка отбирает по состоянию, а не по числу дней.
    """
    dash["cards"] = [
        card for card in dash.get("cards", []) if card.get("name") not in RETIRED_CARD_NAMES
    ]
    # Воронка скорости — про весь корпус регистрации, а не про очередь ожидания,
    # поэтому её место на оперативном мониторинге.
    for card in dash.get("cards", []):
        if card.get("name") == SENT_REGISTRATION_FUNNEL_NAME:
            card["tab"] = "operational"

    # Карточка могла прийти и переименованием прежней, и созданием новой — оставляем одну.
    # Ключ включает вкладку: одна и та же карточка намеренно стоит на нескольких вкладках
    # (например «Объём по клиникам» — на оперативной и в архиве), и дедуп по одному имени
    # снёс бы эти повторы.
    seen: set[tuple[str, str]] = set()
    deduped = []
    for card in dash.get("cards", []):
        key = (card.get("name") or "", card.get("tab") or "")
        if key[0] and key in seen:
            continue
        if key[0]:
            seen.add(key)
        deduped.append(card)
    dash["cards"] = deduped

    existing = {card.get("name") for card in dash.get("cards", [])}
    for new_name, new_display, new_tab in (
        (SENT_TABLE_NAME_NO_RESPONSE, "table", "sent"),
        (SENT_PENDING_FUNNEL_NAME, "funnel", "sent"),
        (SENT_REGISTRATION_FUNNEL_NAME, "funnel", "operational"),
    ):
        if new_name not in existing:
            dash.setdefault("cards", []).append(
                {"name": new_name, "display": new_display, "tab": new_tab}
            )

    for card in dash.get("cards", []):
        if card.get("tab") != "sent" or card.get("display") == "text":
            continue
        name = card.get("name", "")
        query = SENT_TAB_QUERIES.get(name)
        if not query:
            continue
        card["description"] = SENT_TAB_DESCRIPTIONS[name]
        card.pop("query_tier", None)
        card.pop("source_model", None)
        card.pop("metabase-parameter-targets", None)
        card["dataset_query"] = {
            "type": "native",
            "database": 1,
            "native": {
                "query": query,
                "template-tags": deepcopy(SENT_FILTER_TEMPLATE_TAGS),
            },
        }
        card["metabase-field-filters"] = deepcopy(SENT_FIELD_FILTERS)
        viz = card.setdefault("visualization_settings", {})
        if name in (SENT_REGISTRATION_FUNNEL_NAME, SENT_PENDING_FUNNEL_NAME):
            # Карточка построена на rpt_documents, а не на срезе ожидающих: свои привязки
            # фильтров и свой набор тегов.
            card["metabase-field-filters"] = deepcopy(SENT_REGISTRATION_FIELD_FILTERS)
            tags = card["dataset_query"]["native"]["template-tags"]
            for tag in list(tags):
                if tag not in SENT_REGISTRATION_FIELD_FILTERS:
                    tags.pop(tag)
            card["display"] = "funnel"
            for key in [k for k in viz if k.startswith("graph.") or k.startswith("stackable.")]:
                del viz[key]
            viz["funnel.dimension"] = "Этап"
            viz["funnel.metric"] = "Документов"
        elif name in (SENT_STUCK_TOP_CLINICS_NAME, SENT_STUCK_TOP_SEMD_NAME):
            card["display"] = "row"
            viz["graph.dimensions"] = [
                "Клиника" if name == SENT_STUCK_TOP_CLINICS_NAME else "Код СЭМД",
                "Ступень обработки",
            ]
            viz["graph.metrics"] = ["Документов"]
            viz["stackable.stack_type"] = "stacked"
        if name in (SENT_TABLE_NAME_PENDING, SENT_TABLE_NAME_NO_RESPONSE):
            viz["table.columns"] = deepcopy(SENT_TABLE_COLUMNS)
            viz["table.cell_column"] = "Состояние отправки"
            viz.setdefault("column_settings", {})['["name","Подач в ЕГИСЗ"]'] = {
                "column_title": "Подач в ЕГИСЗ",
                "decimals": 0,
                "number_separators": ", ",
            }
        elif name == "Доля без ответа, %":
            viz.setdefault("column_settings", {})['["name","%"]'] = {
                "column_title": "%",
                "decimals": 1,
                "number_separators": ", ",
                "suffix": " %",
            }
        elif card.get("display") in ("row", "bar"):
            viz.setdefault("column_settings", {})['["name","Документов"]'] = {
                "column_title": "Документов",
                "decimals": 0,
                "number_separators": ", ",
            }


def apply_latest_operations(card: dict) -> None:
    card["description"] = (
        "До 50 последних документов в периоде; сортировка по дате последней активности "
        "(«Обработано IPS», новые сверху). Одна строка — один dwh_id."
    )
    card.pop("query_tier", None)
    card.pop("source_model", None)
    card.pop("metabase-parameter-targets", None)
    card["dataset_query"] = {
        "type": "native",
        "database": 1,
        "native": {
            "query": LATEST_OPERATIONS_QUERY,
            "template-tags": deepcopy(DOCUMENTS_FILTER_TEMPLATE_TAGS),
        },
    }
    card["metabase-field-filters"] = deepcopy(DOCUMENTS_FILTER_FIELD_FILTERS)
    viz = card.setdefault("visualization_settings", {})
    viz.pop("table", None)
    viz["table.columns"] = deepcopy(LATEST_OPERATIONS_TABLE_COLUMNS)
    viz["table.cell_column"] = "Клиника"
    # Ширины с прода: первые две колонки авто (null), явные — только у «Клиника»/«СЭМД».
    viz["table.column_widths"] = [None, None, 220, 342]
    viz["column_settings"] = deepcopy(LATEST_OPERATIONS_COLUMN_SETTINGS)
    strip_chart_keys(viz, card.get("display", "table"))


def apply_status_period(card: dict) -> None:
    card["description"] = (
        "Распределение документов за период по состоянию: три исхода РЭМД плюс "
        "«В обработке». «Без ответа» разбирается на вкладке «Отправленные»."
    )
    card.pop("query_tier", None)
    card.pop("source_model", None)
    card.pop("metabase-parameter-targets", None)
    status_tags = {
        k: v for k, v in DOCUMENTS_FILTER_TEMPLATE_TAGS.items() if k != "status"
    }
    status_filters = {
        k: v for k, v in DOCUMENTS_FILTER_FIELD_FILTERS.items() if k != "status"
    }
    card["dataset_query"] = {
        "type": "native",
        "database": 1,
        "native": {
            "query": STATUS_PERIOD_QUERY,
            "template-tags": deepcopy(status_tags),
        },
    }
    card["metabase-field-filters"] = deepcopy(status_filters)
    # pie.rows несёт подписи и цвета срезов. Прежняя раскладка держала скрытую строку
    # «Отправлено», которая из запроса прийти не могла; состав приводится к тому,
    # что карточка действительно возвращает.
    viz = card.setdefault("visualization_settings", {})
    order = list(STATUS_DETAIL_COLORS)
    order.remove("Без ответа")
    viz["pie.rows"] = [
        {
            "key": label,
            "name": label,
            "originalName": label,
            "color": STATUS_DETAIL_COLORS[label]["color"],
            "enabled": True,
            "hidden": False,
        }
        for label in order
    ]


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
        "pending_segment": "Ступень обработки",
        "status_detail_label": "Статус",
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
            "order-by": [["desc", ["field", "Документы:ips_date", None]]],
        },
    }
    card["metabase-parameter-targets"] = deepcopy(DOCUMENTS_PARAM_TARGETS)
    card.pop("metabase-field-filters", None)
    viz = card.setdefault("visualization_settings", {})
    viz["table.columns"] = deepcopy(ARCHIVE_TABLE_COLUMNS)
    cs = viz.setdefault("column_settings", {})
    cs.pop('["name","Сводка ошибки"]', None)
    cs['["name","Типы ошибки"]'] = {"column_title": "Типы ошибки", "text_style": "wrap"}
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


# --- Единая модель имён дат + label-поиск + конфайн фильтров ----------------------
# Дата-токен фильтра во всех дашбордах сводится к ips_date (обработка транспортом IPS,
# EXCHANGELOG.CREATEDATE). Колоночные привязки JID/СЭМД/Статус — на label («код · имя»),
# чтобы фильтр искал и по коду, и по наименованию.
# normalize_dashboard — единственная точка приведения к модели имён: authoring-константы и
# JSON-карточки могут нести доменные токены (dwh_date/processed_at/processed_day/arrival_day,
# клиентский client_period, управленческий mgmt_period); проход ниже канонизирует их к ips_date /
# first_sent_at и привязки JID/СЭМД/Статус к label-колонкам — идемпотентно, на выходе один смысл.
DATE_FILTER_TOKENS = ("dwh_date", "mgmt_period", "client_period")
DATE_PARAM_SLUGS = ("dwh_date_filter", "mgmt_period_filter", "client_period_filter")
IPS_DATE_DISPLAY = "Обработано IPS"

LABEL_FIELD_BY_KEY = {
    "jid": "clinic_label",
    "semd_type": "semd_label",
    "status": "status_detail_label",
}
DATE_FIELD_RENAME = {
    "processed_at": "ips_date",
    "processed_day": "ips_date",
    "arrival_day": "first_sent_at",
    "sent_at": "first_sent_at",
}

# Каноничный порядок фильтров в WHERE-блоке.
FILTER_ORDER = [
    "ips_date", "semd_type", "jid", "local_uid", "relates_to",
    "emdr_id", "status", "log_id", "pending_segment", "error_type",
]

# Конфайн lookup-фильтров: агрегатные карточки несут только 3 общих (+ status, где осмыслен).
# Полный набор document-lookup (local_uid/relates_to/emdr_id/log_id) — лишь на вкладке archive.
SLIM_FILTERS: dict[str, set[str]] = {
    "Объём по клиникам": {"ips_date", "semd_type", "jid", "status"},
    "Топ типов СЭМД по документам": {"ips_date", "semd_type", "jid", "status"},
    "Объём ошибок по клиникам": {"ips_date", "semd_type", "jid", "status"},
    "Топ типов СЭМД по ошибкам": {"ips_date", "semd_type", "jid"},
    "Отказы по часам: связь и асинхронный ответ": {"ips_date", "semd_type", "jid"},
    "РЭМД vs связь": {"ips_date", "semd_type", "jid"},
}


# Миграция статусной модели в SQL карточек, которые не собираются генератором целиком
# (дашборды 05/07/08). Корпус отображения — всё, кроме «Без ответа»: по этим документам
# ответа уже не ожидается, они разбираются на вкладке «Отправленные».
# Знаменатели долей, перечисляющие исходы явным FILTER (...), правило не затрагивает.
STATUS_TOKEN_MIGRATION = [
    ("status <> 'waiting'", "status_detail <> 'no_response'"),
    ("status != 'waiting'", "status_detail <> 'no_response'"),
    ("status = 'waiting'", "status = 'sent'"),
    ("status IN ('waiting')", "status = 'sent'"),
    ("rpt_documents_waiting", "rpt_documents_sent"),
    ("waiting_days", "pending_days"),
    ("waiting_hours", "pending_hours"),
    ("wait_segment", "pending_segment"),
    # Локальные алиасы CTE в управленческом дашборде: имя alias'а читается в заголовках
    # колонок и в условиях риска, поэтому приводится к той же терминологии.
    ("FILTER (WHERE status = 'sent') AS waiting", "FILTER (WHERE status = 'sent') AS sent"),
    ("waiting::numeric", "sent::numeric"),
    ('waiting AS "Отправлено"', 'sent AS "Отправлено"'),
]


def migrate_status_tokens(query: str) -> str:
    if not query:
        return query
    for old, new in STATUS_TOKEN_MIGRATION:
        query = query.replace(old, new)
    return query


def _rename_sql_tokens(query: str) -> str:
    if not query:
        return query
    query = migrate_status_tokens(query)
    q = query
    for tok in DATE_FILTER_TOKENS:
        q = re.sub(r"\{\{\s*" + tok + r"\s*\}\}", "{{ips_date}}", q)
    # День берём из полной даты инлайн (стек уже МСК-pinned, без AT TIME ZONE).
    q = re.sub(r"\bprocessed_day\b", "ips_date::date", q)
    q = re.sub(r"\barrival_day\b", "first_sent_at::date", q)
    q = re.sub(r"\bprocessed_at\b", "ips_date", q)
    q = re.sub(r"\bsent_at\b", "first_sent_at", q)  # \b не задевает first_sent_at
    return q


def set_filter_block(query: str, keys: set[str]) -> str:
    """Заменяет непрерывный ран [[AND {{x}}]] в WHERE на ровно keys (в каноне порядка)."""
    if "[[AND" not in query:
        return query
    block = " ".join("[[AND {{" + k + "}}]]" for k in FILTER_ORDER if k in keys)
    return re.sub(r"(?:\[\[AND \{\{[^}]+\}\}\]\]\s*)+", block + " ", query, count=1)


def _normalize_template_tags(tags: dict) -> dict:
    out: dict = {}
    for key, tag in tags.items():
        nkey = "ips_date" if key in DATE_FILTER_TOKENS else key
        if isinstance(tag, dict):
            tag = dict(tag)
            tag["name"] = nkey
        out[nkey] = tag
    return out


def _normalize_filter_bindings(mapping: dict) -> dict:
    out: dict = {}
    for key, spec in mapping.items():
        nkey = "ips_date" if key in DATE_FILTER_TOKENS else key
        if isinstance(spec, dict):
            spec = dict(spec)
            if nkey in LABEL_FIELD_BY_KEY:
                spec["field_name"] = LABEL_FIELD_BY_KEY[nkey]
            elif spec.get("field_name") in DATE_FIELD_RENAME:
                spec["field_name"] = DATE_FIELD_RENAME[spec["field_name"]]
        out[nkey] = spec
    return out


# Привязки дашборд-параметров в модель-дрилле: дата→ips_date, JID/СЭМД/Статус→label.
MODEL_DRILL_FIELD_RENAME = {
    "processed_at": "ips_date",
    "sent_at": "first_sent_at",
    "arrival_day": "first_sent_at",
    "clinic_jid": "clinic_label",
    "semd_code": "semd_label",
    "status": "status_detail_label",
}


def _rename_qb_field_refs(node):
    """QB-карточки ссылаются на поля модели строкой «Модель:processed_at» — синхронно с rename."""
    if isinstance(node, list):
        return [_rename_qb_field_refs(x) for x in node]
    if isinstance(node, dict):
        return {k: _rename_qb_field_refs(v) for k, v in node.items()}
    if isinstance(node, str):
        node = re.sub(r":(processed_at|processed_day)\b", ":ips_date", node)
        node = re.sub(r":(arrival_day|sent_at)\b", ":first_sent_at", node)
        return node
    return node


def normalize_card(card: dict) -> None:
    dq = card.get("dataset_query") or {}
    if dq.get("type") == "native":
        nat = dq.get("native") or {}
        if nat.get("query"):
            nat["query"] = _rename_sql_tokens(nat["query"])
        if nat.get("template-tags"):
            nat["template-tags"] = _normalize_template_tags(nat["template-tags"])
    elif dq.get("type") == "query" and dq.get("query"):
        dq["query"] = _rename_qb_field_refs(dq["query"])
    if card.get("metabase-field-filters"):
        card["metabase-field-filters"] = _normalize_filter_bindings(card["metabase-field-filters"])
    if card.get("metabase-parameter-targets"):
        card["metabase-parameter-targets"] = _normalize_filter_bindings(card["metabase-parameter-targets"])
    mdp = card.get("metabase-model-drill-params")
    if isinstance(mdp, dict):
        card["metabase-model-drill-params"] = {
            ("ips_date" if k in DATE_FILTER_TOKENS else k): MODEL_DRILL_FIELD_RENAME.get(v, v)
            for k, v in mdp.items()
        }
    cb = card.get("click_behavior") or {}
    if cb.get("linkType") == "dashboard" and isinstance(cb.get("parameterMapping"), dict):
        cb["parameterMapping"] = {
            ("ips_date_filter" if slug in DATE_PARAM_SLUGS else slug): m
            for slug, m in cb["parameterMapping"].items()
        }


def prune_unused_filters(card: dict) -> None:
    """Native: убрать template-tags/field-filters, чьих {{name}} нет в SQL (синхронизация)."""
    dq = card.get("dataset_query") or {}
    if dq.get("type") != "native":
        return
    nat = dq.get("native") or {}
    query = nat.get("query") or ""
    tags = nat.get("template-tags") or {}
    keep = {k for k in tags if re.search(r"\{\{\s*" + re.escape(k) + r"\s*\}\}", query)}
    nat["template-tags"] = {k: v for k, v in tags.items() if k in keep}
    ff = card.get("metabase-field-filters")
    if ff:
        card["metabase-field-filters"] = {k: v for k, v in ff.items() if k in keep}


def normalize_parameters(dash: dict) -> None:
    for p in dash.get("parameters", []):
        slug = p.get("slug")
        if slug in DATE_PARAM_SLUGS:
            p["slug"] = "ips_date_filter"
            p["name"] = IPS_DATE_DISPLAY
        elif slug == "jid_filter":
            p["name"] = "Клиника"
        elif slug == "semd_type_filter":
            p["name"] = "Тип СЭМД"
        elif slug == "wait_segment_filter":
            p["slug"] = "pending_segment_filter"
            p["name"] = "Ступень обработки"
        elif slug == "pending_segment_filter":
            p["name"] = "Ступень обработки"


# Вкладка «очереди» переименована в «Отправленные»: документ без ответа не стоит
# в очереди на стороне шлюза, он уже отправлен. Идентификатор нормализуется здесь,
# чтобы прежние выгрузки дашборда подхватывались без ручной правки.
TAB_RENAME = {"queue": "sent"}


def normalize_tabs(dash: dict) -> None:
    for tab in dash.get("tabs", []) or []:
        if tab.get("id") in TAB_RENAME:
            tab["id"] = TAB_RENAME[tab["id"]]
    for card in dash.get("cards", []):
        if card.get("tab") in TAB_RENAME:
            card["tab"] = TAB_RENAME[card["tab"]]


def normalize_dashboard(dash: dict) -> None:
    """Единый идемпотентный проход: даты→ips_date, привязки→label, чистка orphan-тегов."""
    normalize_tabs(dash)
    normalize_parameters(dash)
    for card in dash.get("cards", []):
        if card.get("display") == "text":
            continue
        normalize_card(card)
        prune_unused_filters(card)


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
        elif name == "Транзакции по дням и статусам" and dq.get("type") == "native":
            apply_transactions_trend(card)
        elif name == "Отказы по часам: связь и асинхронный ответ" and dq.get("type") == "native":
            apply_refusals_hourly(card)
        elif name == "Динамика документов по дням" and dq.get("type") == "native":
            apply_document_volume_by_day(card)
        elif name in (SENT_UNDELIVERED_TO_CLINIC_NAME, SENT_UNDELIVERED_TO_CLINIC_DETAIL_NAME):
            apply_undelivered_to_clinic(card)
        elif name == "Топ типов СЭМД по документам" and card.get("tab") == "archive":
            apply_semd_volume_table(card)
        elif name == "Детализация контроля качества":
            apply_quality_detail(card)

        fix_viz(card.setdefault("visualization_settings", {}), display=card.get("display", "table"))

        if name == "Последние операции":
            apply_latest_operations(card)
        elif name == "Статусы за период":
            apply_status_period(card)

        if name in MODEL_DRILL_BY_NAME:
            target = MODEL_DRILL_TARGET_BY_NAME.get(name, DOCUMENTS_MODEL_REF)
            card["click_behavior"] = build_model_drill(target, MODEL_DRILL_BY_NAME[name])
            params = MODEL_DRILL_DASHBOARD_PARAMS.get(name)
            if params:
                fields = MODEL_PARAM_FIELDS[target]
                card["metabase-model-drill-params"] = {key: fields[key] for key in params}
        elif name in DRILL_BY_NAME and DRILL_BY_NAME[name]:
            card["click_behavior"] = build_drill(DRILL_BY_NAME[name])
        elif name in DRILL_BY_NAME:
            card.pop("click_behavior", None)

        if card.get("tab") == "operational":
            card.pop("click_behavior", None)
            card.pop("metabase-model-drill-params", None)

        if name in SLIM_FILTERS and card.get("dataset_query", {}).get("type") == "native":
            nat = card["dataset_query"]["native"]
            nat["query"] = set_filter_block(nat["query"], SLIM_FILTERS[name])

        if name == "Объём по СЭМД" and card.get("tab") == "operational":
            continue

        filtered.append(card)

    errors_top = next(
        (c for c in filtered if c.get("name") == "Топ по типу ошибки" and c.get("tab") == "errors"),
        None,
    )
    if errors_top and not any(
        c.get("name") == "Топ по типу ошибки" and c.get("tab") == "operational" for c in filtered
    ):
        operational_top = deepcopy(errors_top)
        operational_top["tab"] = "operational"
        filtered.append(operational_top)

    dash["cards"] = filtered
    restore_archive_top_semd(dash)
    normalize_dashboard(dash)
    # После нормализации: вкладка «Отправленные» задаёт собственный блок фильтров
    # (pending_segment вместо status), который общий проход не должен переписывать.
    apply_sent_tab(dash)


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
    normalize_dashboard(dash)
    return write_json_if_changed(path, dash)


def restore_archive_top_semd(dash: dict) -> None:
    if any(c.get("name") == "Топ типов СЭМД по документам" for c in dash["cards"]):
        return
    query = (
        "WITH base AS ( SELECT semd_label, COUNT(DISTINCT dwh_id)::bigint AS cnt "
        "FROM public.rpt_documents WHERE 1=1 [[AND {{ips_date}}]] [[AND {{semd_type}}]] "
        "[[AND {{jid}}]] [[AND {{local_uid}}]] [[AND {{relates_to}}]] [[AND {{emdr_id}}]] "
        "[[AND {{status}}]] [[AND {{log_id}}]] GROUP BY 1 ), "
        "totals AS (SELECT COALESCE(SUM(cnt), 0)::numeric AS total FROM base) "
        'SELECT semd_label AS "СЭМД", cnt AS "Документов", '
        'ROUND(100.0 * cnt / NULLIF((SELECT total FROM totals), 0), 1) AS "%" '
        "FROM base ORDER BY cnt DESC"
    )
    ff = {
        k: {"table_ref": "public.rpt_documents", "field_name": v}
        for k, v in {
            "jid": "clinic_label", "ips_date": "ips_date", "semd_type": "semd_label",
            "local_uid": "semd_local_uid", "relates_to": "relates_to_msgid",
            "emdr_id": "semd_emdr_id", "status": "status_detail_label", "log_id": "logid",
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
                    "ips_date": {"widget-type": "date/all-options", "display-name": "По дате обработки",
                                 "id": "f6a00003-0003-4003-8003-000000000001", "name": "ips_date", "type": "dimension"},
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
                {"enabled": True, "name": "СЭМД"},
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
        "tab": "archive",
        "metabase-field-filters": ff,
        "click_behavior": build_drill([("semd_type_filter", "СЭМД")]),
    }
    idx = next(i for i, c in enumerate(dash["cards"]) if c.get("name") == "Всего клиник" and c.get("tab") == "archive")
    dash["cards"].insert(idx + 1, card)


def _patch_client_where(query: str) -> str:
    """JID — опциональная text-переменная; клиника — field filter по clinic_label."""
    q = query
    for old in (
        r"WHERE clinic_jid::text = \{\{client_jid\}\} \[\[AND \{\{clinic_label\}\}\]\]",
        r"WHERE 1=1 \[\[AND \{\{client_jid\}\}\]\] \[\[AND \{\{clinic_label\}\}\]\]",
        r"WHERE 1=1 \[\[AND \{\{clinic_label\}\}\]\] \[\[AND \{\{client_jid\}\}\]\]",
    ):
        q = re.sub(
            old,
            "WHERE 1=1 [[AND {{clinic_label}}]] [[AND clinic_jid::text = {{client_jid}}]]",
            q,
        )
    return q.replace("[[AND {{client_jid}}]]", "[[AND clinic_jid::text = {{client_jid}}]]")


def _ensure_client_jid_text_tag(tags: dict) -> None:
    cj = tags.setdefault(
        "client_jid",
        {
            "type": "text",
            "display-name": "JID Клиники",
            "name": "client_jid",
            "required": False,
            "widget-type": "string/=",
        },
    )
    cj["type"] = "text"
    cj["required"] = False
    cj.setdefault("widget-type", "string/=")
    cj.setdefault("display-name", "JID Клиники")


def ensure_client_service_linked_clinic_filters(dash: dict) -> None:
    """07: связанные field filters JID ↔ clinic_label (код · наименование)."""
    if dash.get("name") != "Клиентский дашборд. Мониторинг сервиса интеграции с ЕГИСЗ":
        return
    for p in dash.get("parameters", []):
        if p.get("slug") == "client_jid_filter":
            p["name"] = "JID Клиники"
            p.pop("default", None)
            p.pop("required", None)
            p["values_query_type"] = "search"
            p.pop("values_source_type", None)
            p.pop("values_source_config", None)
            p["filteringParameters"] = [CLIENT_CLINIC_PARAM_ID]
        elif p.get("slug") in ("clinic_name", "clinic_label"):
            p["name"] = "Клиника"
            p["slug"] = "clinic_label"
            p["values_query_type"] = "search"
            p.pop("values_source_type", None)
            p.pop("values_source_config", None)
            p["filteringParameters"] = [CLIENT_JID_PARAM_ID]
    for card in dash.get("cards", []):
        if card.get("display") == "text" and "{{clinic_name}}" in (card.get("text") or ""):
            card["text"] = card["text"].replace("{{clinic_name}}", "{{clinic_label}}")
    tag_counter = 0
    for card in dash.get("cards", []):
        dq = card.get("dataset_query") or {}
        if dq.get("type") != "native":
            continue
        native = dq.setdefault("native", {})
        query = native.get("query") or ""
        if "{{client_jid}}" not in query:
            continue
        native["query"] = _patch_client_where(query)
        tags = native.setdefault("template-tags", {})
        _ensure_client_jid_text_tag(tags)
        tags.pop("clinic_name", None)
        if "clinic_label" not in tags:
            tag_counter += 1
            tags["clinic_label"] = {
                "type": "dimension",
                "display-name": "Клиника",
                "id": f"07c{tag_counter:04x}0000-0000-4000-8000-00000000{tag_counter:04x}",
                "name": "clinic_label",
                "widget-type": "string/=",
            }
        # Фильтр клиники — на грейне источника карточки: витрина типов СЭМД в обмене несёт
        # собственный clinic_label; если запрос смешивает обе документные таблицы
        # (period_docs из rpt_documents + join rpt_error_breakdown), привязываем к
        # rpt_documents, иначе предикат {{clinic_label}} в period_docs не развернётся.
        if "public.rpt_clinic_semd_activity" in native["query"]:
            source = "public.rpt_clinic_semd_activity"
        elif "public.rpt_documents" in native["query"]:
            source = "public.rpt_documents"
        else:
            source = "public.rpt_error_breakdown"
        ff = dict(card.get("metabase-field-filters") or {})
        ff.pop("client_jid", None)
        ff["clinic_label"] = {"table_ref": source, "field_name": "clinic_label"}
        ff.pop("clinic_name", None)
        card["metabase-field-filters"] = ff


def fix_client_sql(query: str) -> str:
    query = query.replace("[[AND {{client_semd_code_name}}]]", "[[AND {{client_document_type}}]]")
    query = query.replace("[[AND {{client_semd_label}}]]", "[[AND {{client_document_type}}]]")
    query = query.replace("{{clinic_jid}}", "{{client_jid}}")
    return query.replace("clinic_jid = {{client_jid}}", "clinic_jid::text = {{client_jid}}")


def apply_client_dashboards(path: Path) -> bool:
    if not path.exists():
        return False
    dash = json.loads(path.read_text(encoding="utf-8"))
    ensure_client_service_linked_clinic_filters(dash)
    for card in dash.get("cards", []):
        filters = card.get("metabase-field-filters") or {}
        doc_type = filters.get("client_document_type")
        if isinstance(doc_type, dict) and doc_type.get("field_name") == "semd_code_name":
            doc_type["field_name"] = "semd_label"
        if card.get("name") == "Динамика статусов по дням" and card.get("dataset_query", {}).get("type") == "native":
            native = card["dataset_query"]["native"]
            native["query"] = CLIENT_STATUS_BY_DAY_QUERY
            tags = native.setdefault("template-tags", {})
            tags.setdefault(
                "client_document_type",
                {
                    "widget-type": "string/=",
                    "display-name": "Тип документа",
                    "id": "07c50000-0000-4000-8000-000000000002",
                    "name": "client_document_type",
                    "type": "dimension",
                },
            )
            ff = dict(card.get("metabase-field-filters") or {})
            ff["client_document_type"] = {
                "table_ref": "public.rpt_documents",
                "field_name": "semd_label",
            }
            card["metabase-field-filters"] = ff
            card["description"] = (
                "Доли документов по состоянию за день: успешно, ошибка асинхронного "
                "ответа РЭМД, ошибка связи и «В обработке» (сумма = 100%). «Без ответа» "
                "исключено — см. карточку «Отправленные — клиент»."
            )
            card["display"] = "bar"
            viz = card.setdefault("visualization_settings", {})
            for key in list(viz.keys()):
                if (
                    key.startswith("graph.")
                    or key.startswith("pie.")
                    or key in ("series_settings", "stackable.stack_type")
                ):
                    del viz[key]
            viz["graph.dimensions"] = ["Дата"]
            viz["graph.metrics"] = ["Async ошибки, %", "Сетевые ошибки, %", "Успешно, %"]
            viz["graph.x_axis.scale"] = "timeseries"
            viz["graph.x_axis.title_text"] = "День"
            viz["graph.y_axis.title_text"] = "% документов"
            viz["graph.show_values"] = False
            viz["stackable.stack_type"] = "stacked"
            viz["series_settings"] = {
                "Async ошибки, %": {"axis": "left", "color": "#A989C5"},
                "Сетевые ошибки, %": {"axis": "left", "color": "#F2994A"},
                "Успешно, %": {"color": "#689636"},
            }
            viz["column_settings"] = {
                '["name","Async ошибки, %"]': {
                    "decimals": 1,
                    "number_separators": ", ",
                    "suffix": " %",
                },
                '["name","Сетевые ошибки, %"]': {
                    "decimals": 1,
                    "number_separators": ", ",
                    "suffix": " %",
                },
                '["name","Успешно, %"]': {
                    "decimals": 1,
                    "number_separators": ", ",
                    "suffix": " %",
                },
            }
        dq = card.get("dataset_query", {})
        if dq.get("type") == "native":
            dq["native"]["query"] = fix_client_sql(fix_sql(dq["native"]["query"]))
        fix_viz(card.get("visualization_settings") or {}, display=card.get("display", "table"))
    normalize_dashboard(dash)
    return write_json_if_changed(path, dash)


def build_retired_objects() -> dict[str, list[str]]:
    """Имена объектов, выведенных из обращения, — вход для архивирования при импорте.

    Коллекция живёт на общем Metabase, поэтому импорт не может архивировать всё, чего нет
    в наших JSON: под такое правило попадает любая чужая карточка, положенная в коллекцию.
    Архивируется только то, что мы сами вывели, — прежние имена из карт переименований и
    явные списки снятых объектов, за вычетом имён, которые сейчас в работе.
    """
    live_cards = set(expected_card_names())
    retired_cards = set(RENAME_01) | set(ARCHIVE_FROM_OPERATIONAL) | set(RETIRED_CARD_NAMES)
    for mapping in RENAME_OTHER.values():
        retired_cards |= set(mapping)

    live_dashboards = {
        json.loads(path.read_text(encoding="utf-8")).get("name")
        for path in sorted((ROOT / "metabase_dashboards").glob("*.json"))
    }
    live_models = {
        json.loads(path.read_text(encoding="utf-8")).get("name")
        for path in sorted((ROOT / "metabase_models").glob("*.json"))
    }
    return {
        "cards": sorted(retired_cards - live_cards),
        "dashboards": sorted(RETIRED_DASHBOARD_NAMES - live_dashboards),
        "models": sorted(RETIRED_MODEL_NAMES - live_models),
    }


def expected_card_names() -> set[str]:
    """Имена карточек, присутствующих в собранных дашбордах."""
    names: set[str] = set()
    for path in sorted((ROOT / "metabase_dashboards").glob("*.json")):
        dash = json.loads(path.read_text(encoding="utf-8"))
        for card in dash.get("cards", []):
            if card.get("name") and card.get("display") != "text":
                names.add(card["name"])
    return names


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

    # Список считается ПОСЛЕ пересборки дашбордов: в него не должно попасть имя,
    # которое этим же прогоном вернулось в работу.
    retired_path = ROOT / "metabase" / "retired-objects.json"
    if write_json_if_changed(retired_path, build_retired_objects()):
        print(f"Updated {retired_path}")


if __name__ == "__main__":
    main()
