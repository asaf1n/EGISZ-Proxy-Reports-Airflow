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


def pending_segments() -> tuple[tuple[str, str, int | None, bool], ...]:
    """Лестница ступеней обработки из сида `dim_pending_segments` в db/01_schema.sql.

    Возвращает кортежи (код, наименование, граница в минутах, признак последней ступени)
    в порядке лестницы. Карточки берут перечень колонок и подписи отсюда, поэтому
    переименование или добавление ступени в справочнике доходит до дашборда следующей
    пересборкой и не требует правки SQL.
    """
    schema_sql = (ROOT / "db" / "01_schema.sql").read_text(encoding="utf-8")
    start = schema_sql.index("INSERT INTO dim_pending_segments")
    block = schema_sql[start : schema_sql.index("ON CONFLICT", start)]
    rows = re.findall(
        r"\('(\w+)',\s*'((?:[^']|'')+)',\s*(\d+|NULL),\s*(\d+),\s*(true|false)\)", block
    )
    return tuple(
        (code, label.replace("''", "'"), None if bound == "NULL" else int(bound), terminal == "true")
        for code, label, bound, _sort, terminal in sorted(rows, key=lambda row: int(row[3]))
    )


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

# Фильтр «Очередь на дату» снят: очередь — состояние на текущий момент, второй даты
# на вкладке нет. Слаг и идентификатор остаются здесь, чтобы прогон убирал параметр
# и его привязки с уже опубликованных дашбордов.
RETIRED_PARAM_IDS = {"period_end_filter": "f1a2b3c4-d5e6-4789-a01b-0123456789c1"}

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
    # Воронка читалась как конверсия, хотя ступени — вложенные подмножества одного
    # набора: проценты считались от первой ступени, а спад отражал профиль поступления.
    # Кумулятивный взгляд остался кривой дожития, распределение — гистограммой.
    "В обработке на конец периода",
    # Полосы «объект × ступень» заменены сводными таблицами: концентрация и профиль
    # ожидания читаются в одной сетке, а не в двух графиках с топ-15 и топ-20.
    "Ожидание ответа по клиникам и ступеням",
    "Ожидание ответа по типам СЭМД и ступеням",
    # Распределение очереди осталось одной карточкой — «Текущая очередь по ступеням»
    # на оперативном мониторинге; вторая была тем же графиком под другим якорем.
    "Возраст очереди по ступеням",
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

# Лестница ступеней — данные справочника. Рабочие ступени несут очередь, последняя
# замыкает лестницу и означает, что ответа уже не ждут (README §«Учёт отправленных»).
PENDING_SEGMENTS = pending_segments()
WORKING_SEGMENTS = tuple(row for row in PENDING_SEGMENTS if not row[3])

# Вкладка «Отправленные»: состояние очереди → распределение возраста → локализация
# причины → движение. Имена карточек — подписи на дашборде, поэтому объявлены здесь.
#
# Очередь считается на текущий момент и фильтру периода не подчиняется: период режет
# выборку по дате обработки, а очередь — состояние на момент, и документ, отправленный
# до начала периода, из неё выпадал бы. Оговорка вынесена в наименование, чтобы читатель
# не искал причину, почему карточка не отзывается на фильтр вкладки.
NO_PERIOD_SUFFIX = " (без фильтра периода)"


def _queue_rescue_range() -> str:
    """Диапазон последней рабочей ступени из справочника: «7–15 суток».

    Подписи ступеней — «до 7 суток» / «до 15 суток»; общая единица измерения у соседних
    границ схлопывается в одну, чтобы имя читалось как интервал, а не как два срока.
    """
    bounds = [label.removeprefix("до ") for _code, label, _minutes, _terminal in WORKING_SEGMENTS[-2:]]
    lower, upper = bounds[0], bounds[-1]
    unit = upper.split(" ", 1)[-1]
    return f"{lower.removesuffix(' ' + unit)}–{upper}"


QUEUE_SIZE_NAME = f"Документов в очереди{NO_PERIOD_SUFFIX}"
QUEUE_OVER_24H_NAME = f"Ожидают > 24 часов{NO_PERIOD_SUFFIX}"
QUEUE_RESCUE_NAME = f"Ждут {_queue_rescue_range()}{NO_PERIOD_SUFFIX}"
QUEUE_MAX_AGE_NAME = f"Максимальный возраст в очереди{NO_PERIOD_SUFFIX}"
# Распределение очереди по ступеням — одна карточка на оперативном мониторинге.
# Срез профиля ожидания разбирается кривой долей и матрицами на вкладке «Отправленные».
QUEUE_NOW_NAME = f"Текущая очередь по ступеням{NO_PERIOD_SUFFIX}"
QUEUE_SURVIVAL_NAME = f"Доля ожидающих дольше срока{NO_PERIOD_SUFFIX}"
QUEUE_PIVOT_CLINIC_NAME = f"Клиника × ступень{NO_PERIOD_SUFFIX}"
QUEUE_PIVOT_SEMD_NAME = f"Тип СЭМД × ступень{NO_PERIOD_SUFFIX}"
QUEUE_FLOW_DAYS = 7
QUEUE_FLOW_NAME = f"Движение очереди за {QUEUE_FLOW_DAYS} суток{NO_PERIOD_SUFFIX}"
QUEUE_TAIL_NAME = f"Доля хвоста по неделям{NO_PERIOD_SUFFIX}"

# Переименования, ещё НЕ применённые на целевых контурах. Карта не архив: как только
# прогон импорта прошёл везде, запись отсюда убирается — прежнее имя уже не встретится,
# а копить цепочки «имя → имя → имя» значит хранить историю в рабочем коде.
# Снятые с дашборда карточки живут не здесь, а в RETIRED_CARD_NAMES.
RENAME_01 = {
    # Окно движения объявлено в имени: «за период» путалось с фильтром вкладки.
    "Движение очереди за период": QUEUE_FLOW_NAME,
    "Без ответа": SENT_STATE_NO_RESPONSE_LABEL,
    "Документы без ответа": SENT_TABLE_NAME_NO_RESPONSE,
    "Объём документов по дням": "Динамика документов по дням",
    # Карточки очереди: оговорка о фильтре периода в наименовании, внутренние слова
    # («якорь», «дожитие», «окно спасения») заменены на язык предметной области.
    "Документов в очереди": QUEUE_SIZE_NAME,
    "Ожидают > 24 часов": QUEUE_OVER_24H_NAME,
    "Окно спасения (7–15 суток)": QUEUE_RESCUE_NAME,
    "Максимальный возраст в очереди": QUEUE_MAX_AGE_NAME,
    "Текущая очередь по ступеням": QUEUE_NOW_NAME,
    "Кривая дожития очереди": QUEUE_SURVIVAL_NAME,
    "Клиника × ступень": QUEUE_PIVOT_CLINIC_NAME,
    "Тип СЭМД × ступень": QUEUE_PIVOT_SEMD_NAME,
    "Движение очереди за 7 суток": QUEUE_FLOW_NAME,
    "Доля хвоста по неделям": QUEUE_TAIL_NAME,
}

# Переименования по остальным дашбордам — так же только не применённые.
RENAME_OTHER: dict[str, dict[str, str]] = {
    # Имя и порог «>7 дн.» пережили лестницу ступеней: терминальная граница живёт в
    # dim_pending_segments (сейчас 15 суток), а состояние называется «Ответ не получен».
    "05_executive.json": {"Зависших >7 дн., %": "Ответ не получен, %"},
}

EXECUTIVE_NO_RESPONSE_NAME = "Ответ не получен, %"

# Доля документов, по которым ответа уже не ждут. Порог не объявляется — состояние
# приходит из справочника (README §«Учёт отправленных»), ужесточение делается UPDATE'ом.
EXECUTIVE_NO_RESPONSE_QUERY = (
    "SELECT ROUND(100.0 * COUNT(DISTINCT dwh_id) FILTER (WHERE sent_state = 'no_response') "
    "/ NULLIF(COUNT(DISTINCT dwh_id), 0), 1) AS \"Ответ не получен, %\" "
    "FROM public.rpt_documents WHERE 1=1 [[AND {{ips_date}}]] [[AND {{jid}}]]"
)

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
    "РЭМД vs связь": [("status_detail_label", "Статус")],
    "Объём по клиникам": [("clinic_jid", "JID Клиники")],
    "Успешность по клиникам": [("clinic_jid", "JID Клиники")],
    "Объём ошибок по клиникам": [("clinic_jid", "JID Клиники")],
    "Топ типов СЭМД по документам": [("semd_label", "СЭМД")],
    "Успешность по типам СЭМД": [("semd_label", "СЭМД")],
    "Топ типов СЭМД по ошибкам": [("semd_label", "СЭМД")],
    "Топ типов СЭМД по видам ошибки": [("semd_code", "СЭМД")],
    # Строка матрицы — объект целиком (ступени разложены по колонкам), поэтому дрилл
    # уносит в модель объект строки; разрез по ступени задаётся фильтром вкладки.
    QUEUE_PIVOT_CLINIC_NAME: [("clinic_label", "Клиника")],
    QUEUE_PIVOT_SEMD_NAME: [("semd_code", "Код СЭМД")],
    "Ошибки: тип × клиника": [
        ("error_types", "Тип ошибки", "contains"),
        ("clinic_jid", "JID Клиники"),
    ],
}

# Целевая модель дрилла по карточке (по умолчанию — «Документы»).
MODEL_DRILL_TARGET_BY_NAME: dict[str, str] = {
    "Топ типов СЭМД по ошибкам": ERROR_BREAKDOWN_MODEL_REF,
    "Топ типов СЭМД по видам ошибки": ERROR_BREAKDOWN_MODEL_REF,
    QUEUE_PIVOT_CLINIC_NAME: SENT_MODEL_REF,
    QUEUE_PIVOT_SEMD_NAME: SENT_MODEL_REF,
}

# Активные фильтры дашборда, переносимые в модель (без измерения-грейна самой строки).
MODEL_DRILL_DASHBOARD_PARAMS: dict[str, list[str]] = {
    "Последние операции": ["ips_date", "semd_type", "status"],
    "Статусы за период": ["ips_date", "semd_type", "jid"],
    "РЭМД vs связь": ["ips_date", "semd_type", "jid"],
    "Объём по клиникам": ["ips_date", "semd_type", "status"],
    "Успешность по клиникам": ["ips_date", "semd_type", "status"],
    "Объём ошибок по клиникам": ["ips_date", "semd_type", "status"],
    "Топ типов СЭМД по документам": ["ips_date", "jid", "status"],
    "Успешность по типам СЭМД": ["ips_date", "jid", "status"],
    "Топ типов СЭМД по ошибкам": ["ips_date", "jid"],
    "Топ типов СЭМД по видам ошибки": ["ips_date", "jid"],
    # pending_segment не переносится: ступень — измерение самой ячейки (см. MODEL_DRILL_BY_NAME).
    # Период тоже: очередь — состояние на якорь, а не выборка за период.
    QUEUE_PIVOT_CLINIC_NAME: ["semd_type"],
    QUEUE_PIVOT_SEMD_NAME: ["jid"],
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
    {"enabled": True, "name": "ns2_error"},
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
    '["name","ns2_error"]': {"column_title": "ns2_error", "text_style": "wrap"},
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
STATUS_BY_DAY_QUERY = (
    "SELECT ips_date::date AS \"Дата\", status_detail_label AS \"Статус\", "
    "COUNT(DISTINCT dwh_id)::bigint AS \"Документов\" "
    "FROM public.rpt_documents WHERE status_detail <> 'no_response' "
    "[[AND {{ips_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] "
    "GROUP BY ips_date::date, status_detail_label, status_detail_sort "
    "ORDER BY ips_date::date, status_detail_sort"
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

# «ns2_error» — исходный текст <message> ответа РЭМД (documents.error_text) рядом с
# каноническими типами: типы отвечают «что это за отказ», текст — «что именно ответил
# РЭМД по этому документу», и при разборе инцидента нужен именно он.
LATEST_OPERATIONS_QUERY = (
    "SELECT ips_date AS \"Дата обработки\", status_detail_label AS \"Статус\", "
    "clinic_label AS \"Клиника\", clinic_host AS \"Host Клиники (ГОСТ VPN)\", "
    "semd_label AS \"СЭМД\", semd_local_uid AS \"localUid СЭМД\", "
    "semd_emdr_id AS \"Рег. Номер РЭМД\", error_types AS \"Типы ошибки\", "
    "error_text AS \"ns2_error\" "
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


# Обе таблицы читают от самых старых: наверху списка то, что ждёт дольше всех и требует
# разбора. Свежие отправки в верхних строках вытесняли застрявшие — ровно тот случай,
# ради которого таблицы и разделены по состоянию.
SENT_TABLE_PENDING_QUERY = _sent_table_query("pending", "first_sent_at ASC NULLS LAST")
SENT_TABLE_NO_RESPONSE_QUERY = _sent_table_query("no_response", "first_sent_at ASC NULLS LAST")

# Формат колонок таблиц разбора: только для колонок, которые запрос действительно отдаёт.
SENT_TABLE_COLUMN_SETTINGS = {
    '["name","Подач в ЕГИСЗ"]': {
        "column_title": "Подач в ЕГИСЗ",
        "decimals": 0,
        "number_separators": ", ",
    },
    '["name","Суток с отправки"]': {
        "column_title": "Суток с отправки",
        "decimals": 1,
        "number_separators": ", ",
    },
    '["name","Дата отправки"]': {
        "column_title": "Дата отправки",
        "date_abbreviate": True,
        "date_style": "D MMMM, YYYY",
        "time_style": "HH:mm",
    },
}

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


SENT_NO_RESPONSE_QUERY = (
    'SELECT COUNT(DISTINCT semd_local_uid)::bigint AS "Документов" '
    "FROM public.rpt_documents_sent "
    f"WHERE sent_state = 'no_response' {SENT_FILTERS}"
)

# Очередь — состояние на текущий момент, а не выборка за период: границы периода к ней
# неприменимы. Документ, отправленный до начала периода и не получивший ответа, в очереди
# стоит — отбор по дате отправки выбрасывал бы его и занижал очередь. Остальные срезы
# очередь по времени не режут и остаются. Ступень биндится к справочнику: колонка ступени
# в rpt_documents посчитана от того же момента.
SENT_QUEUE_FIELD_FILTERS = {
    "semd_type": {"table_ref": "public.rpt_documents", "field_name": "semd_label"},
    "jid": {"table_ref": "public.rpt_documents", "field_name": "clinic_label"},
    "pending_segment": {"table_ref": "public.dim_pending_segments", "field_name": "label"},
}

# Набор фильтров у карточек очереди один и тот же; ось-лестница у распределений — не
# фильтр, поэтому у кривой доли, движения и хвоста ступень не биндится (справочник там
# второй экземпляр, разрез по одной ступени ломал бы кумулятивный ряд и баланс потока).
SENT_QUEUE_FILTERS = "[[AND {{semd_type}}]] [[AND {{jid}}]] [[AND {{pending_segment}}]]"
SENT_QUEUE_SLICE_FILTERS = "[[AND {{semd_type}}]] [[AND {{jid}}]]"

# Набор документов очереди — один на все карточки блока: отправлен, первого ответа на
# момент ещё нет, ступень рабочая. Последняя ступень («ответа уже не ждут») в очередь не
# входит нигде: у неё своя пара карточек на этой же вкладке. now() в пределах запроса
# неизменен, поэтому отдельного якоря карточкам не нужно.
#
# Таблицы остаются в FROM без алиасов: field filters Metabase разворачиваются
# в "public"."<таблица>"."<колонка>".
QUEUE_MEMBERSHIP = (
    "public.is_pending_at(public.rpt_documents.first_sent_at, "
    "public.rpt_documents.first_callback_at, now())"
)

SENT_QUEUE_CORPUS = (
    "WITH queue AS ( SELECT public.rpt_documents.dwh_id, "
    "public.rpt_documents.clinic_label, "
    "COALESCE(NULLIF(TRIM(public.rpt_documents.semd_code), ''), '(неизвестно)') AS semd_code, "
    "public.dim_pending_segments.code AS segment_code, "
    "public.dim_pending_segments.sort_order AS segment_sort, "
    "public.dim_pending_segments.label AS segment_label, "
    "EXTRACT(EPOCH FROM (now() - public.rpt_documents.first_sent_at)) / 60.0 AS age_minutes "
    "FROM public.rpt_documents "
    "JOIN public.dim_pending_segments ON public.dim_pending_segments.code = "
    "public.pending_segment_code_at(public.rpt_documents.first_sent_at, now()) "
    f"WHERE {QUEUE_MEMBERSHIP} "
    "AND NOT public.dim_pending_segments.is_no_response "
    f"{SENT_QUEUE_FILTERS} )"
)

# Ступени в порядке лестницы: светло-синий → фиолетовый → жёлтый → оранжевый → красный.
# Цвет несёт то же, что и позиция сегмента, — чем дольше ожидание, тем «горячее» полоса.
# Терминальная ступень отделена серым: это не работа, а исход (README §«Учёт отправленных»).
SENT_STATE_COLORS: dict[str, str] = {
    "pending": STATUS_DETAIL_COLORS["В обработке"]["color"],
    "no_response": STATUS_DETAIL_COLORS["Без ответа"]["color"],
}

# Лестница цветов идёт по позиции ступени, а не по её подписи: переименование ступени
# в справочнике не должно перекрашивать график. Ступеней в справочнике может стать больше,
# чем оттенков, — хвост донашивает последний.
SEGMENT_COLOR_LADDER = (
    "#A6C8E8", "#7FB0DF", "#B79AD3", "#A989C5", "#EDC948", "#F2B056", "#F28E2B", "#E15759",
)

SEGMENT_COLOR_BY_CODE: dict[str, str] = {
    code: SEGMENT_COLOR_LADDER[min(index, len(SEGMENT_COLOR_LADDER) - 1)]
    for index, (code, _label, _minutes, _terminal) in enumerate(WORKING_SEGMENTS)
}

# Подпись ступени приходит из справочника, цвет — из лестницы позиций.
SENT_SEGMENT_COLORS: dict[str, str] = {
    label: SEGMENT_COLOR_BY_CODE[code] for code, label, _minutes, _terminal in WORKING_SEGMENTS
}

QUEUE_TAIL_COLORS = tuple(
    SEGMENT_COLOR_BY_CODE[code] for code in ("p_24h", "p_72h", "p_15d")
)


def _column_key(name: str) -> str:
    """Ключ настроек колонки в visualization_settings."""
    return json.dumps(["name", name], ensure_ascii=False, separators=(",", ":"))

# Пороговая ступень «окна спасения» — последняя рабочая: дальше только терминальная.
# Значение порога берётся из справочника, в карточке его нет.
QUEUE_LAST_WORKING_SEGMENT = (
    "(SELECT MAX(sort_order) FROM public.dim_pending_segments WHERE NOT is_no_response)"
)

# Ряд 1 — состояние очереди. Тренд-плитки читают ряд по дням: Metabase сравнивает
# последнюю точку с предыдущей, поэтому ряд обязан заканчиваться текущим моментом.
QUEUE_TREND_DAYS = 14

QUEUE_TREND_CORPUS = (
    f"WITH win AS ( SELECT date_trunc('day', now()) - INTERVAL '{QUEUE_TREND_DAYS - 1} days' AS start_ts, "
    "now() AS end_ts ), "
    "points AS ( SELECT gs AS day_start, LEAST(gs + INTERVAL '1 day', w.end_ts) AS ts "
    "FROM win w CROSS JOIN generate_series(w.start_ts, date_trunc('day', w.end_ts), INTERVAL '1 day') gs ), "
    # Кандидат — документ, который мог стоять в очереди хотя бы в одной точке окна:
    # либо он в ней уже стоял на левой границе, либо отправлен внутри окна.
    "candidates AS ( SELECT public.rpt_documents.dwh_id, public.rpt_documents.first_sent_at, "
    "public.rpt_documents.first_callback_at "
    "FROM public.rpt_documents CROSS JOIN win w "
    "WHERE public.rpt_documents.first_sent_at <= w.end_ts "
    "AND (public.rpt_documents.first_sent_at >= w.start_ts "
    "OR public.is_pending_at(public.rpt_documents.first_sent_at, "
    "public.rpt_documents.first_callback_at, w.start_ts)) "
    f"{SENT_QUEUE_SLICE_FILTERS} )"
)

# Очередь в точке ряда — тот же набор документов, что и в блоке: членство плюс рабочая
# ступень. Ступень считается на СВОЙ момент ряда, поэтому справочник соединяется внутри
# бокового подзапроса; LEFT JOIN LATERAL сохраняет дни с пустой очередью нулями.
QUEUE_TREND_POINT_QUEUE = (
    "LEFT JOIN LATERAL ( SELECT c.dwh_id, "
    "EXTRACT(EPOCH FROM (p.ts - c.first_sent_at)) / 60.0 AS age_minutes "
    "FROM candidates c "
    "JOIN public.dim_pending_segments ON public.dim_pending_segments.code = "
    "public.pending_segment_code_at(c.first_sent_at, p.ts) "
    "WHERE public.is_pending_at(c.first_sent_at, c.first_callback_at, p.ts) "
    "AND NOT public.dim_pending_segments.is_no_response "
    "[[AND {{pending_segment}}]] ) q ON TRUE"
)

QUEUE_SIZE_QUERY = (
    QUEUE_TREND_CORPUS
    + ' SELECT p.day_start::date AS "Дата", '
    'COUNT(DISTINCT q.dwh_id)::bigint AS "Документов в очереди" '
    f"FROM points p {QUEUE_TREND_POINT_QUEUE} GROUP BY 1 ORDER BY 1"
)

QUEUE_OVER_24H_QUERY = (
    QUEUE_TREND_CORPUS
    + ", threshold AS ( SELECT max_age_minutes AS minutes "
    "FROM public.dim_pending_segments WHERE code = 'p_24h' ) "
    'SELECT p.day_start::date AS "Дата", '
    "COUNT(DISTINCT q.dwh_id) FILTER (WHERE q.age_minutes > t.minutes)"
    '::bigint AS "Ожидают > 24 часов" '
    f"FROM points p CROSS JOIN threshold t {QUEUE_TREND_POINT_QUEUE} "
    "GROUP BY 1 ORDER BY 1"
)

QUEUE_RESCUE_QUERY = (
    SENT_QUEUE_CORPUS
    + ' SELECT COUNT(DISTINCT dwh_id)::bigint AS "Документов" FROM queue '
    f"WHERE segment_sort = {QUEUE_LAST_WORKING_SEGMENT}"
)

QUEUE_MAX_AGE_QUERY = (
    SENT_QUEUE_CORPUS
    + ' SELECT ROUND(MAX(age_minutes) / 1440.0, 1) AS "Суток в очереди" FROM queue'
)

# Распределение возраста. Ступени взаимоисключающие: документ попадает ровно в одну,
# сумма столбцов равна размеру очереди.
QUEUE_NOW_QUERY = (
    SENT_QUEUE_CORPUS
    + ' SELECT segment_label AS "Ступень обработки", '
    'COUNT(DISTINCT dwh_id)::bigint AS "Документов" '
    "FROM queue GROUP BY segment_label, segment_sort ORDER BY segment_sort"
)

# Кумулятивный взгляд на тот же набор документов: доля очереди, которая ждёт дольше
# границы ступени. Подпись оси называет то, что под ней стоит: справочник даёт «до N»,
# ряд же считает документы старше N. Второй ряд — та же кривая неделю назад:
# сравнивается форма ожидания, а не размер очереди.
QUEUE_SURVIVAL_SLICE_NOW = "Сейчас"
QUEUE_SURVIVAL_SLICE_WEEK_AGO = "Неделю назад"

QUEUE_SURVIVAL_QUERY = (
    f"WITH slices AS ( SELECT '{QUEUE_SURVIVAL_SLICE_NOW}' AS slice, 1 AS slice_sort, now() AS ts "
    f"UNION ALL SELECT '{QUEUE_SURVIVAL_SLICE_WEEK_AGO}', 2, now() - INTERVAL '7 days' ), "
    "queue AS ( SELECT s.slice, s.slice_sort, public.rpt_documents.dwh_id, "
    "EXTRACT(EPOCH FROM (s.ts - public.rpt_documents.first_sent_at)) / 60.0 AS age_minutes "
    "FROM public.rpt_documents CROSS JOIN slices s "
    "JOIN public.dim_pending_segments ON public.dim_pending_segments.code = "
    "public.pending_segment_code_at(public.rpt_documents.first_sent_at, s.ts) "
    "WHERE public.is_pending_at(public.rpt_documents.first_sent_at, "
    "public.rpt_documents.first_callback_at, s.ts) "
    "AND NOT public.dim_pending_segments.is_no_response "
    f"{SENT_QUEUE_SLICE_FILTERS} ), "
    "totals AS ( SELECT slice, COUNT(DISTINCT dwh_id) AS total FROM queue GROUP BY 1 ) "
    "SELECT regexp_replace(g.label, '^до ', 'дольше ') AS \"Срок ожидания\", "
    'q.slice AS "Срез", '
    "ROUND(100.0 * COUNT(DISTINCT q.dwh_id) FILTER (WHERE q.age_minutes > g.max_age_minutes) "
    '/ NULLIF(MAX(t.total), 0), 1) AS "Ещё ждут, %" '
    "FROM public.dim_pending_segments g CROSS JOIN queue q JOIN totals t ON t.slice = q.slice "
    "WHERE NOT g.is_no_response "
    "GROUP BY g.sort_order, g.label, q.slice, q.slice_sort "
    "ORDER BY q.slice_sort, g.sort_order"
)

# Ряд 3 — локализация причины. Сводная таблица «объект × ступень» с условным
# форматированием: концентрация и профиль ожидания видны в одной сетке.
QUEUE_TOTAL_COLUMN = "Всего"


def _queue_matrix_query(dimension: str, label: str) -> str:
    """Матрица «объект × ступень»: ступени — колонками, значение — документы очереди.

    Сводная таблица Metabase доступна только вопросам конструктора запросов, поэтому
    разворот делает сам запрос, а тепловую карту даёт условное форматирование колонок.
    Перечень колонок и их подписи берутся из лестницы справочника, порогов в карточке нет.
    Отбор идёт по коду ступени, а не по подписи: переименование ступени в справочнике
    меняет заголовок колонки и не обнуляет её значения.
    """
    columns = "".join(
        f"COUNT(DISTINCT dwh_id) FILTER (WHERE segment_code = '{code}')::bigint "
        f'AS "{label}", '
        for code, label, _minutes, _terminal in WORKING_SEGMENTS
    )
    return (
        SENT_QUEUE_CORPUS
        + f' SELECT {dimension} AS "{label}", {columns}'
        + f'COUNT(DISTINCT dwh_id)::bigint AS "{QUEUE_TOTAL_COLUMN}" '
        + f'FROM queue GROUP BY 1 ORDER BY "{QUEUE_TOTAL_COLUMN}" DESC, 1'
    )


QUEUE_PIVOT_CLINIC_QUERY = _queue_matrix_query("clinic_label", "Клиника")
QUEUE_PIVOT_SEMD_QUERY = _queue_matrix_query("semd_code", "Код СЭМД")

# Ряд 4 — движение очереди. Очередь рабочих ступеней как поток: что было, что пришло,
# что ушло ответом и что ушло по возрасту. Окно объявлено в имени карточки: фильтр
# периода вкладки к состоянию на момент неприменим, и «за период» в заголовке читалось бы
# как выбранный период.
QUEUE_FLOW_QUERY = (
    f"WITH win AS ( SELECT now() - INTERVAL '{QUEUE_FLOW_DAYS} days' AS start_ts, "
    "now() AS end_ts ), "
    "scope AS ( SELECT public.rpt_documents.dwh_id, public.rpt_documents.first_sent_at, "
    "public.rpt_documents.first_callback_at, w.start_ts, w.end_ts "
    "FROM public.rpt_documents CROSS JOIN win w "
    "WHERE public.rpt_documents.first_sent_at <= w.end_ts "
    "AND (public.rpt_documents.first_sent_at > w.start_ts "
    "OR public.is_pending_at(public.rpt_documents.first_sent_at, "
    "public.rpt_documents.first_callback_at, w.start_ts)) "
    f"{SENT_QUEUE_SLICE_FILTERS} ), "
    "flags AS ( SELECT s.dwh_id, "
    "public.is_pending_at(s.first_sent_at, s.first_callback_at, s.start_ts) AS in_start, "
    "public.is_pending_at(s.first_sent_at, s.first_callback_at, s.end_ts) AS in_end, "
    "(s.first_sent_at > s.start_ts) AS arrived, "
    "g_start.is_no_response AS terminal_start, g_end.is_no_response AS terminal_end "
    "FROM scope s "
    "JOIN public.dim_pending_segments g_start "
    "ON g_start.code = public.pending_segment_code_at(s.first_sent_at, s.start_ts) "
    "JOIN public.dim_pending_segments g_end "
    "ON g_end.code = public.pending_segment_code_at(s.first_sent_at, s.end_ts) ), "
    "moves AS ( SELECT "
    "COUNT(DISTINCT dwh_id) FILTER (WHERE in_start AND NOT terminal_start AND NOT arrived) AS opening, "
    "COUNT(DISTINCT dwh_id) FILTER (WHERE arrived) AS arrived, "
    "COUNT(DISTINCT dwh_id) FILTER (WHERE (arrived OR (in_start AND NOT terminal_start)) "
    "AND NOT in_end) AS answered, "
    "COUNT(DISTINCT dwh_id) FILTER (WHERE (arrived OR (in_start AND NOT terminal_start)) "
    "AND in_end AND terminal_end) AS utilized "
    "FROM flags ) "
    "SELECT 1 AS step_sort, 'Очередь на начало' AS \"Этап\", "
    'opening::bigint AS "Документов" FROM moves '
    "UNION ALL SELECT 2, 'Поступило', arrived::bigint FROM moves "
    "UNION ALL SELECT 3, 'Получили ответ', -answered::bigint FROM moves "
    "UNION ALL SELECT 4, 'Утилизировано', -utilized::bigint FROM moves "
    "ORDER BY 1"
)

# Доля хвоста по неделям: очередь на конец каждой недели и её части старше порогов
# справочника. Считается тем же предикатом на недельных якорях — недельная витрина
# разреза по ступеням не несёт, а фильтры вкладки должны действовать полностью.
QUEUE_TAIL_WEEKS = 12

QUEUE_TAIL_QUERY = (
    "WITH win AS ( SELECT date_trunc('week', now() AT TIME ZONE 'Europe/Moscow') "
    f"- INTERVAL '{QUEUE_TAIL_WEEKS - 1} weeks' AS start_wall, "
    "date_trunc('week', now() AT TIME ZONE 'Europe/Moscow') AS last_wall, now() AS end_ts ), "
    "points AS ( SELECT gs::date AS week_start, "
    "LEAST((gs + INTERVAL '7 days') AT TIME ZONE 'Europe/Moscow', w.end_ts) AS ts "
    "FROM win w CROSS JOIN generate_series(w.start_wall, w.last_wall, INTERVAL '1 week') gs ), "
    "candidates AS ( SELECT public.rpt_documents.dwh_id, public.rpt_documents.first_sent_at, "
    "public.rpt_documents.first_callback_at "
    "FROM public.rpt_documents CROSS JOIN win w "
    "WHERE public.rpt_documents.first_sent_at <= w.end_ts "
    "AND (public.rpt_documents.first_sent_at >= w.start_wall AT TIME ZONE 'Europe/Moscow' "
    "OR public.is_pending_at(public.rpt_documents.first_sent_at, "
    "public.rpt_documents.first_callback_at, w.start_wall AT TIME ZONE 'Europe/Moscow')) "
    f"{SENT_QUEUE_SLICE_FILTERS} ), "
    "thresholds AS ( SELECT "
    "MAX(max_age_minutes) FILTER (WHERE code = 'p_24h') AS m_24h, "
    "MAX(max_age_minutes) FILTER (WHERE code = 'p_72h') AS m_72h, "
    "MAX(max_age_minutes) FILTER (WHERE code = 'p_7d') AS m_7d "
    "FROM public.dim_pending_segments ), "
    # Очередь недели — тот же набор документов, что и в остальном блоке: членство плюс
    # рабочая ступень, посчитанная на конец своей недели.
    "per_point AS ( SELECT p.week_start, c.dwh_id, "
    "public.is_pending_at(c.first_sent_at, c.first_callback_at, p.ts) "
    "AND NOT seg.is_no_response AS pending, "
    "EXTRACT(EPOCH FROM (p.ts - c.first_sent_at)) / 60.0 AS age_minutes "
    "FROM points p LEFT JOIN candidates c ON TRUE "
    "LEFT JOIN public.dim_pending_segments seg "
    "ON seg.code = public.pending_segment_code_at(c.first_sent_at, p.ts) ) "
    'SELECT week_start AS "Неделя", '
    "ROUND(100.0 * COUNT(DISTINCT dwh_id) FILTER (WHERE pending AND age_minutes > t.m_24h) "
    '/ NULLIF(COUNT(DISTINCT dwh_id) FILTER (WHERE pending), 0), 1) AS "> 24 часов, %", '
    "ROUND(100.0 * COUNT(DISTINCT dwh_id) FILTER (WHERE pending AND age_minutes > t.m_72h) "
    '/ NULLIF(COUNT(DISTINCT dwh_id) FILTER (WHERE pending), 0), 1) AS "> 3 суток, %", '
    "ROUND(100.0 * COUNT(DISTINCT dwh_id) FILTER (WHERE pending AND age_minutes > t.m_7d) "
    '/ NULLIF(COUNT(DISTINCT dwh_id) FILTER (WHERE pending), 0), 1) AS "> 7 суток, %" '
    "FROM per_point CROSS JOIN thresholds t GROUP BY week_start ORDER BY week_start"
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

# Карточки очереди: один набор документов, один момент, разная подача.
SENT_QUEUE_CARDS = frozenset(
    {
        QUEUE_SIZE_NAME,
        QUEUE_OVER_24H_NAME,
        QUEUE_RESCUE_NAME,
        QUEUE_MAX_AGE_NAME,
        QUEUE_NOW_NAME,
        QUEUE_SURVIVAL_NAME,
        QUEUE_PIVOT_CLINIC_NAME,
        QUEUE_PIVOT_SEMD_NAME,
        QUEUE_FLOW_NAME,
        QUEUE_TAIL_NAME,
    }
)

# Ступень — ось самой карточки, а не срез: кумулятивный ряд долей, баланс потока и доли
# хвоста по неделям разбирают лестницу целиком, и отбор одной ступени лишил бы их смысла.
# Остальные карточки очереди несут полный набор фильтров.
SENT_QUEUE_AXIS_CARDS = frozenset({QUEUE_SURVIVAL_NAME, QUEUE_FLOW_NAME, QUEUE_TAIL_NAME})
SENT_QUEUE_TREND_CARDS = frozenset({QUEUE_SIZE_NAME, QUEUE_OVER_24H_NAME})

# Состав вкладки: имя, подача, вкладка. Распределение возраста показывается дважды —
# на мониторинге это очередь «прямо сейчас», на «Отправленных» — часть разбора.
SENT_TAB_CARD_PLAN: tuple[tuple[str, str, str], ...] = (
    (SENT_TABLE_NAME_NO_RESPONSE, "table", "sent"),
    (SENT_REGISTRATION_FUNNEL_NAME, "funnel", "sent"),
    (QUEUE_SIZE_NAME, "smartscalar", "sent"),
    (QUEUE_OVER_24H_NAME, "smartscalar", "sent"),
    (QUEUE_RESCUE_NAME, "progress", "sent"),
    (QUEUE_MAX_AGE_NAME, "scalar", "sent"),
    (QUEUE_SURVIVAL_NAME, "line", "sent"),
    (QUEUE_PIVOT_CLINIC_NAME, "pivot", "sent"),
    (QUEUE_PIVOT_SEMD_NAME, "pivot", "sent"),
    (QUEUE_FLOW_NAME, "waterfall", "sent"),
    (QUEUE_TAIL_NAME, "line", "sent"),
    (QUEUE_NOW_NAME, "bar", "operational"),
)

QUEUE_TAIL_METRICS = ("> 24 часов, %", "> 3 суток, %", "> 7 суток, %")

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

SENT_TAB_QUERIES: dict[str, str] = {
    SENT_STATE_NO_RESPONSE_LABEL: SENT_NO_RESPONSE_QUERY,
    SENT_TABLE_NAME_PENDING: SENT_TABLE_PENDING_QUERY,
    SENT_TABLE_NAME_NO_RESPONSE: SENT_TABLE_NO_RESPONSE_QUERY,
    SENT_REGISTRATION_FUNNEL_NAME: SENT_REGISTRATION_FUNNEL_QUERY,
    QUEUE_SIZE_NAME: QUEUE_SIZE_QUERY,
    QUEUE_OVER_24H_NAME: QUEUE_OVER_24H_QUERY,
    QUEUE_RESCUE_NAME: QUEUE_RESCUE_QUERY,
    QUEUE_MAX_AGE_NAME: QUEUE_MAX_AGE_QUERY,
    QUEUE_NOW_NAME: QUEUE_NOW_QUERY,
    QUEUE_SURVIVAL_NAME: QUEUE_SURVIVAL_QUERY,
    QUEUE_PIVOT_CLINIC_NAME: QUEUE_PIVOT_CLINIC_QUERY,
    QUEUE_PIVOT_SEMD_NAME: QUEUE_PIVOT_SEMD_QUERY,
    QUEUE_FLOW_NAME: QUEUE_FLOW_QUERY,
    QUEUE_TAIL_NAME: QUEUE_TAIL_QUERY,
}

# Единое правило набора документов очереди — повторяется в описании каждой карточки
# блока: расхождение между ними и было причиной несходящихся итогов.
_QUEUE_SCOPE = (
    "Очередь — документы, отправленные до текущего момента, по которым ещё не пришёл "
    "первый ответ, и возраст которых не вышел за последнюю ступень справочника; "
    "документы последней ступени разбираются отдельной парой карточек внизу вкладки. "
    "Фильтр периода карточку не двигает: очередь — состояние на текущий момент."
)

SENT_TAB_DESCRIPTIONS: dict[str, str] = {
    SENT_STATE_NO_RESPONSE_LABEL: "Отправленные, по которым ожидаемое время истекло: ответа не будет. Из остальных карточек такие документы исключены и подлежат очистке.",
    SENT_TABLE_NAME_PENDING: "Отправленные, по которым ответ ещё ожидается; самые давние сверху. Ступень обработки, число подач.",
    SENT_TABLE_NAME_NO_RESPONSE: "Отправленные, по которым ожидаемое время истекло и ответа уже не будет; самые давние сверху.",
    SENT_REGISTRATION_FUNNEL_NAME: (
        "Документы, по которым ответ уже получен: сколько из них уложилось в срок. "
        "Сроки ужесточаются слева направо, поэтому воронка сужается. Разрыв между базой "
        "и первой ступенью — регистрации дольше самой мягкой границы справочника. "
        "Ожидающие и «Ответ не получен» в набор документов не входят: срока регистрации "
        "у них нет."
    ),
    QUEUE_SIZE_NAME: (
        "Строка — день; значение — сколько документов стояло в очереди на его конец, "
        f"последняя точка — на текущий момент. {_QUEUE_SCOPE} "
        "Сравнение — с предыдущим днём ряда."
    ),
    QUEUE_OVER_24H_NAME: (
        "Строка — день; значение — часть очереди на его конец с возрастом больше границы "
        "ступени «до 24 часов» из справочника. Последняя точка — на текущий момент. "
        f"{_QUEUE_SCOPE} Штатный ответ РЭМД приходит за минуты-часы, поэтому рост ряда — "
        "аномалия, а не сезонность."
    ),
    QUEUE_RESCUE_NAME: (
        "Документы очереди на последней рабочей ступени справочника: после 15 суток "
        f"ответ уже не придёт. Цель — ноль. {_QUEUE_SCOPE}"
    ),
    QUEUE_MAX_AGE_NAME: (
        "Возраст самого давнего документа очереди на текущий момент, в сутках. Возраст "
        f"считается от первой отправки. {_QUEUE_SCOPE}"
    ),
    QUEUE_NOW_NAME: (
        "Строка — ступень возраста; значение — сколько документов очереди попало ровно "
        "в неё на текущий момент. Ступени взаимоисключающие, сумма столбцов равна размеру "
        f"очереди. {_QUEUE_SCOPE}"
    ),
    QUEUE_SURVIVAL_NAME: (
        "Строка — срок ожидания; значение — доля очереди, которая на текущий момент ждёт "
        "дольше этого срока. Ряд кумулятивный — в отличие от взаимоисключающих ступеней "
        "гистограммы «Текущая очередь по ступеням» на вкладке «Оперативный мониторинг». "
        "Второй ряд — та же кривая неделю назад: сравнивается форма ожидания, а не размер "
        f"очереди. {_QUEUE_SCOPE}"
    ),
    QUEUE_PIVOT_CLINIC_NAME: (
        "Строка — клиника, колонка — ступень возраста, значение — документы очереди "
        "на текущий момент; ступени взаимоисключающие. Концентрация хвоста в одной "
        "клинике указывает на транспорт на её стороне; ровный хвост по многим клиникам "
        f"разбирается на соседней сводной по типам СЭМД. {_QUEUE_SCOPE}"
    ),
    QUEUE_PIVOT_SEMD_NAME: (
        "Строка — код СЭМД, колонка — ступень возраста, значение — документы очереди "
        "на текущий момент; ступени взаимоисключающие. Хвост, размазанный по клиникам, "
        "но собранный в одном коде СЭМД, указывает на обработку этого типа документа "
        f"на стороне РЭМД. {_QUEUE_SCOPE}"
    ),
    QUEUE_FLOW_NAME: (
        f"Окно — {QUEUE_FLOW_DAYS} суток до текущего момента. Строка — событие очереди: "
        "что стояло на начало окна, что отправлено внутри него, что ушло полученным "
        "ответом и что ушло по возрасту. Сумма шагов — очередь на конец окна. Оба оттока "
        "окрашены одинаково: у водопада один цвет убыли, шаги различаются подписью. "
        f"{_QUEUE_SCOPE}"
    ),
    QUEUE_TAIL_NAME: (
        "Строка — неделя (МСК); значения — доли очереди на конец недели, которые ждали "
        "дольше 24 часов, 3 и 7 суток. Границы берутся из справочника ступеней. "
        f"Последняя точка — на текущий момент. {_QUEUE_SCOPE}"
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


def apply_status_by_day(card: dict) -> None:
    card["display"] = "bar"
    card["description"] = (
        "Документы по дням и состоянию: три исхода РЭМД плюс «В обработке» (stacked). "
        "«Без ответа» — на вкладке «Отправленные». Клик по сегменту — архив с фильтром по статусу."
    )
    dq = card.setdefault("dataset_query", {})
    dq["native"]["query"] = STATUS_BY_DAY_QUERY
    card["metabase-field-filters"] = {
        "ips_date": {"table_ref": "public.rpt_documents", "field_name": "ips_date"},
        "semd_type": {"table_ref": "public.rpt_documents", "field_name": "semd_label"},
        "jid": {"table_ref": "public.rpt_documents", "field_name": "clinic_label"},
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
    """Карточки очереди и разбора отправленных: единый якорь, единый блок фильтров.

    Пороги нигде не объявляются: и ступень, и её граница приходят из dim_pending_segments
    (README §«Учёт отправленных»), карточка отбирает по состоянию, а не по числу дней.
    """
    dash["cards"] = [
        card for card in dash.get("cards", []) if card.get("name") not in RETIRED_CARD_NAMES
    ]
    # Скорость регистрации описывает судьбу уже отправленного и разбирается вместе с
    # остальными срезами отправки; распределение очереди дополнительно стоит на
    # мониторинге — там вопрос «что стоит прямо сейчас».
    for card in dash.get("cards", []):
        if card.get("name") == SENT_REGISTRATION_FUNNEL_NAME:
            card["tab"] = "sent"

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

    # План — источник правды по размещению: карточка, переехавшая на другую вкладку,
    # не должна оставаться показом на прежней.
    planned_tabs: dict[str, set[str]] = {}
    for planned_name, _display, planned_tab in SENT_TAB_CARD_PLAN:
        planned_tabs.setdefault(planned_name, set()).add(planned_tab)
    dash["cards"] = [
        card
        for card in dash.get("cards", [])
        if card.get("name") not in planned_tabs
        or card.get("tab") in planned_tabs[card.get("name")]
    ]

    existing = {(card.get("name"), card.get("tab")) for card in dash.get("cards", [])}
    for new_name, new_display, new_tab in SENT_TAB_CARD_PLAN:
        if (new_name, new_tab) not in existing:
            dash.setdefault("cards", []).append(
                {"name": new_name, "display": new_display, "tab": new_tab}
            )

    # Отбор по имени, а не по вкладке: карточки этого блока живут на двух вкладках, и
    # привязка к вкладке оставляла бы переехавшую карточку без обновления запроса.
    for card in dash.get("cards", []):
        if card.get("display") == "text":
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
        if name in SENT_QUEUE_CARDS:
            # Набор документов очереди — rpt_documents на текущем моменте; период
            # к состоянию на момент неприменим, поэтому даты в карточке нет вовсе.
            # Перечень тегов равен перечню привязок: тег, которого нет в SQL карточки,
            # не объявляется — иначе фильтр «двигает» одни карточки и молчит на других.
            bindings = {
                key: deepcopy(binding)
                for key, binding in SENT_QUEUE_FIELD_FILTERS.items()
                if key != "pending_segment" or name not in SENT_QUEUE_AXIS_CARDS
            }
            card["metabase-field-filters"] = bindings
            card["dataset_query"]["native"]["template-tags"] = {
                key: deepcopy(SENT_FILTER_TEMPLATE_TAGS[key]) for key in bindings
            }
        elif name == SENT_REGISTRATION_FUNNEL_NAME:
            # Воронка процесса читает полный корпус регистрации: свои привязки фильтров
            # и свой набор тегов.
            card["metabase-field-filters"] = deepcopy(SENT_REGISTRATION_FIELD_FILTERS)
            tags = card["dataset_query"]["native"]["template-tags"]
            for tag in list(tags):
                if tag not in SENT_REGISTRATION_FIELD_FILTERS:
                    tags.pop(tag)
        apply_sent_card_visualization(card, name, viz)


def apply_sent_card_visualization(card: dict, name: str, viz: dict) -> None:
    """Подача карточки задаётся здесь, а не приходит из выгрузки: тип, оси, цвета."""
    for key in [
        k
        for k in viz
        if k.startswith("graph.")
        or k.startswith("funnel.")
        or k.startswith("pivot")
        or k.startswith("waterfall.")
        or k.startswith("progress.")
        or k in ("stackable.stack_type", "series_settings", "scalar.field")
    ]:
        del viz[key]

    if name == SENT_REGISTRATION_FUNNEL_NAME:
        card["display"] = "funnel"
        viz["funnel.dimension"] = "Этап"
        viz["funnel.metric"] = "Документов"
    elif name in SENT_QUEUE_TREND_CARDS:
        card["display"] = "smartscalar"
        viz["scalar.field"] = name
        viz["column_settings"] = {
            _column_key(name): {"decimals": 0, "number_separators": ", "}
        }
    elif name == QUEUE_RESCUE_NAME:
        # Цель карточки — ноль, а полоса прогресса считает долю от цели: при нулевой цели
        # Metabase делит на ноль. Числу «сколько документов вот-вот перестанут ждать»
        # шкала и не нужна — оно читается как счётчик.
        card["display"] = "scalar"
        viz["column_settings"] = {
            _column_key("Документов"): {"decimals": 0, "number_separators": ", "}
        }
    elif name == QUEUE_MAX_AGE_NAME:
        card["display"] = "scalar"
        viz["column_settings"] = {
            _column_key("Суток в очереди"): {"decimals": 1, "number_separators": ", "}
        }
    elif name == QUEUE_NOW_NAME:
        card["display"] = "bar"
        viz["graph.dimensions"] = ["Ступень обработки"]
        viz["graph.metrics"] = ["Документов"]
        viz["graph.x_axis.scale"] = "ordinal"
        viz["graph.x_axis.title_text"] = "Ступень обработки"
        viz["graph.y_axis.title_text"] = "Документов"
        viz["graph.show_values"] = True
        viz["series_settings"] = {
            label: {"color": color} for label, color in SENT_SEGMENT_COLORS.items()
        }
        viz["column_settings"] = {
            _column_key("Документов"): {"decimals": 0, "number_separators": ", "}
        }
    elif name == QUEUE_SURVIVAL_NAME:
        card["display"] = "line"
        viz["graph.dimensions"] = ["Срок ожидания", "Срез"]
        viz["graph.metrics"] = ["Ещё ждут, %"]
        viz["graph.x_axis.scale"] = "ordinal"
        viz["graph.x_axis.title_text"] = "Срок ожидания"
        viz["graph.y_axis.title_text"] = "Ещё ждут, %"
        viz["series_settings"] = {
            QUEUE_SURVIVAL_SLICE_NOW: {"color": SENT_STATE_COLORS["pending"]},
            QUEUE_SURVIVAL_SLICE_WEEK_AGO: {"color": SENT_STATE_COLORS["no_response"]},
        }
        viz["column_settings"] = {
            _column_key("Ещё ждут, %"): {
                "decimals": 1,
                "number_separators": ", ",
                "suffix": " %",
            }
        }
    elif name in (QUEUE_PIVOT_CLINIC_NAME, QUEUE_PIVOT_SEMD_NAME):
        row_field = "Клиника" if name == QUEUE_PIVOT_CLINIC_NAME else "Код СЭМД"
        card["display"] = "table"
        segments = list(SENT_SEGMENT_COLORS)
        viz["table.columns"] = [
            {"enabled": True, "name": column}
            for column in (row_field, *segments, QUEUE_TOTAL_COLUMN)
        ]
        viz["table.cell_column"] = QUEUE_TOTAL_COLUMN
        # Отдельного типа «тепловая карта» в Metabase нет: градиент по значению задаётся
        # условным форматированием, общий для всех ступеней — иначе колонки несравнимы.
        viz["table.column_formatting"] = [
            {
                "type": "range",
                "columns": segments,
                "colors": ["#FFFFFF", SENT_SEGMENT_COLORS["до 15 суток"]],
                "min_type": "all",
                "max_type": "all",
            }
        ]
        viz["column_settings"] = {
            _column_key(column): {"decimals": 0, "number_separators": ", "}
            for column in (*segments, QUEUE_TOTAL_COLUMN)
        }
    elif name == QUEUE_FLOW_NAME:
        card["display"] = "waterfall"
        viz["graph.dimensions"] = ["Этап"]
        viz["graph.metrics"] = ["Документов"]
        viz["graph.x_axis.scale"] = "ordinal"
        viz["graph.show_values"] = True
        viz["waterfall.show_total"] = True
        viz["waterfall.increase_color"] = SENT_STATE_COLORS["pending"]
        # Цвет убыли у водопада один на все шаги вниз, а уходят из очереди по двум разным
        # поводам — ответ и возраст. Зелёный «успех» на обоих подписывал утилизацию
        # достижением, поэтому убыль нейтральна: смысл шага несёт его подпись.
        viz["waterfall.decrease_color"] = SENT_STATE_COLORS["no_response"]
        viz["waterfall.total_color"] = SEGMENT_COLOR_BY_CODE["p_1h"]
        viz["column_settings"] = {
            _column_key("Документов"): {"decimals": 0, "number_separators": ", "}
        }
    elif name == QUEUE_TAIL_NAME:
        card["display"] = "line"
        viz["graph.dimensions"] = ["Неделя"]
        viz["graph.metrics"] = list(QUEUE_TAIL_METRICS)
        viz["graph.x_axis.scale"] = "timeseries"
        viz["graph.y_axis.title_text"] = "Доля очереди, %"
        viz["series_settings"] = {
            metric: {"color": color}
            for metric, color in zip(QUEUE_TAIL_METRICS, QUEUE_TAIL_COLORS)
        }
        viz["column_settings"] = {
            _column_key(metric): {
                "decimals": 1,
                "number_separators": ", ",
                "suffix": " %",
            }
            for metric in QUEUE_TAIL_METRICS
        }
    elif name in (SENT_TABLE_NAME_PENDING, SENT_TABLE_NAME_NO_RESPONSE):
        viz["table.columns"] = deepcopy(SENT_TABLE_COLUMNS)
        viz["table.cell_column"] = "Состояние отправки"
        viz["column_settings"] = deepcopy(SENT_TABLE_COLUMN_SETTINGS)
        # Подсветка строк держалась на снятой колонке «Сегмент ожидания» и порогах
        # прежней лестницы; ступень пришла из справочника и подписывает себя сама.
        viz.pop("table.column_formatting", None)


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
    # Ширины и закрепление первых колонок сведены с живого прода: строка длинная, и без
    # закрепления даты, статуса и клиники горизонтальная прокрутка теряет контекст.
    viz["table.column_widths"] = [None, 198, None, 325, 551]
    viz["table.freeze_columns"] = True
    viz["table.freeze_columns_count"] = 3
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
    # Второй даты на вкладке нет: очередь считается на текущий момент. Параметр снимается
    # с уже опубликованных дашбордов вместе со своими привязками в карточках.
    dash["parameters"] = [p for p in params if p.get("slug") not in RETIRED_PARAM_IDS]
    retired_ids = set(RETIRED_PARAM_IDS.values())
    for card in dash.get("cards", []):
        mappings = card.get("parameter_mappings")
        if mappings:
            card["parameter_mappings"] = [
                m for m in mappings if m.get("parameter_id") not in retired_ids
            ]


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
        elif name == "Статусы регистрации СЭМД" and dq.get("type") == "native":
            apply_status_by_day(card)
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


def apply_executive_no_response(card: dict) -> None:
    """Плитка утилизированных: состояние из справочника вместо порога «>7 дн.» в тексте."""
    card["description"] = (
        "Доля документов, по которым ответ ЕГИСЗ уже не ожидается (состояние отправки "
        "«Ответ не получен», dim_pending_segments), от всех документов периода."
    )
    card["dataset_query"]["native"]["query"] = EXECUTIVE_NO_RESPONSE_QUERY
    viz = card.setdefault("visualization_settings", {})
    viz.pop("scalar.field", None)
    viz["column_settings"] = {
        f'["name","{EXECUTIVE_NO_RESPONSE_NAME}"]': {
            "column_title": EXECUTIVE_NO_RESPONSE_NAME,
            "decimals": 1,
            "number_separators": ", ",
            "suffix": " %",
        }
    }


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
        if card.get("name") == EXECUTIVE_NO_RESPONSE_NAME:
            apply_executive_no_response(card)
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
        elif "public.rpt_documents_sent" in native["query"]:
            source = "public.rpt_documents_sent"
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
