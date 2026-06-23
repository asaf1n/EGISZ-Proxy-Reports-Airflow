#!/usr/bin/env python3
"""Shared native definition for «Объём по клиникам» with % of total."""
from __future__ import annotations

from typing import Any

CLINIC_VOLUME_SQL = (
    "WITH filtered AS ( "
    'SELECT "JID клиники", "Наименование клиники", "Документ (ключ учёта)" '
    "FROM public.v_rpt_documents_ui WHERE 1=1 "
    "[[AND {{dwh_date}}]] [[AND {{semd_type}}]] [[AND {{jid}}]] "
    "[[AND {{local_uid}}]] [[AND {{relates_to}}]] [[AND {{emdr_id}}]] "
    "[[AND {{status}}]] [[AND {{log_id}}]] "
    "), totals AS ( "
    'SELECT COUNT(DISTINCT "Документ (ключ учёта)")::numeric AS total FROM filtered '
    "), per_clinic AS ( "
    'SELECT "JID клиники"::text AS "JID клиники", "Наименование клиники", '
    'COUNT(DISTINCT "Документ (ключ учёта)")::bigint AS cnt '
    "FROM filtered GROUP BY 1, 2 "
    ') SELECT "JID клиники", "Наименование клиники", cnt AS "Документов", '
    'ROUND(100.0 * cnt / NULLIF((SELECT total FROM totals), 0), 1) AS "%" '
    "FROM per_clinic ORDER BY cnt DESC"
)

CLINIC_VOLUME_TAGS: dict[str, Any] = {
    "jid": {
        "display-name": "JID клиники",
        "id": "f2000002-0002-4002-8002-000000000011",
        "name": "jid",
        "type": "dimension",
        "widget-type": "string/=",
    },
    "dwh_date": {
        "display-name": "По дате «Обработано»",
        "id": "f2000002-0002-4002-8002-000000000001",
        "name": "dwh_date",
        "type": "dimension",
        "widget-type": "date/all-options",
    },
    "emdr_id": {
        "display-name": "Рег. номер РЭМД (emdrid)",
        "id": "f2000002-0002-4002-8002-000000000014",
        "name": "emdr_id",
        "type": "dimension",
        "widget-type": "string/=",
    },
    "local_uid": {
        "display-name": "localUid СЭМД",
        "id": "f2000002-0002-4002-8002-000000000012",
        "name": "local_uid",
        "type": "dimension",
        "widget-type": "string/=",
    },
    "log_id": {
        "display-name": "LOGID журнала EXCHANGELOG",
        "id": "f2000002-0002-4002-8002-000000000015",
        "name": "log_id",
        "type": "dimension",
        "widget-type": "string/=",
    },
    "relates_to": {
        "display-name": "Связанное сообщение (relatesToMessage)",
        "id": "f2000002-0002-4002-8002-000000000013",
        "name": "relates_to",
        "type": "dimension",
        "widget-type": "string/=",
    },
    "semd_type": {
        "display-name": "Код СЭМД",
        "id": "f2000002-0002-4002-8002-000000000010",
        "name": "semd_type",
        "type": "dimension",
        "widget-type": "string/=",
    },
    "status": {
        "display-name": "Статус",
        "id": "f2000002-0002-4002-8002-000000000016",
        "name": "status",
        "type": "dimension",
        "widget-type": "string/=",
    },
}

CLINIC_VOLUME_FIELD_FILTERS: dict[str, dict[str, str]] = {
    "dwh_date": {
        "table_ref": "public.v_rpt_documents_ui",
        "field_name": "Дата обработки",
    },
    "semd_type": {
        "table_ref": "public.v_rpt_documents_ui",
        "field_name": "Код СЭМД",
    },
    "jid": {
        "table_ref": "public.v_rpt_documents_ui",
        "field_name": "JID клиники",
    },
    "local_uid": {
        "table_ref": "public.v_rpt_documents_ui",
        "field_name": "localUid СЭМД",
    },
    "relates_to": {
        "table_ref": "public.v_rpt_documents_ui",
        "field_name": "Связанное сообщение",
    },
    "emdr_id": {
        "table_ref": "public.v_rpt_documents_ui",
        "field_name": "Рег. номер РЭМД",
    },
    "status": {
        "table_ref": "public.v_rpt_documents_ui",
        "field_name": "Статус",
    },
    "log_id": {
        "table_ref": "public.v_rpt_documents_ui",
        "field_name": "LOGID журнала EXCHANGELOG",
    },
}

CLINIC_VOLUME_VIZ: dict[str, Any] = {
    "table.column_widths": [186],
    "graph.show_values": True,
    "table.columns": [
        {"enabled": False, "name": "JID клиники"},
        {"enabled": True, "name": "Наименование клиники"},
        {"enabled": True, "name": "Документов"},
        {"enabled": True, "name": "%"},
    ],
    "table.freeze_rows": False,
    "table.freeze_columns": False,
    "table.cell_column": "Наименование клиники",
    "table.freeze_columns_count": 0,
    "graph.metrics": ["Документов"],
    "table.column_formatting": [
        {
            "colors": ["transparent", "#509EE3"],
            "columns": ["Документов"],
            "max_type": None,
            "max_value": 100,
            "min_type": None,
            "min_value": 0,
            "type": "range",
        }
    ],
    "table.row_index": True,
    "table.pivot_column": "Документов",
    "column_settings": {
        '["name","%"]': {
            "column_title": "%",
            "decimals": 1,
            "number_separators": ", ",
            "suffix": " %",
        },
        '["name","JID клиники"]': {
            "column_title": "JID клиники",
            "decimals": 0,
            "number_separators": ", ",
        },
        '["name","Документов"]': {
            "column_title": "Количество",
            "decimals": 0,
            "number_separators": ", ",
        },
        '["name","Наименование клиники"]': {
            "column_title": "Клиника",
        },
    },
    "graph.dimensions": ["Наименование клиники"],
}

ARCHIVE_CLICK = {
    "type": "link",
    "linkType": "dashboard",
    "targetDashboard": "Интеграция с ЕГИСЗ",
    "parameterMapping": {},
    "tab": "archive",
}


def clinic_volume_card(
    *,
    row: int,
    col: int,
    size_x: int,
    size_y: int,
    tab: str | None = None,
    with_click: bool = True,
) -> dict[str, Any]:
    card: dict[str, Any] = {
        "name": "Объём по клиникам",
        "description": (
            "Топ клиник по числу документов (DISTINCT «Документ (ключ учёта)»), все статусы. "
            "Колонка «%» — доля от общего числа документов в срезе."
        ),
        "dataset_query": {
            "type": "native",
            "native": {
                "query": CLINIC_VOLUME_SQL,
                "template-tags": CLINIC_VOLUME_TAGS,
            },
            "database": 1,
        },
        "display": "table",
        "visualization_settings": CLINIC_VOLUME_VIZ,
        "sizeX": size_x,
        "sizeY": size_y,
        "row": row,
        "col": col,
        "metabase-field-filters": CLINIC_VOLUME_FIELD_FILTERS,
    }
    if tab:
        card["tab"] = tab
    if with_click:
        card["click_behavior"] = ARCHIVE_CLICK
    return card
