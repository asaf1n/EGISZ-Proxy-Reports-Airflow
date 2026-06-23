#!/usr/bin/env python3
"""Apply Query Builder card definitions to dashboard JSON files."""
from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS = ROOT / "metabase_dashboards"

ARCHIVE_CLICK = {
    "type": "link",
    "linkType": "dashboard",
    "targetDashboard": "Интеграция с ЕГИСЗ",
    "parameterMapping": {},
    "tab": "archive",
}

# Cards that must stay native SQL to preserve pivoted metrics and stacked visuals.
NATIVE_ONLY = {
    "Успешность по клиникам",
    "Успешность по типам СЭМД",
    "Виды ошибок по категориям",
    "Виды ошибок по типам СЭМД",
    "Объём по клиникам",
}


def qb_query(
    model: str,
    *,
    aggregation: list[Any] | None = None,
    breakout: list[Any] | None = None,
    filter_expr: list[Any] | None = None,
    order_by: list[Any] | None = None,
    limit: int | None = None,
    source_table: str | None = None,
) -> dict[str, Any]:
    query: dict[str, Any] = {
        "source-table": source_table or f"model:{model}",
    }
    if aggregation is not None:
        query["aggregation"] = aggregation
    if breakout is not None:
        query["breakout"] = breakout
    if filter_expr is not None:
        query["filter"] = filter_expr
    if order_by is not None:
        query["order-by"] = order_by
    if limit is not None:
        query["limit"] = limit
    return query


def field(model: str, name: str) -> list[Any]:
    return ["field", f"{model}:{name}", None]


def qb_card(
    *,
    model: str,
    query: dict[str, Any],
    display: str,
    param_targets: dict[str, dict[str, str]] | None = None,
    click: dict[str, Any] | None = None,
    scalar_field: str | None = None,
) -> dict[str, Any]:
    card: dict[str, Any] = {
        "query_tier": "query_builder",
        "source_model": model,
        "dataset_query": {
            "type": "query",
            "query": query,
            "database": 1,
        },
    }
    if param_targets:
        card["metabase-parameter-targets"] = param_targets
    if click:
        card["click_behavior"] = click
    if display == "scalar" and scalar_field:
        card.setdefault("visualization_settings", {})["scalar.field"] = scalar_field
    return card


DOC = "Документы"
BRK = "Разбивка ошибок"
DOC_KEY = field(DOC, "Документ (ключ учёта)")
ERROR_FILTER = [
    "or",
    ["=", field(DOC, "Статус"), "Ошибка связи"],
    ["=", field(DOC, "Статус"), "Ошибка асинхронного ответа РЭМД"],
]
DOC_PARAMS = {
    "dwh_date": {"model_ref": DOC, "field_name": "Дата обработки"},
    "jid": {"model_ref": DOC, "field_name": "JID клиники"},
    "semd_type": {"model_ref": DOC, "field_name": "Код СЭМД"},
    "status": {"model_ref": DOC, "field_name": "Статус"},
}
BRK_PARAMS = {
    "dwh_date": {"model_ref": BRK, "field_name": "Обработано IPS"},
    "jid": {"model_ref": BRK, "field_name": "JID клиники"},
    "semd_type": {"model_ref": BRK, "field_name": "Код СЭМД"},
}

QB_CARDS: dict[str, dict[str, Any]] = {
    "Документов с ошибкой": qb_card(
        model=DOC,
        display="scalar",
        scalar_field="count",
        query=qb_query(
            DOC,
            aggregation=[["distinct", DOC_KEY]],
            filter_expr=ERROR_FILTER,
        ),
        param_targets={k: v for k, v in DOC_PARAMS.items() if k != "status"},
        click=ARCHIVE_CLICK,
    ),
    "Доля ошибок, %": qb_card(
        model=DOC,
        display="scalar",
        scalar_field="expression",
        query=qb_query(
            DOC,
            aggregation=[
                [
                    "/",
                    ["distinct", DOC_KEY],
                    [
                        "distinct",
                        DOC_KEY,
                        {
                            "filter": [
                                "or",
                                ["=", field(DOC, "Статус"), "Успешно зарегистрирован"],
                                *ERROR_FILTER[1:],
                            ]
                        },
                    ],
                ]
            ],
            filter_expr=ERROR_FILTER,
        ),
        param_targets={k: v for k, v in DOC_PARAMS.items() if k != "status"},
    ),
    "Клиник с ошибками": qb_card(
        model=DOC,
        display="scalar",
        scalar_field="count",
        query=qb_query(
            DOC,
            aggregation=[["distinct", field(DOC, "JID клиники")]],
            filter_expr=ERROR_FILTER,
        ),
        param_targets={k: v for k, v in DOC_PARAMS.items() if k != "status"},
        click=ARCHIVE_CLICK,
    ),
    "Ошибок регистрации в РЭМД": qb_card(
        model=DOC,
        display="scalar",
        scalar_field="count",
        query=qb_query(
            DOC,
            aggregation=[["distinct", DOC_KEY]],
            filter_expr=["=", field(DOC, "Статус"), "Ошибка асинхронного ответа РЭМД"],
        ),
        param_targets={k: v for k, v in DOC_PARAMS.items() if k != "status"},
        click=ARCHIVE_CLICK,
    ),
    "Виды ошибок по категориям": qb_card(
        model=BRK,
        display="row",
        query=qb_query(
            BRK,
            aggregation=[["distinct", field(BRK, "Документ (ключ учёта)")]],
            breakout=[field(BRK, "Категория ошибки")],
            order_by=[["desc", ["aggregation", 0, None]]],
            limit=15,
        ),
        param_targets=BRK_PARAMS,
        click=ARCHIVE_CLICK,
    ),
    "Виды ошибок по типам СЭМД": qb_card(
        model=BRK,
        display="bar",
        query=qb_query(
            BRK,
            aggregation=[["distinct", field(BRK, "Документ (ключ учёта)")]],
            breakout=[field(BRK, "Код СЭМД"), field(BRK, "Тип ошибки")],
            order_by=[["desc", ["aggregation", 0, None]]],
            limit=15,
        ),
        param_targets=BRK_PARAMS,
        click=ARCHIVE_CLICK,
    ),
    "Успешность по клиникам": qb_card(
        model=DOC,
        display="table",
        query=qb_query(
            DOC,
            aggregation=[["distinct", DOC_KEY]],
            breakout=[
                field(DOC, "JID клиники"),
                field(DOC, "Наименование клиники"),
                field(DOC, "Статус"),
            ],
        ),
        param_targets=DOC_PARAMS,
        click=ARCHIVE_CLICK,
    ),
    "Успешность по типам СЭМД": qb_card(
        model=DOC,
        display="table",
        query=qb_query(
            DOC,
            aggregation=[["distinct", DOC_KEY]],
            breakout=[field(DOC, "Код СЭМД"), field(DOC, "Тип СЭМД (код · НСИ)"), field(DOC, "Статус")],
        ),
        param_targets=DOC_PARAMS,
        click=ARCHIVE_CLICK,
    ),
    "Статусы за период": qb_card(
        model=DOC,
        display="pie",
        query=qb_query(
            DOC,
            aggregation=[["distinct", DOC_KEY]],
            breakout=[field(DOC, "Статус")],
        ),
        param_targets=DOC_PARAMS,
    ),
    "Последние операции": qb_card(
        model=DOC,
        display="table",
        query=qb_query(
            DOC,
            order_by=[["desc", field(DOC, "Дата обработки")]],
            limit=50,
        ),
        param_targets=DOC_PARAMS,
        click=ARCHIVE_CLICK,
    ),
}


def merge_card(existing: dict[str, Any], qb: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(existing)
    for key in (
        "query_tier",
        "source_model",
        "dataset_query",
        "metabase-parameter-targets",
        "click_behavior",
    ):
        if key in qb:
            merged[key] = copy.deepcopy(qb[key])
    merged.pop("metabase-field-filters", None)
    if qb.get("visualization_settings"):
        merged.setdefault("visualization_settings", {}).update(qb["visualization_settings"])
    return merged


def apply_to_dashboard(path: Path) -> int:
    dashboard = json.loads(path.read_text(encoding="utf-8"))
    changed = 0
    for card in dashboard.get("cards", []):
        name = card.get("name")
        if not name or name not in QB_CARDS or name in NATIVE_ONLY:
            continue
        idx = dashboard["cards"].index(card)
        dashboard["cards"][idx] = merge_card(card, QB_CARDS[name])
        changed += 1
    if changed:
        path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return changed


def main() -> int:
    total = 0
    for path in sorted(DASHBOARDS.glob("*.json")):
        n = apply_to_dashboard(path)
        if n:
            print(f"{path.name}: converted {n} card(s)")
            total += n
    print(f"total converted: {total}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
