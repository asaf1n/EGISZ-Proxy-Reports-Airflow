#!/usr/bin/env python3
"""Apply Metabase dashboard card audit: deletions, renames, rounding, viz."""
from __future__ import annotations

import json
import re
from copy import deepcopy
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS_DIR = ROOT / "metabase_dashboards"

# Global card renames (old -> new)
RENAMES: dict[str, str] = {
    "async vs network": "Отказы: асинхронный ответ vs связь",
    "Healthcheck": "Статус healthcheck",
    "Сигналы healthcheck": "Детализация healthcheck",
    "Топ сетевых формулировок": "Типы сетевых ошибок (за период)",
    "Топ формулировок сетевых ошибок": "Типы сетевых ошибок (все дни)",
    "Топ клиник по ошибкам (период)": "Топ клиник по доле отказов",
    "Топ клиник по ошибкам связи": "Топ клиник по сбоям транспорта",
    "Последние ошибки связи": "Последние сбои транспорта",
    "First-pass acceptance, %": "Доля успеха с первой попытки, %",
    "First-pass acceptance (по клинике), %": "Доля успеха с первой попытки (клиент), %",
    "Heatmap клиники × дни (доля ошибок)": "Тепловая карта: клиника × день",
    "Доступность по клиникам (день × JID)": "Доступность транспорта: день × JID",
    "MRR (trailing-30d), ₽": "MRR (30 дн.), ₽",
    "ARR (MRR × 12), ₽": "ARR (год.), ₽",
    "Динамика MRR (trailing-30d), ₽": "Динамика MRR (30 дн.), ₽",
    "Динамика активных JID (trailing-30d)": "Динамика активных JID (30 дн.)",
}

DELETE_CARDS: dict[str, set[str]] = {
    "04_quality_and_errors.json": {"% успеха"},
    "02_service.json": {"Объём по клиникам (поток)"},
}

STATUS_COLORS = {
    "Успешно зарегистрирован": "#88BF4D",
    "Ошибка асинхронного ответа РЭМД": "#A989C5",
    "Ошибка связи": "#F2994A",
    "В обработке": "#509EE3",
}

# SQL alias fixes when card is renamed
SQL_ALIAS_FIXES: dict[str, tuple[str, str]] = {
    "Доля ошибок за период, %": ('AS "Доля ошибок, %"', 'AS "Доля ошибок за период, %"'),
    "Доля успеха с первой попытки, %": (
        'AS "First-pass acceptance, %"',
        'AS "Доля успеха с первой попытки, %"',
    ),
    "Доля успеха с первой попытки (клиент), %": (
        'AS "First-pass acceptance, %"',
        'AS "Доля успеха с первой попытки (клиент), %"',
    ),
    "Отказы: асинхронный ответ vs связь": (
        'AS "Уникальных документов"',
        'AS "Документов"',
    ),
    "Тепловая карта: клиника × день": (
        'AS "Доля ошибок, %" FROM d',
        'AS "Доля ошибок, %" FROM d',  # ROUND fix only
    ),
}


def fix_round_in_query(query: str) -> str:
    """ROUND(..., 2) -> ROUND(..., 1) for percentages/averages."""
    q = query.replace(", 2)::numeric AS", ", 1)::numeric AS")
    q = q.replace(", 2) AS", ", 1) AS")
    return q


def rename_in_obj(obj: object, old: str, new: str) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "name" and v == old:
                obj[k] = new
            elif k == "text" and isinstance(v, str):
                obj[k] = v.replace(old, new)
            elif k == "description" and isinstance(v, str):
                obj[k] = v.replace(old, new)
            elif isinstance(v, str) and v == old:
                obj[k] = new
            else:
                rename_in_obj(v, old, new)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, str) and item == old:
                obj[i] = new
            else:
                rename_in_obj(item, old, new)


def ensure_column_setting(viz: dict, col_name: str, settings: dict) -> None:
    viz.setdefault("column_settings", {})
    key = f'["name","{col_name}"]'
    existing = viz["column_settings"].get(key, {})
    merged = {**existing, **settings}
    viz["column_settings"][key] = merged


def apply_viz_defaults(card: dict) -> None:
    if card.get("display") == "text":
        return
    name = card.get("name", "")
    display = card.get("display", "")
    viz = card.setdefault("visualization_settings", {})
    query = ""
    dq = card.get("dataset_query", {})
    if dq.get("type") == "native":
        query = dq.get("native", {}).get("query", "")

    # Pie status colors (only full status breakdown pies, not error-only slices)
    if display == "pie" and "Статус" in query and "async_error', 'network_error')" not in query:
        rows = []
        for status, color in STATUS_COLORS.items():
            rows.append({"key": status, "name": status, "color": color})
        viz["pie.dimension"] = ["Статус"]
        viz["pie.rows"] = rows
    if display == "pie" and name == "Отказы: асинхронный ответ vs связь":
        viz["pie.dimension"] = ["Статус"]
        viz["pie.metric"] = "Документов"
        viz["pie.rows"] = [
            {"key": "Ошибка асинхронного ответа РЭМД", "name": "Ошибка асинхронного ответа РЭМД", "color": "#A989C5"},
            {"key": "Ошибка связи", "name": "Ошибка связи", "color": "#F2994A"},
        ]

    # Scalar decimals
    if display == "scalar":
        m = re.search(r'AS "([^"]+)"\s*FROM', query, re.I | re.S)
        if not m:
            m = re.search(r'AS "([^"]+)"\s*$', query.strip(), re.I)
        if m:
            col = m.group(1)
            is_pct = "%" in col or "доля" in col.lower() or "успех" in col.lower() or "отказ" in col.lower()
            is_money = "₽" in col or "MRR" in col or "ARR" in col or "Выручка" in col
            if is_pct:
                ensure_column_setting(viz, col, {"decimals": 1, "suffix": " %", "number_separators": "."})
            elif is_money:
                ensure_column_setting(viz, col, {"decimals": 0, "number_separators": " "})
            else:
                ensure_column_setting(viz, col, {"decimals": 0, "number_separators": "."})
            viz["scalar.field"] = col

    # Line/bar row graph metrics with % in name
    if display in ("line", "bar", "area", "row", "combo"):
        metrics = viz.get("graph.metrics", [])
        for metric in metrics:
            if "%" in metric or "доля" in metric.lower():
                ensure_column_setting(
                    viz, metric, {"decimals": 1, "suffix": " %", "number_separators": "."}
                )
            elif metric in ("Документов", "Количество", "Ошибок", "Час", "День", "Документов с ошибкой"):
                if metric not in ("Час", "День"):
                    ensure_column_setting(viz, metric, {"decimals": 0, "number_separators": "."})
            elif "мин" in metric.lower() or "доставк" in metric.lower():
                ensure_column_setting(viz, metric, {"decimals": 1, "number_separators": "."})

    # Numeric columns from query aliases (all chart types)
    for m in re.finditer(r'AS "([^"]+)"', query):
        col = m.group(1)
        key = f'["name","{col}"]'
        if key in viz.get("column_settings", {}) and "decimals" in viz["column_settings"][key]:
            continue
        if col in ("Создано", "Отправлено", "Дата обработки", "Дата", "День", "Месяц", "Час"):
            continue
        if "%" in col or "доля" in col.lower():
            ensure_column_setting(viz, col, {"decimals": 1, "suffix": " %", "number_separators": "."})
        elif "₽" in col or col.startswith("MRR") or col.startswith("ARR"):
            ensure_column_setting(viz, col, {"decimals": 0, "number_separators": " "})
        elif any(
            tok in col.lower()
            for tok in (
                "документ", "ошиб", "колич", "уникаль", "пациент", "врач",
                "попыт", "очеред", "активн", "mrr", "клиник",
            )
        ):
            ensure_column_setting(viz, col, {"decimals": 0, "number_separators": "."})
        elif "мин" in col.lower() or "/" in col:
            ensure_column_setting(viz, col, {"decimals": 1, "number_separators": "."})

    # Table numeric columns from query aliases (legacy path — kept for explicit table-only titles)
    if display == "table":
        pass

    # Pie charts with numeric metric
    if display == "pie":
        metric = viz.get("pie.metric") or (viz.get("graph.metrics") or [None])[0]
        if metric and metric != "Статус":
            if "%" in str(metric):
                ensure_column_setting(viz, metric, {"decimals": 1, "suffix": " %"})
            else:
                ensure_column_setting(viz, metric, {"decimals": 0, "number_separators": "."})
        viz.setdefault("pie.decimal_places", 1)

    # Axis titles for trends
    axis_titles: dict[str, dict[str, str]] = {
        "Сетевые ошибки (тренд)": {"graph.x_axis.title_text": "Час", "graph.y_axis.title_text": "Документов"},
        "Ошибки регистрации в РЭМД ЕГИСЗ (тренд)": {
            "graph.x_axis.title_text": "Час",
            "graph.y_axis.title_text": "Документов",
        },
        "Доля ошибок по дням": {
            "graph.x_axis.title_text": "День",
            "graph.y_axis.title_text": "Доля ошибок, %",
        },
        "Транзакции по дням и статусам": {
            "graph.x_axis.title_text": "Дата",
            "graph.y_axis.title_text": "Документов",
        },
        "Динамика статусов по дням": {
            "graph.x_axis.title_text": "День",
            "graph.y_axis.title_text": "Документов",
        },
        "Динамика MRR (30 дн.), ₽": {
            "graph.x_axis.title_text": "День",
            "graph.y_axis.title_text": "MRR, ₽",
        },
        "Динамика активных JID (30 дн.)": {
            "graph.x_axis.title_text": "День",
            "graph.y_axis.title_text": "Активных JID",
        },
        "Тренд ошибок связи по дням": {
            "graph.x_axis.title_text": "День",
            "graph.y_axis.title_text": "Ошибок",
        },
        "Объёмы доступности по дням": {
            "graph.x_axis.title_text": "День",
            "graph.y_axis.title_text": "Попыток",
        },
        "Доля доступности по дням": {
            "graph.x_axis.title_text": "День",
            "graph.y_axis.title_text": "Доля доступности, %",
        },
        "Типы сетевых ошибок (за период)": {
            "graph.show_values": True,
        },
        "Типы сетевых ошибок (все дни)": {
            "graph.show_values": True,
        },
        "Топ клиник по доле отказов": {
            "graph.x_axis.title_text": "Доля ошибок, %",
            "graph.y_axis.title_text": "Клиника",
            "graph.show_values": True,
        },
    }
    if name in axis_titles:
        for k, v in axis_titles[name].items():
            viz[k] = v

    # Short column titles
    short_titles = {
        "JID+Наименование": "Клиника",
        "Тип СЭМД (код · НСИ)": "Тип СЭМД",
        "Наименование клиники": "Клиника",
        "Процент успешных (по документам)": "% успеха",
        "Процент успешных": "% успеха",
    }
    for col, title in short_titles.items():
        key = f'["name","{col}"]'
        if key in viz.get("column_settings", {}) or col in query:
            ensure_column_setting(viz, col, {"column_title": title})

    # Heatmap formatting
    if name == "Тепловая карта: клиника × день":
        ensure_column_setting(viz, "Доля ошибок, %", {"decimals": 1, "suffix": " %"})
        viz["table.cell_column"] = "Доля ошибок, %"


def process_dashboard(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    delete_names = DELETE_CARDS.get(path.name, set())

    # Delete cards
    removed_rows: list[tuple[int, int]] = []
    new_cards = []
    for card in data.get("cards", []):
        if card.get("name") in delete_names:
            removed_rows.append((card.get("row", 0), card.get("sizeY", 4)))
            continue
        new_cards.append(card)
    data["cards"] = new_cards

    # Shift layout after 02 deletion (row 15, sizeY 6)
    if path.name == "02_service.json" and removed_rows:
        shift = sum(sy for _, sy in removed_rows)
        threshold = min(r for r, _ in removed_rows)
        for card in data["cards"]:
            if card.get("row", 0) > threshold:
                card["row"] = card["row"] - shift

    # Shift 04 KPI row: move Клиник с ошибками from col 18 to col 12
    if path.name == "04_quality_and_errors.json":
        for card in data["cards"]:
            if card.get("name") == "Клиник с ошибками":
                card["col"] = 12
                card["sizeX"] = 12

    # Renames and query fixes
    for old, new in RENAMES.items():
        rename_in_obj(data, old, new)

    for card in data["cards"]:
        name = card.get("name", "")
        if card.get("display") == "text":
            continue
        dq = card.get("dataset_query", {})
        if dq.get("type") != "native":
            continue
        native = dq.setdefault("native", {})
        q = native.get("query", "")
        q = fix_round_in_query(q)
        if name in SQL_ALIAS_FIXES:
            old_alias, new_alias = SQL_ALIAS_FIXES[name]
            if old_alias != new_alias:
                q = q.replace(old_alias, new_alias)
        native["query"] = q
        apply_viz_defaults(card)

    # Update text header for heatmap section in 04
    if path.name == "04_quality_and_errors.json":
        for card in data["cards"]:
            if card.get("display") == "text" and "Heatmap" in card.get("text", ""):
                card["text"] = card["text"].replace(
                    "## Heatmap клиники × дни (доля ошибок)",
                    "## Тепловая карта: клиника × день",
                )

    # Update 05 executive placeholder text
    if path.name == "05_executive.json":
        for card in data["cards"]:
            if card.get("display") == "text" and "Помесячная динамика" in card.get("text", ""):
                card["text"] = (
                    "## Помесячная динамика (после накопления данных)\n"
                    "MRR Waterfall, NRR и Logo retention — при ≥2 закрытых месяцах. "
                    "Дневные графики MRR и активных JID выше уже доступны."
                )

    # Update descriptions referencing old names in 02
    if path.name == "02_service.json":
        for card in data["cards"]:
            desc = card.get("description", "")
            if "Последние ошибки связи" in desc:
                card["description"] = desc.replace(
                    "Последние ошибки связи", "Последние сбои транспорта"
                ).replace(
                    "дашборд «Ошибки и качество данных»",
                    "раздел «Ошибки связи» на дашборде качества",
                )

    return data


def main() -> None:
    for path in sorted(DASHBOARDS_DIR.glob("*.json")):
        updated = process_dashboard(path)
        path.write_text(
            json.dumps(updated, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"Updated {path.name}: {len(updated['cards'])} cards")


if __name__ == "__main__":
    main()
