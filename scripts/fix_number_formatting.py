"""Normalize Metabase dashboard number formatting to Russian-style space thousands."""
from __future__ import annotations

import json
from pathlib import Path

TEXT_COLUMN_MARKERS = (
    "Клиника",
    "клиника",
    "Вид ошибки",
    "Категория ошибки",
    "Тип ошибки",
    "Тип СЭМД",
    "Наименование",
    "Код СЭМД",
    "Статус",
    "День",
    "Дата",
    "Час",
    "lbl",
    "code",
    "Сегмент",
    "Тип сетевой",
    "Категория",
    "Сигнал",
)


def is_text_column_key(key: str) -> bool:
    return any(marker in key for marker in TEXT_COLUMN_MARKERS)


def fix_card(card: dict) -> list[str]:
    changes: list[str] = []
    name = card.get("name", "(text)")
    viz = card.setdefault("visualization_settings", {})
    cs = viz.get("column_settings")
    if not cs:
        return changes

    if name == "Среднее время доставки" and card.get("display") == "scalar":
        key = '["name","Среднее время доставки"]'
        if key in cs:
            cs[key] = {}
            changes.append(f"{name}: removed numeric formatting from text scalar")

    if name == "Клиник без единого успеха":
        key = '["name","Клиник без успеха"]'
        cs[key] = {"decimals": 0, "number_separators": " "}
        changes.append(f"{name}: fixed count formatting (was wrongly shown as percent)")

    for key, settings in list(cs.items()):
        if not isinstance(settings, dict):
            continue

        if settings.get("number_separators") == ".":
            settings["number_separators"] = " "
            changes.append(f"{name}: space thousands separator")

        if "decimals" in settings and "number_separators" not in settings:
            settings["number_separators"] = " "
            changes.append(f"{name}: added space thousands separator")

        if name == "ЭМД/пациент" and key.endswith('ЭМД/пациент"]'):
            if settings.get("decimals") == 0:
                settings["decimals"] = 1
                changes.append(f"{name}: decimals 0 -> 1")

        if name == "ЭМД в сутки (среднее по периоду)" and key.endswith('ЭМД/сутки"]'):
            if settings.get("decimals") == 0:
                settings["decimals"] = 1
                changes.append(f"{name}: decimals 0 -> 1")

        if is_text_column_key(key) and "suffix" not in settings and settings.get("decimals", 0) == 0:
            if settings.pop("number_separators", None) is not None:
                changes.append(f"{name}: removed separator from text column")
            if settings.pop("decimals", None) == 0:
                changes.append(f"{name}: removed decimals from text column")
            if not settings:
                cs.pop(key)

    if not cs:
        viz.pop("column_settings", None)
    return changes


def main() -> None:
    root = Path(__file__).resolve().parents[1] / "metabase_dashboards"
    for path in sorted(root.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        changes: list[str] = []
        for card in data.get("cards", []):
            changes.extend(fix_card(card))
        if changes:
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            print(f"{path.name}: {len(changes)} changes")


def check_scalar_cards() -> list[str]:
    issues: list[str] = []
    root = Path(__file__).resolve().parents[1] / "metabase_dashboards"
    for path in sorted(root.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        for card in data.get("cards", []):
            if card.get("display") != "scalar":
                continue
            name = card.get("name", "")
            viz = card.get("visualization_settings") or {}
            field = viz.get("scalar.field")
            cs = viz.get("column_settings") or {}
            key = f'["name","{field}"]' if field else None
            settings = cs.get(key) if key else None
            query = ((card.get("dataset_query") or {}).get("native") or {}).get("query", "")
            is_text_scalar = "::text" in query and field and field in query
            if is_text_scalar and settings:
                issues.append(f"{path.name}/{name}: text scalar still has column_settings")
            elif not is_text_scalar and field and not settings:
                issues.append(f"{path.name}/{name}: numeric scalar missing column_settings")
            elif settings and settings.get("number_separators") not in (None, " "):
                issues.append(f"{path.name}/{name}: bad separator")
    return issues


if __name__ == "__main__":
    main()
