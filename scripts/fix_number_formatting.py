"""Normalize Metabase dashboard number formatting to Russian locale."""
from __future__ import annotations

import json
from pathlib import Path

DEFAULT_NUMBER_SEPARATORS = ", "
INTEGER_DECIMALS = 0
PERCENT_DECIMALS = 1
FRACTIONAL_DECIMALS = 1

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
    "localUid",
    "OID",
    "ИНН",
    "СНИЛС",
    "ФИО",
    "текст",
    "Сводка",
    "Исходный",
    "Хост",
    "Сообщение",
    "emdrid",
    "Рег. номер",
    "relatesTo",
    "Врач",
    "Пациент",
    "document_type",
)


def column_name_from_key(key: str) -> str:
    if '["name","' in key:
        return key.split('["name","', 1)[1].rsplit('"]', 1)[0]
    return key


def is_text_column_key(key: str) -> bool:
    if key.endswith('"_percentage"]'):
        return False
    return any(marker in key for marker in TEXT_COLUMN_MARKERS)


def is_percent_column(key: str, settings: dict) -> bool:
    if key.endswith('"_percentage"]'):
        return True
    if settings.get("suffix") == " %":
        return True
    col = column_name_from_key(key)
    return col == "%" or col.endswith(", %") or col.endswith(" %")


def is_fractional_column(key: str, settings: dict, card: dict) -> bool:
    col = column_name_from_key(key)
    if "/" in col:
        return True
    if ", мин" in col or col == "Доставка, мин":
        return True
    if col == "₽ за успешный СЭМД" and card.get("name") == "Эфф. цена успешного СЭМД, ₽":
        return True
    return False


def target_decimals(key: str, settings: dict, card: dict) -> int:
    if is_percent_column(key, settings):
        return PERCENT_DECIMALS
    if is_fractional_column(key, settings, card):
        return FRACTIONAL_DECIMALS
    return INTEGER_DECIMALS


def apply_ru_number_format(key: str, settings: dict, card: dict) -> bool:
    changed = False
    decimals = target_decimals(key, settings, card)
    if settings.get("number_separators") != DEFAULT_NUMBER_SEPARATORS:
        settings["number_separators"] = DEFAULT_NUMBER_SEPARATORS
        changed = True
    if settings.get("decimals") != decimals:
        settings["decimals"] = decimals
        changed = True
    return changed


def fix_card(card: dict) -> list[str]:
    changes: list[str] = []
    name = card.get("name", "(text)")
    viz = card.setdefault("visualization_settings", {})
    cs = viz.get("column_settings")

    if name == "Среднее время доставки" and card.get("display") == "scalar" and cs:
        key = '["name","Среднее время доставки"]'
        if key in cs:
            cs[key] = {}
            changes.append(f"{name}: removed numeric formatting from text scalar")

    cs = viz.get("column_settings")
    if cs:
        for key, settings in list(cs.items()):
            if not isinstance(settings, dict):
                continue

            if is_text_column_key(key):
                if "suffix" not in settings and settings.get("decimals", 0) == 0:
                    if settings.pop("number_separators", None) is not None:
                        changes.append(f"{name}: removed separator from text column")
                    if settings.pop("decimals", None) == 0:
                        changes.append(f"{name}: removed decimals from text column")
                    if not settings:
                        cs.pop(key)
                continue

            if not any(k in settings for k in ("decimals", "number_separators", "suffix")):
                continue

            if apply_ru_number_format(key, settings, card):
                changes.append(f"{name}: RU number formatting for {key}")

        if not cs:
            viz.pop("column_settings", None)

    if card.get("display") == "pie" and viz.get("pie.decimal_places", 0) >= 1:
        if viz.get("pie.decimal_places") != PERCENT_DECIMALS:
            viz["pie.decimal_places"] = PERCENT_DECIMALS
            changes.append(f"{name}: pie.decimal_places -> {PERCENT_DECIMALS}")

        metric = viz.get("pie.metric") or (viz.get("graph.metrics") or [None])[0]
        pie_cs = viz.setdefault("column_settings", {})
        if metric:
            metric_key = f'["name","{metric}"]'
            entry = pie_cs.setdefault(metric_key, {})
            if apply_ru_number_format(metric_key, entry, card):
                changes.append(f"{name}: pie count metric integer formatting")
        percent_key = '["name","_percentage"]'
        percent_entry = pie_cs.setdefault(percent_key, {})
        if apply_ru_number_format(percent_key, percent_entry, card):
            changes.append(f"{name}: pie slice percent formatting")

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
            elif settings and settings.get("number_separators") != DEFAULT_NUMBER_SEPARATORS:
                issues.append(f"{path.name}/{name}: bad separator")
            elif settings and key and settings.get("decimals") != target_decimals(key, settings, card):
                issues.append(f"{path.name}/{name}: bad decimals")
    return issues


if __name__ == "__main__":
    main()
