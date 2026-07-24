"""Reorganize Metabase dashboards: compact 24-column grids, no operational/service scalar KPIs."""
from __future__ import annotations

import json
import sys
from pathlib import Path

# up.ps1 запускает скрипт под PowerShell с cp1251-консолью; имена карточек содержат
# символы вне cp1251 (например «×»). Печатаем в UTF-8, иначе print() падает.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:  # pragma: no cover - reconfigure доступен с Python 3.7+
    pass

ROOT = Path(__file__).resolve().parents[1]
INTEGRATION = ROOT / "metabase_dashboards" / "01_integration_egisz.json"


def write_json_if_changed(path: Path, data: dict) -> bool:
    text = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == text:
        return False
    path.write_text(text, encoding="utf-8")
    return True

OPERATIONAL_SCALAR_NAMES = frozenset({"Всего документов", "Всего клиник", "В обработке"})
SERVICE_SCALAR_NAMES = frozenset({"Сбоев связи за период", "Пик сбоев связи за день"})

OPERATIONAL_LAYOUT: dict[str, tuple[int, int, int, int]] = {
    "Последние операции": (0, 0, 24, 8),
    "Транзакции по дням и статусам": (8, 0, 16, 6),
    "Статусы за период": (8, 16, 8, 6),
    "Топ по типу ошибки": (14, 0, 12, 8),
    "Успешность по типам СЭМД": (14, 12, 12, 8),
    "Объём по клиникам": (22, 0, 12, 8),
    "Успешность по клиникам": (22, 12, 12, 8),
    "Объём ошибок по клиникам": (30, 0, 24, 7),
    "Тепловая карта: клиника × день": (37, 0, 24, 10),
}

ARCHIVE_LAYOUT: dict[str, tuple[int, int, int, int]] = {
    "Объём по клиникам": (0, 0, 12, 7),
    "Топ типов СЭМД по документам": (0, 12, 12, 7),
    "Всего документов": (7, 0, 4, 3),
    "Динамика документов по дням": (7, 4, 20, 5),
    "Всего клиник": (10, 0, 4, 2),
    "Архив СЭМД": (12, 0, 24, 10),
}

SERVICE_LAYOUT: dict[str, tuple[int, int, int, int]] = {
    "Отказы по часам: связь и асинхронный ответ": (0, 0, 20, 6),
    "РЭМД vs связь": (0, 20, 4, 6),
    "Детализация healthcheck": (6, 0, 14, 6),
    "Контроль качества данных": (6, 14, 10, 6),
    "Детализация контроля качества": (12, 0, 24, 8),
    "Тренд ошибок связи по дням": (20, 0, 24, 5),
    "Топ клиник по сбоям транспорта": (25, 0, 12, 6),
    "Типы сетевых ошибок (за период)": (25, 12, 12, 6),
    "Последние сбои транспорта": (31, 0, 24, 5),
}

ERRORS_LAYOUT: dict[str, tuple[int, int, int, int]] = {
    "Топ типов СЭМД по ошибкам": (0, 0, 12, 7),
    "Топ типов СЭМД по видам ошибки": (0, 12, 12, 7),
    "Топ по типу ошибки": (7, 0, 12, 7),
    "Топ категорий и типов ошибки": (7, 12, 12, 7),
    "Ошибки: тип × клиника": (14, 0, 24, 8),
    "% ошибок: клиника × тип СЭМД": (22, 0, 12, 7),
    "% ошибок: тип ошибки × тип СЭМД": (22, 12, 12, 7),
}

QUEUE_LAYOUT: dict[str, tuple[int, int, int, int]] = {
    "В очереди (всего)": (0, 0, 6, 3),
    "Зависших >3 дней": (0, 6, 6, 3),
    "Зависших >7 дней": (0, 12, 6, 3),
    "Зависших >30 дней": (0, 18, 6, 3),
    "Очередь без ответа": (3, 0, 24, 10),
    "Топ клиник в очереди по документам": (13, 0, 12, 7),
    "Сегменты ожидания": (13, 12, 12, 7),
    "Топ типов СЭМД в очереди": (20, 0, 24, 7),
}

EXECUTIVE_LAYOUT: dict[str, tuple[int, int, int, int]] = {
    "Успешных СЭМД (NSM)": (1, 0, 8, 3),
    "Документов за период": (1, 8, 8, 3),
    "Доля успеха с первой попытки, %": (1, 16, 8, 3),
    "MRR (30 дн.), ₽": (6, 0, 6, 3),
    "ARR (год.), ₽": (6, 6, 6, 3),
    "Активных JID (30 дн)": (6, 12, 6, 3),
    "Эфф. цена успешного СЭМД, ₽": (6, 18, 6, 3),
    "Динамика MRR (30 дн.), ₽": (10, 0, 12, 6),
    "Динамика активных JID (30 дн.)": (10, 12, 12, 6),
    "Зависших >7 дн., %": (18, 0, 8, 3),
    "Отказов РЭМД, %": (18, 8, 8, 3),
    "Ошибок связи, %": (18, 16, 8, 3),
    "Сегменты ценности (MRR × ₽/успех)": (23, 0, 24, 5),
    "Выручка под риском «ноль ценности», ₽": (28, 0, 8, 3),
    "Выручка под риском «затык канала», ₽": (28, 8, 8, 3),
    "Клиник без единого успеха": (28, 16, 8, 3),
    "Очередь оттока: JID с нулём успехов": (31, 0, 24, 7),
}

# row/col отсчитываются ВНУТРИ вкладки (Обзор / Ошибки регистрации ЭМД / Документы),
# поэтому карточки разных вкладок могут делить одинаковые координаты.
CLIENT_SERVICE_LAYOUT: dict[str, tuple[int, int, int, int]] = {
    "Документов за период — клиент": (1, 0, 6, 3),
    "Успешно зарегистрирован": (1, 6, 6, 3),
    "Ошибка асинхронного ответа РЭМД": (1, 12, 6, 3),
    "Ошибка связи": (1, 18, 6, 3),
    "Динамика статусов по дням": (4, 0, 12, 7),
    "Топ типов СЭМД — клиент": (4, 12, 12, 7),
    "Топ-10 типов СЭМД по документам": (11, 0, 24, 5),
    "Объёмы ошибок по категориям — клиент": (1, 0, 12, 7),
    "Структура ошибок по категориям — клиент": (1, 12, 12, 7),
    "Топ типов ошибок — клиент": (8, 0, 24, 7),
    "Динамика ошибок по дням — клиент": (15, 0, 12, 7),
    "Топ СЭМД по ошибкам — клиент": (15, 12, 12, 7),
    "Журнал документов — клиент": (1, 0, 24, 12),
    "Доступные типы СЭМД — клиент": (1, 0, 24, 10),
}

CLIENT_BI_LAYOUT: dict[str, tuple[int, int, int, int]] = {
    "Документов за период": (1, 0, 6, 3),
    "Доля успеха с первой попытки (клиент), %": (1, 6, 6, 3),
    "Медиана доставки, мин": (1, 12, 6, 3),
    "ЭМД в сутки (среднее по периоду)": (1, 18, 6, 3),
    "Динамика документов по типам СЭМД": (4, 0, 12, 7),
    "% успеха регистрации по типам СЭМД": (4, 12, 12, 7),
    "Среднее время доставки по типам СЭМД": (11, 0, 24, 6),
    "Журнал документов с ошибками регистрации": (17, 0, 24, 7),
    "Уникальных пациентов": (27, 0, 8, 3),
    "Уникальных врачей": (27, 8, 8, 3),
    "ЭМД на пациента (среднее)": (27, 16, 8, 3),
    "Распределение пациентов по числу ЭМД": (30, 0, 12, 6),
    "Топ врачей по документам": (30, 12, 12, 6),
}

CLIENT_BI_TEXT_LAYOUT: dict[str, tuple[int, int, int, int]] = {
    "Медицинские показатели (агрегаты по пациентам и врачам)": (24, 0, 24, 1),
}

# Вкладки «Динамика по неделям» / «Динамика по месяцам» управленческого дашборда:
# row/col отсчитываются внутри вкладки, поэтому сетки совпадают.
WEEKLY_LAYOUT: dict[str, tuple[int, int, int, int]] = {
    "Статусы по неделям": (1, 0, 12, 6),
    "Объём документов по неделям": (1, 12, 12, 6),
    "Контрольная p-карта: доля ошибок по неделям": (7, 0, 24, 6),
    "Категории ошибок по неделям": (14, 0, 24, 6),
    "Сводка по неделям": (20, 0, 24, 7),
}

MONTHLY_LAYOUT: dict[str, tuple[int, int, int, int]] = {
    "Статусы по месяцам": (1, 0, 12, 6),
    "Объём документов по месяцам": (1, 12, 12, 6),
    "Контрольная p-карта: доля ошибок по месяцам": (7, 0, 24, 6),
    "Категории ошибок по месяцам": (14, 0, 24, 6),
    "Сводка по месяцам": (20, 0, 24, 7),
}


def _apply_layout(card: dict, layout: tuple[int, int, int, int]) -> None:
    row, col, size_x, size_y = layout
    card["row"] = row
    card["col"] = col
    card["sizeX"] = size_x
    card["sizeY"] = size_y


def _layout_named_cards(
    dashboard: dict,
    layout: dict[str, tuple[int, int, int, int]],
    *,
    text_layout: dict[str, tuple[int, int, int, int]] | None = None,
) -> None:
    for card in dashboard["cards"]:
        name = card.get("name", "")
        if text_layout and card.get("display") == "text" and name in text_layout:
            _apply_layout(card, text_layout[name])
            continue
        if card.get("display") == "text":
            continue
        if name in layout:
            _apply_layout(card, layout[name])


def _layout_integration(dashboard: dict) -> None:
    removed_scalars = {
        "operational": OPERATIONAL_SCALAR_NAMES,
        "service": SERVICE_SCALAR_NAMES,
    }
    dashboard["cards"] = [
        card
        for card in dashboard["cards"]
        if not (
            card.get("name") in removed_scalars.get(card.get("tab") or "", frozenset())
            and card.get("display") == "scalar"
        )
    ]

    tab_layouts = {
        "operational": OPERATIONAL_LAYOUT,
        "archive": ARCHIVE_LAYOUT,
        "service": SERVICE_LAYOUT,
        "errors": ERRORS_LAYOUT,
        "queue": QUEUE_LAYOUT,
    }
    for card in dashboard["cards"]:
        if card.get("display") == "text":
            continue
        tab = card.get("tab")
        name = card.get("name", "")
        layout = tab_layouts.get(tab or "", {}).get(name)
        if layout:
            _apply_layout(card, layout)


def main() -> None:
    integration = json.loads(INTEGRATION.read_text(encoding="utf-8"))
    _layout_integration(integration)
    write_json_if_changed(INTEGRATION, integration)
    op = [c["name"] for c in integration["cards"] if c.get("tab") == "operational"]
    print(f"operational ({len(op)}):", ", ".join(op))

    other = {
        ROOT / "metabase_dashboards" / "05_executive.json": {
            **EXECUTIVE_LAYOUT,
            **WEEKLY_LAYOUT,
            **MONTHLY_LAYOUT,
        },
        ROOT / "metabase_dashboards" / "07_client_service.json": CLIENT_SERVICE_LAYOUT,
    }
    for path, layout in other.items():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        _layout_named_cards(dashboard, layout)
        if write_json_if_changed(path, dashboard):
            print(f"updated {path.name}")

    bi_path = ROOT / "metabase_dashboards" / "08_client_bianalytic.json"
    bi_dashboard = json.loads(bi_path.read_text(encoding="utf-8"))
    _layout_named_cards(bi_dashboard, CLIENT_BI_LAYOUT, text_layout=CLIENT_BI_TEXT_LAYOUT)
    if write_json_if_changed(bi_path, bi_dashboard):
        print(f"updated {bi_path.name}")


if __name__ == "__main__":
    main()
