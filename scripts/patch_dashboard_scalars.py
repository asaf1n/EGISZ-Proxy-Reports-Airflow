#!/usr/bin/env python3
"""Patch integration dashboard: restore archive top clinics, drop scalar KPI rows."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
INTEGRATION = ROOT / "metabase_dashboards/01_integration_egisz.json"

REMOVE_BY_TAB: dict[str, set[str]] = {
    "errors": {
        "Документов с ошибкой",
        "Доля ошибок, %",
        "Клиник с ошибками",
        "Ошибок регистрации в РЭМД",
    },
    "service": {
        "Сводка прокси-БД и очереди",
        "Документов за период",
        "Доля ошибок за период, %",
        "Успешно за период",
        "В обработке",
    },
}


def compact_tab_rows(cards: list[dict[str, Any]], tab: str) -> None:
    tab_cards = [c for c in cards if c.get("tab") == tab and c.get("display") != "text"]
    if len(tab_cards) < 2:
        return

    row_starts = sorted({c.get("row", 0) for c in tab_cards})
    band_height: dict[int, int] = {}
    for card in tab_cards:
        row = card.get("row", 0)
        end = row + card.get("sizeY", 4)
        band_height[row] = max(band_height.get(row, row), end)

    shift = 0
    prev_end = 0
    row_map: dict[int, int] = {}
    for row in row_starts:
        adjusted = row - shift
        gap = adjusted - prev_end
        if gap > 0:
            shift += gap
        row_map[row] = row - shift
        prev_end = row_map[row] + (band_height[row] - row)

    for card in tab_cards:
        card["row"] = row_map[card.get("row", 0)]


def relayout_tab(cards: list[dict[str, Any]], tab: str) -> None:
    tab_cards = [c for c in cards if c.get("tab") == tab and c.get("display") != "text"]
    if not tab_cards:
        return
    min_row = min(c.get("row", 0) for c in tab_cards)
    if min_row > 0:
        for card in tab_cards:
            card["row"] = card.get("row", 0) - min_row
    compact_tab_rows(cards, tab)


def main() -> None:
    dashboard = json.loads(INTEGRATION.read_text(encoding="utf-8"))
    cards: list[dict[str, Any]] = []
    for card in dashboard.get("cards") or []:
        tab = card.get("tab")
        name = card.get("name")
        if tab in REMOVE_BY_TAB and name in REMOVE_BY_TAB[tab]:
            continue
        cards.append(card)

    for tab in REMOVE_BY_TAB:
        relayout_tab(cards, tab)

    dashboard["cards"] = cards
    INTEGRATION.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    import unify_clinic_volume_card

    unify_clinic_volume_card.main()

    print("Patched dashboards")
    dashboard = json.loads(INTEGRATION.read_text(encoding="utf-8"))
    for tab in ("errors", "service", "archive"):
        names = [
            c["name"]
            for c in dashboard["cards"]
            if c.get("tab") == tab and c.get("display") != "text"
        ]
        print(f"  {tab}: {len(names)} cards")


if __name__ == "__main__":
    main()
