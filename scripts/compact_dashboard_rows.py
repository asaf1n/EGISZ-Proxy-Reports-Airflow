#!/usr/bin/env python3
"""Compact vertical gaps on dashboard tabs after card removal."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

INTEGRATION = Path(__file__).resolve().parents[1] / "metabase_dashboards/01_integration_egisz.json"


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


def main() -> None:
    dashboard = json.loads(INTEGRATION.read_text(encoding="utf-8"))
    cards = dashboard["cards"]
    for tab in ("errors", "service", "archive"):
        compact_tab_rows(cards, tab)
    INTEGRATION.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("compacted")


if __name__ == "__main__":
    main()
