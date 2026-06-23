#!/usr/bin/env python3
"""Apply shared «Объём по клиникам» native card (with % column) to all placements."""
from __future__ import annotations

import copy
import json
from pathlib import Path

from clinic_volume_card import clinic_volume_card

ROOT = Path(__file__).resolve().parents[1]
INTEGRATION = ROOT / "metabase_dashboards/01_integration_egisz.json"
ARCHIVE = ROOT / "metabase_dashboards/06_semd_archive.json"

OPERATIONAL_LAYOUT = {"row": 8, "col": 16, "size_x": 8, "size_y": 7}
ARCHIVE_LAYOUT = {"row": 6, "col": 12, "size_x": 12, "size_y": 8}


def main() -> None:
    integration = json.loads(INTEGRATION.read_text(encoding="utf-8"))
    integration["cards"] = [
        c for c in integration["cards"] if c.get("name") != "Объём по клиникам"
    ]
    integration["cards"].append(
        clinic_volume_card(tab="operational", with_click=True, **OPERATIONAL_LAYOUT)
    )
    integration["cards"].append(
        clinic_volume_card(tab="archive", with_click=True, **ARCHIVE_LAYOUT)
    )
    INTEGRATION.write_text(
        json.dumps(integration, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    standalone = json.loads(ARCHIVE.read_text(encoding="utf-8"))
    standalone["cards"] = [c for c in standalone["cards"] if c.get("name") != "Объём по клиникам"]
    standalone_card = clinic_volume_card(with_click=False, **ARCHIVE_LAYOUT)
    standalone["cards"].append(standalone_card)
    standalone["cards"].sort(key=lambda c: (c.get("row", 0), c.get("col", 0)))
    ARCHIVE.write_text(
        json.dumps(standalone, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    print("Applied «Объём по клиникам» with % column")


if __name__ == "__main__":
    main()
