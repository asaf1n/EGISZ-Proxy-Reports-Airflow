"""Sync Metabase visualization metric labels with SQL aliases (Документов)."""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path


def sync_card_visualization(card: dict) -> bool:
    dq = card.get("dataset_query") or {}
    nat = dq.get("native") or {}
    q = nat.get("query")
    if not isinstance(q, str) or 'AS "Документов"' not in q:
        return False
    vs = card.get("visualization_settings")
    if not isinstance(vs, dict):
        return False
    changed = False
    gm = vs.get("graph.metrics")
    if isinstance(gm, list) and "Количество" in gm:
        vs["graph.metrics"] = ["Документов" if x == "Количество" else x for x in gm]
        changed = True
    # column_settings keys like ["name","Количество"]
    cs = vs.get("column_settings")
    if isinstance(cs, dict):
        new_cs = {}
        for k, v in cs.items():
            if k == '[\"name\",\"Количество\"]':
                nk = '[\"name\",\"Документов\"]'
                nv = dict(v) if isinstance(v, dict) else v
                if isinstance(nv, dict):
                    nv.setdefault("column_title", "Документов")
                new_cs[nk] = nv
                changed = True
            else:
                new_cs[k] = v
        if changed:
            vs["column_settings"] = new_cs
    return changed


def main() -> None:
    root = Path("metabase_dashboards")
    for fp in sorted(root.glob("*.json")):
        data = json.loads(fp.read_text(encoding="utf-8"))
        cards = data.get("cards")
        if not isinstance(cards, list):
            continue
        n = 0
        for card in cards:
            if isinstance(card, dict) and sync_card_visualization(card):
                n += 1
        if n:
            fp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            print(fp.name, n)


if __name__ == "__main__":
    main()
