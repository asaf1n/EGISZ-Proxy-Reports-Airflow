"""
Export Metabase dashboard to project JSON format.
Usage: python scripts/export_dashboard.py <dashboard_id> <output_file> [--keep-parameters <existing_json>]
"""
from __future__ import annotations
import json
import sys
import urllib.request
from pathlib import Path

MB_URL = "http://127.0.0.1:3000"
TOKEN = sys.argv[1]
DASHBOARD_ID = int(sys.argv[2])
OUTPUT = Path(sys.argv[3])
KEEP_PARAMS_FROM = Path(sys.argv[4]) if len(sys.argv) > 4 else None


def api_get(path: str) -> dict:
    req = urllib.request.Request(f"{MB_URL}{path}", headers={"X-Metabase-Session": TOKEN})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read().decode("utf-8"))


def clean_template_tags(tags: dict) -> dict:
    """Strip 'dimension' key (contains volatile field IDs) from each template tag."""
    result = {}
    for name, tag in (tags or {}).items():
        result[name] = {k: v for k, v in tag.items() if k != "dimension"}
    return result


def extract_query_and_tags(card: dict) -> tuple[str, dict]:
    """Handle both Metabase v0.50 (native.query) and v0.60 (stages[0]) formats."""
    dq = card.get("dataset_query") or {}
    if "stages" in dq:
        stage = dq["stages"][0]
        query = stage.get("native") or ""
        tags = stage.get("template-tags") or {}
    else:
        native = dq.get("native") or {}
        query = native.get("query") or ""
        tags = native.get("template-tags") or {}
    return query, clean_template_tags(tags)


def load_existing_field_filters(path: Path | None) -> dict[str, dict]:
    """Map card name -> metabase-field-filters block from a previously-saved JSON."""
    if not path or not path.exists():
        return {}
    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    result: dict[str, dict] = {}
    for c in existing.get("cards") or []:
        name = c.get("name")
        ff = c.get("metabase-field-filters")
        if name and ff:
            result[name] = ff
    return result


def export_dashboard(dash_id: int, keep_params_from: Path | None) -> dict:
    dashboard = api_get(f"/api/dashboard/{dash_id}")
    dashcards = dashboard.get("dashcards") or dashboard.get("ordered_cards") or []
    existing_field_filters = load_existing_field_filters(keep_params_from)

    def is_text_dc(dc: dict) -> bool:
        viz = dc.get("visualization_settings") or {}
        virtual = viz.get("virtual_card") or {}
        return virtual.get("display") == "text"

    # Fetch all unique SQL cards
    card_cache: dict[int, dict] = {}
    for dc in dashcards:
        if dc.get("card") and dc["card"].get("id"):
            cid = dc["card"]["id"]
            if cid not in card_cache:
                card_cache[cid] = api_get(f"/api/card/{cid}")
                print(f"  Fetched card {cid}: {card_cache[cid]['name']}", file=sys.stderr)

    # Sort dashcards by (row, col); keep SQL cards and text dashcards
    valid_dcs = [
        dc for dc in dashcards
        if (dc.get("card") and dc["card"].get("id")) or is_text_dc(dc)
    ]
    valid_dcs.sort(key=lambda dc: (dc.get("row", 0), dc.get("col", 0)))

    cards = []
    for dc in valid_dcs:
        if is_text_dc(dc) and not (dc.get("card") and dc["card"].get("id")):
            viz = dc.get("visualization_settings") or {}
            card_obj = {
                "display": "text",
                "text": viz.get("text", ""),
                "sizeX": dc.get("size_x", 12),
                "sizeY": dc.get("size_y", 6),
                "row": dc.get("row", 0),
                "col": dc.get("col", 0),
            }
            cards.append(card_obj)
            continue

        cid = dc["card"]["id"]
        card = card_cache[cid]
        query, tags = extract_query_and_tags(card)

        card_obj = {
            "name": card["name"],
            "description": card.get("description"),
            "dataset_query": {
                "type": "native",
                "native": {
                    "query": query,
                    "template-tags": tags,
                },
                "database": 1,
            },
            "display": card.get("display", "table"),
            "visualization_settings": card.get("visualization_settings") or {},
            "sizeX": dc.get("size_x", 12),
            "sizeY": dc.get("size_y", 6),
            "row": dc.get("row", 0),
            "col": dc.get("col", 0),
        }
        prior_ff = existing_field_filters.get(card["name"])
        if prior_ff:
            ff = {k: v for k, v in prior_ff.items() if k in tags}
            if ff:
                card_obj["metabase-field-filters"] = ff
        cards.append(card_obj)

    # Use existing parameters if requested, else use from Metabase dashboard
    if keep_params_from and keep_params_from.exists():
        existing = json.loads(keep_params_from.read_text(encoding="utf-8"))
        parameters = existing.get("parameters", [])
    else:
        parameters = dashboard.get("parameters") or []

    return {
        "name": dashboard["name"],
        "width": "full",
        "description": dashboard.get("description") or "",
        "parameters": parameters,
        "cards": cards,
    }


if __name__ == "__main__":
    print(f"Exporting dashboard {DASHBOARD_ID}...", file=sys.stderr)
    result = export_dashboard(DASHBOARD_ID, KEEP_PARAMS_FROM)
    OUTPUT.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Saved {len(result['cards'])} cards → {OUTPUT}", file=sys.stderr)
