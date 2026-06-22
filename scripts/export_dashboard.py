#!/usr/bin/env python3
"""Export Metabase dashboards into metabase_dashboards/*.json project format.

Usage:
  python scripts/export_dashboard.py
      Export every dashboard JSON in metabase_dashboards/ by matching dashboard
      name against the configured Metabase collection.

  python scripts/export_dashboard.py <dashboard_id> <output_file>
      Export a single dashboard by Metabase id into the given path.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

MB_URL = os.environ.get("MB_URL", "http://127.0.0.1:3000")
ADMIN_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@egisz.local")
ADMIN_PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "egisz")
DASHBOARDS_DIR = Path(__file__).resolve().parents[1] / "metabase_dashboards"
COLLECTION_NAME = os.environ.get("METABASE_COLLECTION_NAME", "Интеграция с ЕГИСЗ")


def api(method: str, path: str, token: str, payload: dict | None = None) -> object:
    data = None
    headers = {"X-Metabase-Session": token}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(f"{MB_URL}{path}", data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} -> HTTP {exc.code}: {body}") from exc
    return json.loads(body) if body else None


def login() -> str:
    payload = json.dumps({"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD}).encode("utf-8")
    req = urllib.request.Request(
        f"{MB_URL}/api/session",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    token = body.get("id")
    if not token:
        raise RuntimeError(f"login failed: {body}")
    return token


def collection_dashboard_ids(token: str) -> dict[str, int]:
    collections = api("GET", "/api/collection", token)
    col_id = next(c["id"] for c in collections if c.get("name") == COLLECTION_NAME)
    items = api("GET", f"/api/collection/{col_id}/items?models=dashboard&limit=1000", token)
    return {
        row["name"]: row["id"]
        for row in items.get("data", [])
        if row.get("model") == "dashboard"
    }


def clean_template_tags(tags: dict) -> dict:
    """Strip volatile Metabase field ids from template tags."""
    result: dict = {}
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
    for card in existing.get("cards") or []:
        name = card.get("name")
        field_filters = card.get("metabase-field-filters")
        if name and field_filters:
            result[name] = field_filters
    return result


def export_dashboard(token: str, dash_id: int, keep_params_from: Path | None) -> dict:
    dashboard = api("GET", f"/api/dashboard/{dash_id}", token)
    dashcards = dashboard.get("dashcards") or dashboard.get("ordered_cards") or []
    existing_field_filters = load_existing_field_filters(keep_params_from)

    def is_text_dc(dc: dict) -> bool:
        viz = dc.get("visualization_settings") or {}
        virtual = viz.get("virtual_card") or {}
        return virtual.get("display") == "text"

    card_cache: dict[int, dict] = {}
    for dc in dashcards:
        if dc.get("card") and dc["card"].get("id"):
            card_id = dc["card"]["id"]
            if card_id not in card_cache:
                card_cache[card_id] = api("GET", f"/api/card/{card_id}", token)
                print(f"  Fetched card {card_id}: {card_cache[card_id]['name']}", file=sys.stderr)

    valid_dcs = [
        dc for dc in dashcards
        if (dc.get("card") and dc["card"].get("id")) or is_text_dc(dc)
    ]
    valid_dcs.sort(key=lambda dc: (dc.get("row", 0), dc.get("col", 0)))

    cards = []
    for dc in valid_dcs:
        if is_text_dc(dc) and not (dc.get("card") and dc["card"].get("id")):
            viz = dc.get("visualization_settings") or {}
            cards.append({
                "display": "text",
                "text": viz.get("text", ""),
                "sizeX": dc.get("size_x", 12),
                "sizeY": dc.get("size_y", 6),
                "row": dc.get("row", 0),
                "col": dc.get("col", 0),
            })
            continue

        card_id = dc["card"]["id"]
        card = card_cache[card_id]
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
            field_filters = {k: v for k, v in prior_ff.items() if k in tags}
            if field_filters:
                card_obj["metabase-field-filters"] = field_filters
        cards.append(card_obj)

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


def write_dashboard(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def export_all(token: str) -> int:
    by_name = collection_dashboard_ids(token)
    failures: list[str] = []

    for path in sorted(DASHBOARDS_DIR.glob("*.json")):
        existing = json.loads(path.read_text(encoding="utf-8"))
        name = existing.get("name")
        if not name:
            failures.append(f"{path.name}: missing dashboard name")
            continue

        dash_id = by_name.get(name)
        if dash_id is None:
            failures.append(f"{path.name}: dashboard {name!r} not found in collection {COLLECTION_NAME!r}")
            print(f"[MISSING] {name} ({path.name})", file=sys.stderr)
            continue

        print(f"Exporting {name} (id={dash_id}) -> {path.name}...", file=sys.stderr)
        payload = export_dashboard(token, dash_id, keep_params_from=path)
        write_dashboard(path, payload)
        print(f"[OK] {path.name}: {len(payload['cards'])} cards", file=sys.stderr)

    if failures:
        print("=== FAILURES ===", file=sys.stderr)
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1

    return 0


def export_one(token: str, dash_id: int, output: Path) -> int:
    keep_params_from = output if output.exists() else None
    print(f"Exporting dashboard {dash_id} -> {output}...", file=sys.stderr)
    payload = export_dashboard(token, dash_id, keep_params_from)
    write_dashboard(output, payload)
    print(f"Saved {len(payload['cards'])} cards -> {output}", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    token = login()

    if not args:
        return export_all(token)

    if len(args) == 2:
        return export_one(token, int(args[0]), Path(args[1]))

    print(__doc__.strip(), file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
