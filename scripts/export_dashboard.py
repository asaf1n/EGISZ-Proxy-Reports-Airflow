#!/usr/bin/env python3
"""Export Metabase dashboards into metabase_dashboards/*.json project format.

Usage:
  python scripts/export_dashboard.py
      Export every dashboard JSON in metabase_dashboards/ by matching dashboard
      name against the configured Metabase collection.

  python scripts/export_dashboard.py <dashboard_id> <output_file>
      Export a single dashboard by Metabase id into the given path.

Выгрузка перезаписывает файлы, которые собирают apply_dashboard_plan.py и
layout_operational_tab.py: если живой Metabase отстал от генераторов, экспорт вернёт
старую карточку, а следующий прогон генератора молча её перепишет. После экспорта
прогонять генераторы и `pytest tests/test_dashboards.py`, а осознанные правки карточек
переносить в генератор.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from mb_api import api, login

DASHBOARDS_DIR = Path(__file__).resolve().parents[1] / "metabase_dashboards"
COLLECTION_NAME = os.environ.get("METABASE_COLLECTION_NAME", "Интеграция с ЕГИСЗ")

PROVISIONING_KEYS = (
    "query_tier",
    "source_model",
    "metabase-parameter-targets",
    "metabase-field-filters",
    "metabase-model-drill-params",
    "click_behavior",
)

PARAM_KEYS = (
    "id",
    "name",
    "slug",
    "type",
    "sectionId",
    "default",
    "required",
    "isMultiSelect",
    # values_query_type="none" подавляет выпадающий список значений фильтра — на
    # клиентских дашбордах не раскрывает справочник клиник; терять при экспорте нельзя.
    "values_query_type",
    "values_source_type",
    "values_source_config",
    "filteringParameters",
)


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


def extract_query_and_tags(card: dict) -> tuple[str | dict, dict, str]:
    """Handle native SQL, Query Builder, and Metabase v0.62 stages formats."""
    dq = card.get("dataset_query") or {}
    if dq.get("type") == "query":
        return dq.get("query") or {}, {}, "query"
    if "stages" in dq:
        stage = dq["stages"][0]
        if stage.get("lib/type") == "mbql/query" or dq.get("type") == "query":
            return stage.get("query") or {}, {}, "query"
        # pMBQL-этап (v0.61+): QB-запрос с instance-id (source-card, field id) —
        # реверс в проектный вид без реестра моделей невозможен, запрос
        # восстанавливает keep-prior ветка по репозиторному JSON.
        if stage.get("lib/type") == "mbql.stage/mbql":
            return {}, {}, "query"
        query = stage.get("native") or ""
        tags = stage.get("template-tags") or {}
        return query, clean_template_tags(tags), "native"
    native = dq.get("native") or {}
    query = native.get("query") or ""
    tags = native.get("template-tags") or {}
    return query, clean_template_tags(tags), "native"


def source_model_from_query(query: object) -> str | None:
    if not isinstance(query, dict):
        return None
    source = query.get("source-table")
    if isinstance(source, str) and source.startswith("model:"):
        return source.split(":", 1)[1]
    return None


def load_existing_dashboard(path: Path | None) -> dict:
    if not path or not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"cannot read existing dashboard JSON: {path}") from exc


def index_existing_cards(existing: dict) -> dict[str, dict]:
    """Index prior cards for provisioning-metadata carry-over.

    Provisioning metadata (field-filters, parameter-targets, click_behavior,
    query_tier, source_model) is a property of the card identity, not its
    dashboard position — so it must survive moves. Position-keyed matching
    silently drops bindings whenever a card is reordered; we therefore index by
    name and by (name, tab) as well, and only fall back to looser keys when they
    resolve unambiguously.
    """
    by_full: dict[tuple, dict] = {}
    by_name_tab: dict[tuple, list[dict]] = {}
    by_name: dict[str, list[dict]] = {}
    for card in existing.get("cards") or []:
        name = card.get("name")
        if not name:
            continue
        by_full[(name, card.get("tab"), card.get("row"), card.get("col"))] = card
        by_name_tab.setdefault((name, card.get("tab")), []).append(card)
        by_name.setdefault(name, []).append(card)
    return {"by_full": by_full, "by_name_tab": by_name_tab, "by_name": by_name}


def match_existing_card(
    indexed: dict[str, dict],
    name: str,
    tab_slug: str | None,
    row: int,
    col: int,
) -> dict | None:
    exact = indexed["by_full"].get((name, tab_slug, row, col))
    if exact is not None:
        return exact
    same_tab = indexed["by_name_tab"].get((name, tab_slug))
    if same_tab and len(same_tab) == 1:
        return same_tab[0]
    same_name = indexed["by_name"].get(name)
    if same_name and len(same_name) == 1:
        return same_name[0]
    return None


def build_tab_slug_map(live_tabs: list[dict], existing_tabs: list[dict]) -> tuple[dict[int, str], dict[int, int]]:
    """Map live Metabase tab id -> project slug id; tab id -> sort position."""
    slug_by_name_pos: dict[tuple[str, int], str] = {}
    for tab in existing_tabs:
        slug_by_name_pos[(tab.get("name", ""), tab.get("position", 0))] = tab.get("id", "")

    id_to_slug: dict[int, str] = {}
    id_to_position: dict[int, int] = {}
    for tab in sorted(live_tabs, key=lambda t: t.get("position", 0)):
        tab_id = tab["id"]
        name = tab.get("name", "")
        position = tab.get("position", 0)
        id_to_position[tab_id] = position
        slug = slug_by_name_pos.get((name, position))
        if not slug:
            slug = slug_by_name_pos.get((name, position - 1)) or slug_by_name_pos.get((name, position + 1))
        if not slug:
            slug = f"tab_{position}"
        id_to_slug[tab_id] = slug
    return id_to_slug, id_to_position


def export_tabs(live_tabs: list[dict], existing_tabs: list[dict]) -> list[dict]:
    id_to_slug, _ = build_tab_slug_map(live_tabs, existing_tabs)
    slug_by_live_id = id_to_slug
    exported: list[dict] = []
    for tab in sorted(live_tabs, key=lambda t: t.get("position", 0)):
        exported.append(
            {
                "id": slug_by_live_id[tab["id"]],
                "name": tab.get("name", ""),
                "position": tab.get("position", 0),
            }
        )
    return exported


def export_parameters(live_parameters: list[dict], existing_parameters: list[dict]) -> list[dict]:
    existing_by_slug = {p.get("slug"): p for p in existing_parameters if p.get("slug")}
    exported: list[dict] = []
    for param in live_parameters:
        cleaned = {k: param[k] for k in PARAM_KEYS if k in param}
        slug = cleaned.get("slug")
        prior = existing_by_slug.get(slug) if slug else None
        if prior:
            for key in PARAM_KEYS:
                if key not in cleaned and key in prior:
                    cleaned[key] = prior[key]
        exported.append(cleaned)
    return exported


def merge_visualization_settings(card: dict, dashcard: dict) -> dict:
    viz = dict(card.get("visualization_settings") or {})
    dash_viz = dashcard.get("visualization_settings") or {}
    for key, value in dash_viz.items():
        if key in ("virtual_card", "click_behavior"):
            continue
        viz[key] = value
    return viz


def is_volatile_click_behavior(behavior: dict | None) -> bool:
    """Metabase serializes model drill with instance field ids — not portable in git."""
    if not behavior:
        return False
    for key in (behavior.get("parameterMapping") or {}):
        if key.startswith('["dimension",["field",'):
            return True
    return False


def is_model_compiled_native(query: str) -> bool:
    """Query Builder on a model is returned by the API as native SQL over __mb_source."""
    return '"__mb_source"' in query or '"public"."rpt_documents"' in query


def apply_provisioning_metadata(card_obj: dict, prior: dict | None, tags: dict, live_viz: dict) -> None:
    if prior:
        for key in PROVISIONING_KEYS:
            if key in prior:
                card_obj[key] = prior[key]

    live_click = live_viz.get("click_behavior") or card_obj.get("click_behavior")
    prior_click = prior.get("click_behavior") if prior else None
    if live_click and not is_volatile_click_behavior(live_click):
        card_obj["click_behavior"] = live_click
    elif prior_click and not is_volatile_click_behavior(prior_click):
        card_obj["click_behavior"] = prior_click
    elif is_volatile_click_behavior(card_obj.get("click_behavior")):
        card_obj.pop("click_behavior", None)

    if card_obj.get("query_tier") == "query_builder" or "dataset_query" in card_obj and card_obj["dataset_query"].get("type") == "query":
        if "query_tier" not in card_obj:
            card_obj["query_tier"] = "query_builder"
        if "source_model" not in card_obj:
            model = source_model_from_query(card_obj["dataset_query"].get("query"))
            if model:
                card_obj["source_model"] = model

    if prior and prior.get("metabase-field-filters"):
        field_filters = {
            k: v for k, v in prior["metabase-field-filters"].items() if k in tags
        }
        if field_filters:
            card_obj["metabase-field-filters"] = field_filters


def export_dashboard(token: str, dash_id: int, keep_params_from: Path | None) -> dict:
    dashboard = api("GET", f"/api/dashboard/{dash_id}", token)
    existing = load_existing_dashboard(keep_params_from)
    existing_cards = index_existing_cards(existing)
    existing_tabs = existing.get("tabs") or []
    live_tabs = dashboard.get("tabs") or []
    id_to_slug, id_to_position = build_tab_slug_map(live_tabs, existing_tabs)

    dashcards = dashboard.get("dashcards") or dashboard.get("ordered_cards") or []

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

    def sort_key(dc: dict) -> tuple:
        tab_id = dc.get("dashboard_tab_id")
        tab_pos = id_to_position.get(tab_id, 999) if tab_id is not None else 999
        return (tab_pos, dc.get("row", 0), dc.get("col", 0))

    valid_dcs.sort(key=sort_key)

    cards: list[dict] = []
    for dc in valid_dcs:
        row = dc.get("row", 0)
        col = dc.get("col", 0)
        size_x = dc.get("size_x", 12)
        size_y = dc.get("size_y", 6)
        tab_slug = id_to_slug.get(dc.get("dashboard_tab_id")) if dc.get("dashboard_tab_id") is not None else None

        if is_text_dc(dc) and not (dc.get("card") and dc["card"].get("id")):
            viz = dc.get("visualization_settings") or {}
            text_card: dict = {
                "display": "text",
                "text": viz.get("text", ""),
                "sizeX": size_x,
                "sizeY": size_y,
                "row": row,
                "col": col,
            }
            if tab_slug:
                text_card["tab"] = tab_slug
            cards.append(text_card)
            continue

        card_id = dc["card"]["id"]
        card = card_cache[card_id]
        name = card["name"]
        query, tags, query_type = extract_query_and_tags(card)
        live_viz = merge_visualization_settings(card, dc)
        prior = match_existing_card(existing_cards, name, tab_slug, row, col)

        if query_type == "query":
            card_obj: dict = {
                "name": name,
                "description": card.get("description"),
                "query_tier": "query_builder",
                "dataset_query": {
                    "type": "query",
                    "query": query,
                    "database": 1,
                },
                "display": card.get("display", "table"),
                "visualization_settings": live_viz,
                "sizeX": size_x,
                "sizeY": size_y,
                "row": row,
                "col": col,
            }
        else:
            card_obj = {
                "name": name,
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
                "visualization_settings": live_viz,
                "sizeX": size_x,
                "sizeY": size_y,
                "row": row,
                "col": col,
            }

        if tab_slug:
            card_obj["tab"] = tab_slug

        apply_provisioning_metadata(card_obj, prior, tags, live_viz)

        if (
            prior
            and prior.get("query_tier") == "query_builder"
            and card_obj.get("dataset_query", {}).get("type") == "native"
            and isinstance(query, str)
            and is_model_compiled_native(query)
        ):
            card_obj["query_tier"] = prior["query_tier"]
            if "source_model" in prior:
                card_obj["source_model"] = prior["source_model"]
            card_obj["dataset_query"] = prior["dataset_query"]
        elif (
            prior
            and prior.get("query_tier") == "query_builder"
            and card_obj.get("dataset_query", {}).get("type") == "query"
            and not card_obj["dataset_query"].get("query")
        ):
            card_obj["dataset_query"] = prior["dataset_query"]
        cards.append(card_obj)

    parameters = export_parameters(
        dashboard.get("parameters") or [],
        existing.get("parameters") or [],
    )

    payload: dict = {
        "name": dashboard["name"],
        "width": existing.get("width", "full"),
        "description": dashboard.get("description") or "",
        "parameters": parameters,
        "cards": cards,
    }
    if live_tabs:
        payload["tabs"] = export_tabs(live_tabs, existing_tabs)
    return payload


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
