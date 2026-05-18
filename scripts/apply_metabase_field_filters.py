"""
Дополняет metabase-field-filters у native-карточек и переводит
поддерживаемые text template-tags в настоящие dimension field filters.

Правила маппинга tag → (table_ref, field_name) живут в
metabase_dashboards/field_filter_defaults.yaml. Скрипт — резолвер,
а не носитель бизнес-логики.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import yaml


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DASHBOARDS_DIR = _PROJECT_ROOT / "metabase_dashboards"
_RULES_PATH = _DASHBOARDS_DIR / "field_filter_defaults.yaml"


def _load_rules() -> dict[str, list[dict[str, Any]]]:
    raw = yaml.safe_load(_RULES_PATH.read_text(encoding="utf-8")) or {}
    version = raw.get("version")
    if version != 2:
        raise RuntimeError(
            f"{_RULES_PATH.name}: expected version 2, got {version!r}. "
            "Update the resolver or migrate the YAML."
        )

    tags = raw.get("tags") or {}
    normalized: dict[str, list[dict[str, Any]]] = {}
    for tag_name, body in tags.items():
        rules = (body or {}).get("rules") or []
        if not isinstance(rules, list) or not rules:
            raise RuntimeError(f"{_RULES_PATH.name}: tag {tag_name!r} has no rules")
        for index, rule in enumerate(rules):
            if "table_ref" not in rule or "field_name" not in rule:
                raise RuntimeError(
                    f"{_RULES_PATH.name}: tag {tag_name!r} rule #{index} is missing table_ref/field_name"
                )
        normalized[tag_name] = rules
    return normalized


def _all_present(haystack: str, needles: Iterable[str]) -> bool:
    return all(needle in haystack for needle in needles)


def _none_present(haystack: str, needles: Iterable[str]) -> bool:
    return all(needle not in haystack for needle in needles)


def _rule_matches(rule: dict[str, Any], display_name: str, query: str) -> bool:
    display_lower = display_name.lower()
    if not _all_present(query, rule.get("query_contains") or []):
        return False
    if not _none_present(query, rule.get("query_not_contains") or []):
        return False
    if not _all_present(display_name, rule.get("display_name_contains") or []):
        return False
    if not _none_present(display_name, rule.get("display_name_not_contains") or []):
        return False
    if not _all_present(display_lower, [s.lower() for s in (rule.get("display_name_contains_lower") or [])]):
        return False
    return True


def _resolve(
    tag_name: str,
    tag_def: dict[str, Any],
    query: str,
    rules_by_tag: dict[str, list[dict[str, Any]]],
) -> tuple[str, str] | None:
    rules = rules_by_tag.get(tag_name)
    if not rules:
        return None
    display = (tag_def or {}).get("display-name") or ""
    for rule in rules:
        if _rule_matches(rule, display, query or ""):
            return rule["table_ref"], rule["field_name"]
    return None


def patch_file(path: Path, rules_by_tag: dict[str, list[dict[str, Any]]]) -> int:
    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)
    n = 0
    dimension_tags = set(rules_by_tag.keys())
    for card in data.get("cards", []):
        dq = card.get("dataset_query") or {}
        native = dq.get("native") or {}
        query = native.get("query") or ""
        tags = native.get("template-tags") or {}
        if not tags:
            continue

        ff = dict(card.get("metabase-field-filters") or {})
        changed = False
        for tname, tdef in tags.items():
            if tname in dimension_tags and (tdef or {}).get("type") != "dimension":
                tdef["type"] = "dimension"
                changed = True
            if (tdef or {}).get("type") != "dimension":
                continue
            if tname in ff:
                continue

            resolved = _resolve(tname, tdef, query, rules_by_tag)
            if not resolved:
                raise RuntimeError(f"{path.name} / {card.get('name')!r}: cannot resolve dimension {tname!r}")
            tr, fn = resolved
            ff[tname] = {"table_ref": tr, "field_name": fn}
            changed = True
            n += 1

        if changed:
            card["metabase-field-filters"] = ff

    if n:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return n


def main() -> None:
    rules_by_tag = _load_rules()
    total = 0
    for f in sorted(_DASHBOARDS_DIR.glob("*.json")):
        total += patch_file(f, rules_by_tag)
    print(f"Patched {total} dimension bindings across metabase_dashboards/*.json")


if __name__ == "__main__":
    main()
