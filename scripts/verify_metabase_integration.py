#!/usr/bin/env python3
"""Verify Metabase dashboards match repo JSON contracts."""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

from mb_api import DEFAULT_EMAIL, DEFAULT_PASSWORD, DEFAULT_URL, api_json, login

ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS_DIR = ROOT / "metabase_dashboards"
INTEGRATION_JSON = DASHBOARDS_DIR / "01_integration_egisz.json"


def verify_dashboard_contracts(
    base_url: str = DEFAULT_URL,
    email: str = DEFAULT_EMAIL,
    password: str = DEFAULT_PASSWORD,
) -> list[str]:
    errors: list[str] = []

    try:
        urllib.request.urlopen(f"{base_url}/api/health", timeout=5)
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return [f"health check failed: {exc}"]

    session_id = login(base_url, email, password)
    headers = {"X-Metabase-Session": session_id}
    search = api_json(f"{base_url}/api/search?models=dashboard", headers=headers)
    live_by_name = {item.get("name"): item for item in search.get("data", [])}

    for path in sorted(DASHBOARDS_DIR.glob("*.json")):
        spec = json.loads(path.read_text(encoding="utf-8"))
        name = spec.get("name")
        expected_cards = len(spec.get("cards") or [])
        if not name:
            errors.append(f"{path.name}: dashboard JSON has no name")
            continue
        item = live_by_name.get(name)
        if item is None:
            errors.append(f"{path.name}: dashboard {name!r} is missing in Metabase")
            continue
        live = api_json(f"{base_url}/api/dashboard/{item['id']}", headers=headers)
        actual_cards = len(live.get("dashcards") or live.get("ordered_cards") or [])
        if actual_cards != expected_cards:
            errors.append(
                f"{path.name}: dashboard {name!r} has {actual_cards}/{expected_cards} dashcards"
            )

    integration = json.loads(INTEGRATION_JSON.read_text(encoding="utf-8"))
    integration_name = integration["name"]
    item = live_by_name.get(integration_name)
    if item is None:
        errors.append(f"integration dashboard {integration_name!r} is missing in Metabase")
        return errors

    live = api_json(f"{base_url}/api/dashboard/{item['id']}", headers=headers)
    expected_period = next(
        p["default"] for p in integration["parameters"] if p.get("slug") == "ips_date_filter"
    )
    live_period = next(
        p["default"] for p in live.get("parameters", []) if p.get("slug") == "ips_date_filter"
    )
    if live_period != expected_period:
        errors.append(
            f"period default: live={live_period!r} expected={expected_period!r}"
        )

    expected_tabs = len(integration.get("tabs") or [])
    live_tabs = live.get("tabs") or []
    if len(live_tabs) != expected_tabs:
        errors.append(
            f"integration tabs: live={len(live_tabs)} expected={expected_tabs}"
        )

    operational_tab_name = next(
        tab["name"] for tab in integration["tabs"] if tab.get("id") == "operational"
    )
    expected_op = sorted(
        card["name"]
        for card in integration["cards"]
        if card.get("tab") == "operational" and card.get("display") != "text"
    )
    tab_by_id = {tab["id"]: tab["name"] for tab in live_tabs}
    live_op = sorted(
        dashcard["card"]["name"]
        for dashcard in live.get("dashcards", [])
        if tab_by_id.get(dashcard.get("dashboard_tab_id")) == operational_tab_name
        and dashcard.get("card")
        and dashcard["card"].get("name")
    )
    if live_op != expected_op:
        errors.append(f"operational cards: live={live_op!r} expected={expected_op!r}")

    return errors


def main() -> int:
    errors = verify_dashboard_contracts()
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        return 1
    integration = json.loads(INTEGRATION_JSON.read_text(encoding="utf-8"))
    operational_count = sum(
        1
        for card in integration["cards"]
        if card.get("tab") == "operational" and card.get("display") != "text"
    )
    print(
        "OK: Metabase dashboards verified "
        f"(integration operational={operational_count} cards)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
