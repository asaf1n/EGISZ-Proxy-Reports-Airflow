#!/usr/bin/env python3
"""Audit Metabase dashboards: card counts and query execution."""
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


def expected_dashboards() -> dict[str, int]:
    out: dict[str, int] = {}
    for path in sorted(DASHBOARDS_DIR.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        out[payload["name"]] = len(payload.get("cards", []))
    return out


def main() -> int:
    report_path = Path(__file__).resolve().parents[1] / "audit_metabase_report.json"
    token = login()
    collections = api("GET", "/api/collection", token)
    col_id = next(c["id"] for c in collections if c.get("name") == COLLECTION_NAME)
    items = api("GET", f"/api/collection/{col_id}/items?models=dashboard&limit=1000", token)
    dashboards = {row["name"]: row["id"] for row in items.get("data", []) if row.get("model") == "dashboard"}

    expected = expected_dashboards()
    failures: list[str] = []
    report: list[dict] = []
    print(f"Collection: {COLLECTION_NAME} (id={col_id})\n")

    for name, exp_cards in expected.items():
        dash_id = dashboards.get(name)
        if dash_id is None:
            failures.append(f"MISSING dashboard: {name}")
            print(f"[MISSING] {name}")
            continue

        dash = api("GET", f"/api/dashboard/{dash_id}", token)
        dashcards = dash.get("dashcards") or dash.get("ordered_cards") or []
        act_cards = len(dashcards)
        status = "OK" if act_cards >= exp_cards else "CARD_COUNT"
        if act_cards < exp_cards:
            failures.append(f"{name}: {act_cards}/{exp_cards} dashcards")

        print(f"[{status}] {name} (id={dash_id}): {act_cards}/{exp_cards} dashcards")

        for dc in dashcards:
            card = dc.get("card") or {}
            card_id = dc.get("card_id") or card.get("id")
            card_name = card.get("name") or f"text#{dc.get('id')}"
            entry = {
                "dashboard": name,
                "dashcard_id": dc.get("id"),
                "card_id": card_id,
                "card_name": card_name,
                "row": dc.get("row"),
                "col": dc.get("col"),
                "size_x": dc.get("size_x"),
                "size_y": dc.get("size_y"),
            }
            if not card_id:
                entry["status"] = "text"
                report.append(entry)
                print(f"  - {card_name}: text card")
                continue
            try:
                result = api("POST", f"/api/card/{card_id}/query", token, {"ignore_cache": True})
                rows = result.get("data", {}).get("rows", [])
                err = result.get("error")
                entry["row_count"] = len(rows)
                if err:
                    entry["status"] = "error"
                    entry["error"] = err
                    failures.append(f"{name} / {card_name}: {err}")
                    print(f"  - FAIL {card_name} (card {card_id}): {err}")
                else:
                    entry["status"] = "ok"
                    print(f"  - OK   {card_name} (card {card_id}): {len(rows)} rows")
            except RuntimeError as exc:
                entry["status"] = "http_error"
                entry["error"] = str(exc)
                failures.append(f"{name} / {card_name}: {exc}")
                print(f"  - FAIL {card_name} (card {card_id}): {exc}")
            report.append(entry)

        print()

    missing_in_mb = set(expected) - set(dashboards)
    extra_in_mb = set(dashboards) - set(expected)
    if missing_in_mb:
        failures.append(f"dashboards in JSON but not in Metabase: {sorted(missing_in_mb)}")
    if extra_in_mb:
        print(f"Extra dashboards in Metabase: {sorted(extra_in_mb)}")

    report_path.write_text(
        json.dumps({"failures": failures, "cards": report}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Report written to {report_path}")

    if failures:
        print("=== FAILURES ===")
        for f in failures:
            print(f)
        return 1

    print("All dashboards and cards passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
