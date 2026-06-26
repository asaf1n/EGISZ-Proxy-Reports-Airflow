#!/usr/bin/env python3
"""Run every dashboard card query in Metabase and report failures."""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS_DIR = ROOT / "metabase_dashboards"
DEFAULT_URL = "http://localhost:3000"
DEFAULT_EMAIL = "admin@egisz.local"
DEFAULT_PASSWORD = "egisz"
CLIENT_DASHBOARD_NAMES = frozenset(
    {
        "Клиентский дашборд. Мониторинг сервиса интеграции с ЕГИСЗ",
        "Клиентский дашборд. BI-аналитика ЭМД",
    }
)
SAMPLE_CLIENT_JID_SQL = (
    "SELECT clinic_jid::text FROM public.rpt_documents "
    "WHERE clinic_jid IS NOT NULL LIMIT 1"
)


def api_json(
    url: str,
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    method: str | None = None,
) -> object:
    request = urllib.request.Request(
        url,
        data=data,
        headers=headers or {},
        method=method or ("POST" if data else "GET"),
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def login(base_url: str, email: str, password: str) -> str:
    body = json.dumps({"username": email, "password": password}).encode()
    payload = api_json(
        f"{base_url}/api/session",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    session_id = payload.get("id")
    if not session_id:
        raise RuntimeError(f"cannot login to Metabase as {email}")
    return session_id


def native_sql(card: dict) -> str:
    dq = card.get("dataset_query") or {}
    if dq.get("type") == "native":
        native = dq.get("native") or {}
        return native.get("query") or ""
    stages = dq.get("stages") or []
    if stages and isinstance(stages[0], dict):
        return stages[0].get("native") or ""
    return ""


def template_tags(card: dict) -> dict:
    dq = card.get("dataset_query") or {}
    if dq.get("type") == "native":
        return (dq.get("native") or {}).get("template-tags") or {}
    stages = dq.get("stages") or []
    if stages and isinstance(stages[0], dict):
        return stages[0].get("template-tags") or {}
    return {}


def is_virtual_text_dashcard(dashcard: dict, card: dict) -> bool:
    if card.get("display") == "text":
        return True
    viz = dashcard.get("visualization_settings") or {}
    if viz.get("virtual_card"):
        return True
    if not card.get("id") and not card.get("name"):
        return True
    return False


def resolve_app_database_id(base_url: str, headers: dict[str, str]) -> int | None:
    payload = api_json(f"{base_url}/api/database", headers=headers)
    for item in payload.get("data", []):
        if item.get("engine") == "postgres":
            return int(item["id"])
    return None


def fetch_sample_client_jid(base_url: str, headers: dict[str, str], db_id: int) -> str | None:
    body = json.dumps(
        {
            "database": db_id,
            "type": "native",
            "native": {"query": SAMPLE_CLIENT_JID_SQL},
        }
    ).encode()
    try:
        result = api_json(
            f"{base_url}/api/dataset",
            data=body,
            headers=headers,
            method="POST",
        )
    except urllib.error.HTTPError:
        return None
    rows = (result.get("data") or {}).get("rows") or []
    if not rows or rows[0][0] is None:
        return None
    return str(rows[0][0])


def build_query_parameters(tags: dict, sample_client_jid: str | None) -> list[dict] | None:
    params: list[dict] = []
    for key, tag in tags.items():
        if tag.get("type") != "text" or not tag.get("required"):
            continue
        if key != "client_jid":
            continue
        if sample_client_jid is None:
            return None
        params.append(
            {
                "type": "category",
                "target": ["variable", ["template-tag", key]],
                "value": sample_client_jid,
            }
        )
    return params


def verify_cards(base_url: str = DEFAULT_URL) -> list[str]:
    errors: list[str] = []
    session_id = login(base_url, DEFAULT_EMAIL, DEFAULT_PASSWORD)
    headers = {"X-Metabase-Session": session_id, "Content-Type": "application/json"}

    search = api_json(f"{base_url}/api/search?models=dashboard", headers=headers)
    dashboards = {item["name"]: item["id"] for item in search.get("data", [])}
    app_db_id = resolve_app_database_id(base_url, headers)
    sample_client_jid: str | None = None
    if app_db_id is not None:
        sample_client_jid = fetch_sample_client_jid(base_url, headers, app_db_id)

    for path in sorted(DASHBOARDS_DIR.glob("*.json")):
        spec = json.loads(path.read_text(encoding="utf-8"))
        dash_name = spec.get("name")
        dash_id = dashboards.get(dash_name)
        if dash_id is None:
            errors.append(f"{path.name}: dashboard {dash_name!r} missing")
            continue

        live = api_json(f"{base_url}/api/dashboard/{dash_id}", headers=headers)
        dash_params = {p["id"]: p["slug"] for p in live.get("parameters", [])}

        for dashcard in live.get("dashcards", []):
            card = dashcard.get("card") or {}
            name = card.get("name") or f"dashcard-{dashcard.get('id')}"
            if is_virtual_text_dashcard(dashcard, card):
                continue

            card_id = card.get("id")
            if not card_id:
                errors.append(f"{dash_name} / {name}: no card id")
                continue

            full = api_json(f"{base_url}/api/card/{card_id}", headers=headers)
            dq = full.get("dataset_query") or {}
            sql = native_sql(full)
            tags = template_tags(full)
            is_query_builder = dq.get("type") == "query" or dq.get("lib/type") == "mbql/query"
            if not sql and not is_query_builder:
                errors.append(f"{dash_name} / {name}: missing native SQL")

            unbound = [
                key
                for key, tag in tags.items()
                if tag.get("type") == "dimension" and not tag.get("dimension")
            ]
            if unbound:
                errors.append(f"{dash_name} / {name}: unbound tags {unbound}")

            mappings = dashcard.get("parameter_mappings") or []
            mapped_params = {dash_params.get(m["parameter_id"], m["parameter_id"]) for m in mappings}
            if tags and not mapped_params:
                errors.append(f"{dash_name} / {name}: no parameter_mappings for native card")

            query_params = build_query_parameters(tags, sample_client_jid)
            if query_params is None and dash_name in CLIENT_DASHBOARD_NAMES:
                print(
                    f"SKIP: {dash_name} / {name}: no clinic_jid in DWH for required client_jid",
                    file=sys.stderr,
                )
                continue

            try:
                result = api_json(
                    f"{base_url}/api/card/{card_id}/query",
                    data=json.dumps({"parameters": query_params or []}).encode(),
                    headers=headers,
                    method="POST",
                )
                if result.get("error"):
                    errors.append(f"{dash_name} / {name}: query error: {result['error']}")
            except urllib.error.HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")[:500]
                errors.append(f"{dash_name} / {name}: HTTP {exc.code}: {body}")

            click = (dashcard.get("visualization_settings") or {}).get("click_behavior") or {}
            if name == "Ошибки: тип × клиника":
                if click.get("linkType") != "question":
                    errors.append(
                        f"{dash_name} / {name}: click must drill to model (linkType=question), "
                        f"got {click.get('linkType')!r}"
                    )

    return errors


def main() -> int:
    try:
        errors = verify_cards()
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        print(f"health/login failed: {exc}", file=sys.stderr)
        return 1

    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        print(f"FAILED: {len(errors)} card issue(s)", file=sys.stderr)
        return 1

    print("OK: all dashboard cards queried successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
