#!/usr/bin/env python3
"""Run every dashboard card query in Metabase and report failures."""

from __future__ import annotations

import functools
import json
import os
import sys
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import mb_api
from mb_api import DEFAULT_EMAIL, DEFAULT_PASSWORD, DEFAULT_URL, login

ROOT = Path(__file__).resolve().parents[1]
DASHBOARDS_DIR = ROOT / "metabase_dashboards"
QUERY_TIMEOUT_SECONDS = 45
DEFAULT_WORKERS = 8
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
DOCUMENTS_MODEL_REF = "Документы"
ERROR_BREAKDOWN_MODEL_REF = "Разбивка ошибок"
ERROR_TYPE_CLINIC_CARD = "Ошибки: тип × клиника"
ERROR_TYPE_CLINIC_DRILL_COLUMNS = frozenset({"Тип ошибки", "JID Клиники"})
ERROR_TYPE_CLINIC_DASHBOARD_PARAMS = frozenset(
    {"ips_date_filter", "semd_type_filter"}
)
TOP_ERROR_TYPE_CARD = "Топ по типу ошибки"


# Запросы карточек длиннее логина/метаданных — общий хелпер с локальным таймаутом.
api_json = functools.partial(mb_api.api_json, timeout=QUERY_TIMEOUT_SECONDS)


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
    stages = dq.get("stages") or []
    if stages:
        return stages[0].get("template-tags") or {}
    if dq.get("type") == "native":
        return (dq.get("native") or {}).get("template-tags") or {}
    return {}


def is_query_builder_card(card: dict) -> bool:
    dq = card.get("dataset_query") or {}
    if dq.get("type") == "query":
        return True
    stages = dq.get("stages") or []
    return bool(stages and stages[0].get("lib/type") == "mbql.stage/mbql")


def mapping_target_issues(
    dash_name: str,
    name: str,
    card: dict,
    tags: dict,
    mappings: list[dict],
) -> list[str]:
    issues: list[str] = []
    is_qb = is_query_builder_card(card)
    for mapping in mappings:
        target = mapping.get("target") or []
        if len(target) < 2 or target[0] != "dimension":
            continue
        inner = target[1]
        if not isinstance(inner, list) or not inner:
            continue
        kind = inner[0]
        if is_qb:
            if kind != "field":
                issues.append(f"{dash_name} / {name}: QB card mapping must target field, got {kind!r}")
            elif len(inner) < 3 or not isinstance(inner[2], dict) or inner[2].get("stage-number") is None:
                issues.append(f"{dash_name} / {name}: QB field mapping missing stage-number")
            continue
        if kind == "template-tag":
            continue
        if kind == "field":
            if not is_qb:
                issues.append(
                    f"{dash_name} / {name}: native card must map via template-tag, not field"
                )
            elif len(inner) < 3 or not isinstance(inner[2], dict) or inner[2].get("stage-number") is None:
                issues.append(f"{dash_name} / {name}: QB field mapping missing stage-number")
            continue
        issues.append(f"{dash_name} / {name}: unsupported mapping target {kind!r}")
    return issues


def model_drill_source_columns(click: dict) -> set[str]:
    mapping = click.get("parameterMapping") or {}
    return {
        (spec.get("source") or {}).get("name")
        for spec in mapping.values()
        if (spec.get("source") or {}).get("type") == "column"
    } - {None}


def model_drill_dashboard_param_slugs(click: dict, dash_params: dict[str, dict]) -> set[str]:
    mapping = click.get("parameterMapping") or {}
    slugs: set[str] = set()
    for spec in mapping.values():
        source = spec.get("source") or {}
        if source.get("type") != "parameter":
            continue
        param = dash_params.get(source.get("id") or "")
        if param and param.get("slug"):
            slugs.add(param["slug"])
    return slugs


def model_drill_contains_error_types(click: dict) -> bool:
    """«Тип ошибки» must map to the Документы model's error_types list with operator
    'contains' — a document with several errors is matched by element containment and
    not missed."""
    mapping = click.get("parameterMapping") or {}
    for spec in mapping.values():
        source = spec.get("source") or {}
        if source.get("type") != "column" or source.get("name") != "Тип ошибки":
            continue
        target = spec.get("target") or {}
        return target.get("operator") == "contains"
    return False


def error_type_clinic_model_drill_issues(
    click: dict,
    dash_params: dict[str, dict] | None = None,
) -> list[str]:
    issues: list[str] = []
    if click.get("type") != "link":
        issues.append("click_behavior.type must be 'link'")
        return issues
    if click.get("linkType") != "question":
        issues.append(
            "click must drill to model "
            f"(linkType=question), got linkType={click.get('linkType')!r}"
        )
        return issues
    if click.get("targetModel") not in (None, DOCUMENTS_MODEL_REF) and not click.get("targetId"):
        issues.append(
            "click must target model "
            f"{DOCUMENTS_MODEL_REF!r}, got targetModel={click.get('targetModel')!r}"
        )
    if click.get("tab") == "archive" or click.get("tabId") is not None:
        issues.append("click must not drill to archive tab")
    missing = ERROR_TYPE_CLINIC_DRILL_COLUMNS - model_drill_source_columns(click)
    if missing:
        issues.append(f"click missing column mappings: {sorted(missing)}")
    if not model_drill_contains_error_types(click):
        issues.append("click must map «Тип ошибки» with operator=contains on error_types (Документы model)")
    if dash_params is not None:
        missing_params = ERROR_TYPE_CLINIC_DASHBOARD_PARAMS - model_drill_dashboard_param_slugs(
            click, dash_params
        )
        if missing_params:
            issues.append(f"click missing dashboard params: {sorted(missing_params)}")
        if "JID Клиники" not in model_drill_source_columns(click):
            if "jid_filter" not in model_drill_dashboard_param_slugs(click, dash_params):
                issues.append("click must map «JID Клиники» column or jid_filter dashboard param")
    return issues


def top_error_type_table_issues(name: str, card: dict) -> list[str]:
    if card.get("display") != "table":
        return [f"{name}: display must be table"]
    viz = card.get("visualization_settings") or {}
    issues: list[str] = []
    columns = {col.get("name") for col in viz.get("table.columns") or []}
    for required in ("Категория ошибки", "Тип ошибки", "Документов", "%"):
        if required not in columns:
            issues.append(f"{name}: table.columns must include «{required}»")
    query = native_sql(card)
    if "error_category" not in query or '"Тип ошибки"' not in query:
        issues.append(f"{name}: SQL must expose category and atomic error type")
    if 'AS "%"' not in query:
        issues.append(f"{name}: SQL must expose «%» share column")
    return issues


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


@dataclass(frozen=True)
class CardQueryJob:
    dash_name: str
    name: str
    card_id: int
    query_params: list[dict] | None
    skip_query: bool


def verify_card_query(
    base_url: str,
    headers: dict[str, str],
    job: CardQueryJob,
) -> list[str]:
    if job.skip_query:
        return []
    try:
        result = api_json(
            f"{base_url}/api/card/{job.card_id}/query",
            data=json.dumps({"parameters": job.query_params or []}).encode(),
            headers=headers,
            method="POST",
        )
        if result.get("error"):
            return [f"{job.dash_name} / {job.name}: query error: {result['error']}"]
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:500]
        return [f"{job.dash_name} / {job.name}: HTTP {exc.code}: {body}"]
    return []


def verify_cards(base_url: str = DEFAULT_URL, workers: int = DEFAULT_WORKERS) -> list[str]:
    errors: list[str] = []
    session_id = login(base_url, DEFAULT_EMAIL, DEFAULT_PASSWORD)
    headers = {"X-Metabase-Session": session_id, "Content-Type": "application/json"}

    search = api_json(f"{base_url}/api/search?models=dashboard", headers=headers)
    dashboards = {item["name"]: item["id"] for item in search.get("data", [])}
    app_db_id = resolve_app_database_id(base_url, headers)
    sample_client_jid: str | None = None
    if app_db_id is not None:
        sample_client_jid = fetch_sample_client_jid(base_url, headers, app_db_id)

    query_jobs: list[CardQueryJob] = []

    for path in sorted(DASHBOARDS_DIR.glob("*.json")):
        spec = json.loads(path.read_text(encoding="utf-8"))
        dash_name = spec.get("name")
        dash_id = dashboards.get(dash_name)
        if dash_id is None:
            errors.append(f"{path.name}: dashboard {dash_name!r} missing")
            continue

        live = api_json(f"{base_url}/api/dashboard/{dash_id}", headers=headers)
        dash_params = {p["id"]: p for p in live.get("parameters", [])}
        dash_param_slugs = {p["id"]: p["slug"] for p in live.get("parameters", [])}

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
            mapped_params = {dash_param_slugs.get(m["parameter_id"], m["parameter_id"]) for m in mappings}
            if tags and not mapped_params:
                errors.append(f"{dash_name} / {name}: no parameter_mappings for native card")
            errors.extend(mapping_target_issues(dash_name, name, full, tags, mappings))

            query_params = build_query_parameters(tags, sample_client_jid)
            skip_query = False
            if query_params is None and dash_name in CLIENT_DASHBOARD_NAMES:
                print(
                    f"SKIP: {dash_name} / {name}: no clinic_jid in DWH for required client_jid",
                    file=sys.stderr,
                )
                skip_query = True

            query_jobs.append(
                CardQueryJob(
                    dash_name=dash_name,
                    name=name,
                    card_id=int(card_id),
                    query_params=query_params,
                    skip_query=skip_query,
                )
            )

            click = (dashcard.get("visualization_settings") or {}).get("click_behavior") or {}
            if name == ERROR_TYPE_CLINIC_CARD:
                for issue in error_type_clinic_model_drill_issues(click, dash_params):
                    errors.append(f"{dash_name} / {name}: {issue}")
            if name == TOP_ERROR_TYPE_CARD:
                for issue in top_error_type_table_issues(name, full):
                    errors.append(f"{dash_name} / {name}: {issue}")

    runnable = [job for job in query_jobs if not job.skip_query]
    if runnable:
        print(
            f"Querying {len(runnable)} dashboard card(s) with {workers} worker(s)...",
            file=sys.stderr,
        )
        with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
            futures = {
                pool.submit(verify_card_query, base_url, headers, job): job
                for job in runnable
            }
            done = 0
            for future in as_completed(futures):
                done += 1
                job = futures[future]
                errors.extend(future.result())
                if done % 10 == 0 or done == len(runnable):
                    print(f"  {done}/{len(runnable)} cards queried", file=sys.stderr)

    return errors


def main() -> int:
    workers = int(os.environ.get("METABASE_VERIFY_WORKERS", DEFAULT_WORKERS))
    try:
        errors = verify_cards(workers=workers)
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
