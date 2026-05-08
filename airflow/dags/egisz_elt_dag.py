from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

from egisz_elt.fb_client import (
    connect_fb,
    fetch_egisz_messages_after_cursor,
    fetch_egisz_messages_by_identifiers,
    fetch_exchangelog_after_cursor,
    fetch_licenses,
    fetch_organizations,
)
from egisz_elt.pg_client import (
    connect_pg,
    ensure_tables,
    get_cursors,
    load_raw_messages,
    load_raw_logs,
    sync_directory,
    transform_raw_to_facts,
    update_cursors,
)

log = logging.getLogger(__name__)

PIPELINE = "egisz"
BATCH_SIZE = 3500
DWH_CONN_ID = "dwh_egisz_pg"
PROXY_CONN_ID = "proxy_egisz_fb"


def _dwh_connection():
    return connect_pg(BaseHook.get_connection(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(BaseHook.get_connection(PROXY_CONN_ID))


def _xml_text_values(payload: str | None, tag_name: str) -> set[str]:
    if not payload or "<" not in payload:
        return set()
    safe_tag = re.sub(r"[^A-Za-z0-9_:-]", "", tag_name)
    if not safe_tag:
        return set()
    pattern = re.compile(
        rf"<(?:[A-Za-z0-9_]+:)?{safe_tag}(?:\s[^>]*)?>(.*?)</(?:[A-Za-z0-9_]+:)?{safe_tag}>",
        re.IGNORECASE | re.DOTALL,
    )
    return {match.strip() for match in pattern.findall(payload) if match.strip()}


@dag(
    dag_id="egisz_elt_dag",
    schedule="*/5 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh"],
)
def egisz_elt_pipeline() -> None:
    @task
    def bootstrap_dwh() -> None:
        pg_conn = _dwh_connection()
        try:
            ensure_tables(pg_conn)
        finally:
            pg_conn.close()

    @task
    def sync_dimensions() -> None:
        fb_conn = _proxy_connection()
        pg_conn = _dwh_connection()
        try:
            organization_rows = fetch_organizations(fb_conn)
            license_rows = fetch_licenses(fb_conn)
            sync_directory(pg_conn, "dim_organizations", organization_rows)
            sync_directory(pg_conn, "dim_licenses", license_rows)
            log.info(
                "Synced %s organization row(s) and %s license row(s) into DWH dimensions.",
                len(organization_rows),
                len(license_rows),
            )
        finally:
            fb_conn.close()
            pg_conn.close()

    @task
    def extract_from_proxy() -> dict[str, Any]:
        pg_conn = _dwh_connection()
        try:
            last_log_id, last_egmid = get_cursors(pg_conn, PIPELINE)
        finally:
            pg_conn.close()

        fb_conn = _proxy_connection()
        try:
            log_rows = fetch_exchangelog_after_cursor(
                fb_conn,
                after_log_id=last_log_id,
                limit=BATCH_SIZE,
            )
            message_rows = fetch_egisz_messages_after_cursor(
                fb_conn,
                after_egmid=last_egmid,
                limit=BATCH_SIZE,
            )
            related_msgids: set[str] = set()
            related_document_ids: set[str] = set()
            for row in log_rows:
                if row.get("msgid"):
                    related_msgids.add(str(row["msgid"]).strip())
                payload = row.get("msgtext")
                related_msgids.update(_xml_text_values(payload, "messageId"))
                related_msgids.update(_xml_text_values(payload, "relatesToMessage"))
                related_msgids.update(_xml_text_values(payload, "relatesTo"))
                related_document_ids.update(_xml_text_values(payload, "localUid"))
                related_document_ids.update(_xml_text_values(payload, "DOCUMENTID"))
            related_message_rows = fetch_egisz_messages_by_identifiers(
                fb_conn,
                msgids=related_msgids,
                document_ids=related_document_ids,
            )
        finally:
            fb_conn.close()

        cursor_max_egmid = max((int(row["egmid"]) for row in message_rows), default=last_egmid)
        messages_by_egmid = {int(row["egmid"]): row for row in message_rows}
        for row in related_message_rows:
            messages_by_egmid[int(row["egmid"])] = row
        message_rows = list(messages_by_egmid.values())
        max_id = max((int(row["logid"]) for row in log_rows), default=last_log_id)
        log.info(
            "Extracted %s EXCHANGELOG row(s), max LOGID=%s; %s EGISZ_MESSAGES row(s), cursor max EGMID=%s.",
            len(log_rows),
            max_id,
            len(message_rows),
            cursor_max_egmid,
        )
        return {
            "count": len(log_rows),
            "message_count": len(message_rows),
            "last_log_id": last_log_id,
            "last_egmid": last_egmid,
            "max_id": max_id,
            "max_egmid": cursor_max_egmid,
            "rows": log_rows,
            "message_rows": message_rows,
        }

    @task
    def load_to_dwh(extraction_result: dict[str, Any]) -> dict[str, Any]:
        if extraction_result["count"] <= 0 and extraction_result.get("message_count", 0) <= 0:
            return extraction_result

        pg_conn = _dwh_connection()
        try:
            if extraction_result["count"] > 0:
                load_raw_logs(pg_conn, extraction_result["rows"])
            if extraction_result.get("message_count", 0) > 0:
                load_raw_messages(pg_conn, extraction_result["message_rows"])
            log.info(
                "Loaded %s EXCHANGELOG row(s) into exchangelog_raw and %s EGISZ_MESSAGES row(s) into egisz_messages_raw.",
                extraction_result["count"],
                extraction_result.get("message_count", 0),
            )
        finally:
            pg_conn.close()
        return extraction_result

    @task
    def transform_data(load_info: dict[str, Any]) -> dict[str, Any]:
        if load_info["max_id"] <= 0 and load_info.get("message_count", 0) <= 0:
            return {**load_info, "transformed": 0}

        pg_conn = _dwh_connection()
        try:
            transformed = transform_raw_to_facts(
                pg_conn,
                min_log_id=int(load_info.get("last_log_id", 0)),
                max_log_id=int(load_info["max_id"]),
                min_egmid=int(load_info.get("last_egmid", 0)),
                max_egmid=int(load_info.get("max_egmid", 0)),
            )
            log.info("Transformed %s row(s) into fact_egisz_transactions.", transformed)
        finally:
            pg_conn.close()
        return {**load_info, "transformed": transformed}

    @task
    def update_watermark(load_info: dict[str, Any]) -> None:
        if load_info["max_id"] <= 0 and load_info.get("max_egmid", 0) <= 0:
            return

        pg_conn = _dwh_connection()
        try:
            update_cursors(pg_conn, PIPELINE, log_id=int(load_info["max_id"]), egmid=int(load_info.get("max_egmid", 0)))
            log.info(
                "Updated %s watermark to LOGID=%s, EGMID=%s.",
                PIPELINE,
                load_info["max_id"],
                load_info.get("max_egmid", 0),
            )
        finally:
            pg_conn.close()

    initialized = bootstrap_dwh()
    dimensions = sync_dimensions()
    extraction = extract_from_proxy()
    loading = load_to_dwh(extraction)
    transformed = transform_data(loading)
    watermark = update_watermark(transformed)

    initialized >> dimensions >> extraction >> loading >> transformed >> watermark


egisz_elt_pipeline()
