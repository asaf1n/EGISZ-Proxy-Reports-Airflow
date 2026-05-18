from __future__ import annotations

import logging
import re
import time
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
    get_cursors,
    load_raw_messages,
    load_raw_logs,
    sync_directory,
    transform_raw_to_facts,
    update_cursors,
)

log = logging.getLogger(__name__)

PIPELINE = "egisz"
BATCH_SIZE = 5000
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
    def sync_dimensions() -> None:
        log.info("Starting dimension sync from proxy_egisz into dwh_egisz.")
        fb_conn = _proxy_connection()
        pg_conn = _dwh_connection()
        try:
            log.info("Fetching organizations directory from JPERSONS.")
            organization_rows = fetch_organizations(fb_conn)
            log.info("Fetched %s organization row(s); syncing dim_organizations.", len(organization_rows))
            sync_directory(pg_conn, "dim_organizations", organization_rows)

            log.info("Fetching licenses directory from EGISZ_LICENSES.")
            license_rows = fetch_licenses(fb_conn)
            log.info("Fetched %s license row(s); syncing dim_licenses.", len(license_rows))
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
            started_at = time.monotonic()
            log_rows = fetch_exchangelog_after_cursor(
                fb_conn,
                after_log_id=last_log_id,
                limit=BATCH_SIZE,
            )
            log.info(
                "Fetched %s EXCHANGELOG row(s) after LOGID=%s in %.2fs.",
                len(log_rows),
                last_log_id,
                time.monotonic() - started_at,
            )
            started_at = time.monotonic()
            message_rows = fetch_egisz_messages_after_cursor(
                fb_conn,
                after_egmid=last_egmid,
                limit=BATCH_SIZE,
            )
            log.info(
                "Fetched %s EGISZ_MESSAGES row(s) after EGMID=%s in %.2fs.",
                len(message_rows),
                last_egmid,
                time.monotonic() - started_at,
            )
            started_at = time.monotonic()
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
            log.info(
                "Fetched %s related EGISZ_MESSAGES row(s) for %s MSGID(s) and %s DOCUMENTID(s) in %.2fs.",
                len(related_message_rows),
                len(related_msgids),
                len(related_document_ids),
                time.monotonic() - started_at,
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
            return {**load_info, "transformed": 0, "duration_ms": 0, "errors_count": 0}

        pg_conn = _dwh_connection()
        started_at = time.monotonic()
        errors_count = 0
        try:
            transformed = transform_raw_to_facts(
                pg_conn,
                min_log_id=int(load_info.get("last_log_id", 0)),
                max_log_id=int(load_info["max_id"]),
                min_egmid=int(load_info.get("last_egmid", 0)),
                max_egmid=int(load_info.get("max_egmid", 0)),
            )
            log.info("Transformed %s row(s) into fact_egisz_transactions.", transformed)
            # Count error rows in the current batch range to feed etl_run_log.
            with pg_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)::int
                    FROM public.fact_egisz_transactions
                    WHERE exchangelog_log_id BETWEEN %s AND %s
                      AND status = 'error'
                    """,
                    (int(load_info.get("last_log_id", 0)), int(load_info["max_id"])),
                )
                row = cur.fetchone()
                errors_count = int(row[0]) if row and row[0] is not None else 0
        finally:
            pg_conn.close()
        duration_ms = int((time.monotonic() - started_at) * 1000)
        return {
            **load_info,
            "transformed": transformed,
            "duration_ms": duration_ms,
            "errors_count": errors_count,
        }

    @task
    def refresh_materialized_views(load_info: dict[str, Any]) -> dict[str, Any]:
        if load_info["max_id"] <= 0 and load_info.get("max_egmid", 0) <= 0:
            return load_info

        pg_conn = _dwh_connection()
        try:
            with pg_conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.v_egisz_transactions_enriched_ui")
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.v_stg_channel_errors_by_document")
            pg_conn.commit()
            log.info("Refreshed materialized views v_egisz_transactions_enriched_ui and v_stg_channel_errors_by_document.")
        finally:
            pg_conn.close()
        return load_info

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
            # Записываем строку в etl_run_log для часовой динамики пайплайна
            # (используется v_service_health_ui и потенциальными графиками).
            with pg_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.etl_run_log
                        (run_ts, docs_processed, errors_count, duration_ms,
                         batch_min_id, batch_max_id, batch_min_egmid, batch_max_egmid)
                    VALUES (now(), %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_ts) DO NOTHING
                    """,
                    (
                        int(load_info.get("transformed", 0) or 0),
                        int(load_info.get("errors_count", 0) or 0),
                        int(load_info.get("duration_ms", 0) or 0),
                        int(load_info.get("last_log_id", 0) or 0),
                        int(load_info.get("max_id", 0) or 0),
                        int(load_info.get("last_egmid", 0) or 0),
                        int(load_info.get("max_egmid", 0) or 0),
                    ),
                )
            pg_conn.commit()
        finally:
            pg_conn.close()

    dimensions = sync_dimensions()
    extraction = extract_from_proxy()
    loading = load_to_dwh(extraction)
    transformed = transform_data(loading)
    refreshed = refresh_materialized_views(transformed)
    watermark = update_watermark(refreshed)

    dimensions >> extraction >> loading >> transformed >> refreshed >> watermark


egisz_elt_pipeline()
