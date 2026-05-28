from __future__ import annotations

import logging
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
    get_related_message_identifiers,
    load_messages,
    load_raw_logs,
    sync_directory,
    transform_raw_to_facts,
    update_cursors,
)

log = logging.getLogger(__name__)

PIPELINE = "egisz"
BATCH_SIZE = 3000
DWH_CONN_ID = "dwh_egisz_pg"
PROXY_CONN_ID = "proxy_egisz_fb"
SOURCE_MIN_CREATED_AT = datetime(2026, 5, 18)


def _dwh_connection():
    return connect_pg(BaseHook.get_connection(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(BaseHook.get_connection(PROXY_CONN_ID))


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
    def extract_cursor_batches() -> dict[str, Any]:
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
                after_logid=last_log_id,
                limit=BATCH_SIZE,
                created_from=SOURCE_MIN_CREATED_AT,
            )
            log.info(
                "Fetched %s EXCHANGELOG row(s) after LOGID=%s in %.2fs.",
                len(log_rows),
                last_log_id,
                time.monotonic() - started_at,
            )
            cursor_logid = max((int(row["logid"]) for row in log_rows), default=last_log_id)
            started_at = time.monotonic()
            cursor_message_rows = fetch_egisz_messages_after_cursor(
                fb_conn,
                after_egmid=last_egmid,
                limit=BATCH_SIZE,
                created_from=SOURCE_MIN_CREATED_AT,
            )
            log.info(
                "Fetched %s EGISZ_MESSAGES row(s) after EGMID=%s in %.2fs.",
                len(cursor_message_rows),
                last_egmid,
                time.monotonic() - started_at,
            )
        finally:
            fb_conn.close()

        cursor_egmid = max((int(row["egmid"]) for row in cursor_message_rows), default=last_egmid)
        messages_by_egmid = {int(row["egmid"]): row for row in cursor_message_rows}
        message_rows = list(messages_by_egmid.values())
        log.info(
            "Extracted %s EXCHANGELOG row(s), next LOGID cursor=%s; %s EGISZ_MESSAGES row(s), next EGMID cursor=%s.",
            len(log_rows),
            cursor_logid,
            len(message_rows),
            cursor_egmid,
        )
        return {
            "count": len(log_rows),
            "message_count": len(message_rows),
            "cursor_message_count": len(cursor_message_rows),
            "last_log_id": last_log_id,
            "last_egmid": last_egmid,
            "cursor_logid": cursor_logid,
            "cursor_egmid": cursor_egmid,
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
                load_messages(pg_conn, extraction_result["message_rows"])
            log.info(
                "Loaded %s EXCHANGELOG row(s) into exchangelog_raw and %s EGISZ_MESSAGES row(s) into stg_egisz_messages.",
                extraction_result["count"],
                extraction_result.get("message_count", 0),
            )
        finally:
            pg_conn.close()
        return extraction_result

    @task
    def analyze_staging(load_info: dict[str, Any]) -> dict[str, Any]:
        """Освежает planner-статистику для raw-таблиц после bulk-загрузки.

        Без этого PostgreSQL planner использует pg_class.reltuples=0 после первичного
        COPY и выбирает seq-scan по exchangelog_raw / stg_egisz_messages даже когда
        функциональные индексы (msgid_norm, document_id_norm) уже существуют.
        Autovacuum не запустит ANALYZE, пока не накопится достаточно изменений после
        bulk-загрузки — на спокойном пайплайне это могут быть дни, и к тому моменту
        запросы Metabase уже виснут на 8-16 минут. Свежий ANALYZE на каждом батче
        дешёвый (~1с sample scan) и гарантирует адекватные планы.
        """
        if load_info["count"] <= 0 and load_info.get("message_count", 0) <= 0:
            return load_info

        pg_conn = _dwh_connection()
        try:
            # ANALYZE нельзя выполнять внутри транзакции — выходим в autocommit.
            pg_conn.set_session(autocommit=True)
            with pg_conn.cursor() as cur:
                if load_info["count"] > 0:
                    cur.execute("ANALYZE public.exchangelog_raw")
                if load_info.get("message_count", 0) > 0:
                    cur.execute("ANALYZE public.stg_egisz_messages")
            log.info("ANALYZE done for staging/fact tables touched in this batch.")
        finally:
            pg_conn.close()
        return load_info

    @task
    def resolve_related_refs_from_dwh(load_info: dict[str, Any]) -> dict[str, Any]:
        if int(load_info.get("cursor_logid", 0)) <= int(load_info.get("last_log_id", 0)):
            return {**load_info, "related_msgids": [], "related_document_ids": []}

        pg_conn = _dwh_connection()
        try:
            related = get_related_message_identifiers(
                pg_conn,
                from_logid=int(load_info.get("last_log_id", 0)),
                to_logid=int(load_info.get("cursor_logid", 0)),
            )
        finally:
            pg_conn.close()

        related_msgids = sorted(related["msgids"])
        related_document_ids = sorted(related["document_ids"])
        log.info(
            "Resolved %s related MSGID(s) and %s DOCUMENTID(s) from DWH staging.",
            len(related_msgids),
            len(related_document_ids),
        )
        return {
            **load_info,
            "related_msgids": related_msgids,
            "related_document_ids": related_document_ids,
        }

    @task
    def load_related_messages(load_info: dict[str, Any]) -> dict[str, Any]:
        related_msgids = set(load_info.get("related_msgids", []))
        related_document_ids = set(load_info.get("related_document_ids", []))
        if not related_msgids and not related_document_ids:
            return {**load_info, "related_message_count": 0}

        fb_conn = _proxy_connection()
        try:
            started_at = time.monotonic()
            related_message_rows = fetch_egisz_messages_by_identifiers(
                fb_conn,
                msgids=related_msgids,
                document_ids=related_document_ids,
                created_from=SOURCE_MIN_CREATED_AT,
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

        if related_message_rows:
            pg_conn = _dwh_connection()
            try:
                load_messages(pg_conn, related_message_rows)
                pg_conn.set_session(autocommit=True)
                with pg_conn.cursor() as cur:
                    cur.execute("ANALYZE public.stg_egisz_messages")
            finally:
                pg_conn.close()

        return {
            **load_info,
            "message_count": int(load_info.get("message_count", 0)) + len(related_message_rows),
            "related_message_count": len(related_message_rows),
        }

    @task
    def transform_data(load_info: dict[str, Any]) -> dict[str, Any]:
        if (
            int(load_info.get("cursor_logid", 0)) <= int(load_info.get("last_log_id", 0))
            and int(load_info.get("cursor_egmid", 0)) <= int(load_info.get("last_egmid", 0))
        ):
            return {**load_info, "transformed": 0}

        pg_conn = _dwh_connection()
        try:
            transformed = transform_raw_to_facts(
                pg_conn,
                from_logid=int(load_info.get("last_log_id", 0)),
                to_logid=int(load_info["cursor_logid"]),
                from_egmid=int(load_info.get("last_egmid", 0)),
                to_egmid=int(load_info.get("cursor_egmid", 0)),
            )
            if transformed > 0:
                with pg_conn.cursor() as cur:
                    cur.execute("ANALYZE public.fact_egisz_transactions")
                    cur.execute("ANALYZE public.fact_egisz_documents")
                    cur.execute("ANALYZE public.fact_egisz_channel_errors")
                pg_conn.commit()
            log.info("Transformed %s row(s) into document facts and callback lineage.", transformed)
        finally:
            pg_conn.close()
        return {**load_info, "transformed": transformed}

    @task
    def refresh_materialized_views(load_info: dict[str, Any]) -> dict[str, Any]:
        if load_info.get("transformed", 0) <= 0:
            log.info("Skipping MV refresh: transform produced 0 rows.")
            return load_info

        pg_conn = _dwh_connection()
        try:
            with pg_conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.v_egisz_documents_enriched_ui")
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.v_egisz_documents_daily_ui")
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.v_stg_channel_errors_by_document")
                cur.execute("ANALYZE public.v_egisz_documents_enriched_ui")
                cur.execute("ANALYZE public.v_egisz_documents_daily_ui")
                cur.execute("ANALYZE public.v_stg_channel_errors_by_document")
            pg_conn.commit()
            log.info("Refreshed document and channel materialized views.")
        finally:
            pg_conn.close()
        return load_info

    @task
    def update_watermark(load_info: dict[str, Any]) -> None:
        cursor_logid = int(load_info.get("cursor_logid", 0))
        cursor_egmid = int(load_info.get("cursor_egmid", 0))
        if cursor_logid <= int(load_info.get("last_log_id", 0)) and cursor_egmid <= int(load_info.get("last_egmid", 0)):
            return

        pg_conn = _dwh_connection()
        try:
            update_cursors(pg_conn, PIPELINE, logid=cursor_logid, egmid=cursor_egmid)
            log.info(
                "Updated %s watermark to LOGID=%s, EGMID=%s.",
                PIPELINE,
                cursor_logid,
                cursor_egmid,
            )
        finally:
            pg_conn.close()

    dimensions = sync_dimensions()
    extraction = extract_cursor_batches()
    loading = load_to_dwh(extraction)
    analyzed = analyze_staging(loading)
    related_refs = resolve_related_refs_from_dwh(analyzed)
    related_loaded = load_related_messages(related_refs)
    transformed = transform_data(related_loaded)
    refreshed = refresh_materialized_views(transformed)
    watermark = update_watermark(refreshed)

    dimensions >> extraction >> loading >> analyzed >> related_refs >> related_loaded >> transformed >> refreshed >> watermark


egisz_elt_pipeline()
