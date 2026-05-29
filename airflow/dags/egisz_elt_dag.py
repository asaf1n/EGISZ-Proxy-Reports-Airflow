from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

from egisz_elt.fb_client import (
    connect_fb,
    fetch_exchangelog_after_cursor,
    fetch_licenses,
    fetch_organizations,
)
from egisz_elt.pg_client import (
    connect_pg,
    get_cursors,
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
            cursor_state = get_cursors(pg_conn, PIPELINE)
        finally:
            pg_conn.close()

        last_logid = int(cursor_state.get("last_logid", 0))

        fb_conn = _proxy_connection()
        try:
            started_at = time.monotonic()
            log_rows = fetch_exchangelog_after_cursor(
                fb_conn,
                after_logid=last_logid,
                limit=BATCH_SIZE,
                created_from=SOURCE_MIN_CREATED_AT,
            )
            log.info(
                "Fetched %s EXCHANGELOG row(s) after LOGID=%s in %.2fs.",
                len(log_rows),
                last_logid,
                time.monotonic() - started_at,
            )
            cursor_logid = max((int(row["logid"]) for row in log_rows), default=last_logid)
        finally:
            fb_conn.close()

        log.info(
            "Extracted %s EXCHANGELOG row(s), next LOGID cursor=%s.",
            len(log_rows),
            cursor_logid,
        )
        return {
            "count": len(log_rows),
            "last_logid": last_logid,
            "cursor_logid": cursor_logid,
            "rows": log_rows,
        }

    @task
    def load_to_dwh(extraction_result: dict[str, Any]) -> dict[str, Any]:
        if extraction_result["count"] <= 0:
            return extraction_result

        pg_conn = _dwh_connection()
        try:
            load_raw_logs(pg_conn, extraction_result["rows"])
            log.info(
                "Loaded %s EXCHANGELOG row(s) into exchangelog_raw.",
                extraction_result["count"],
            )
        finally:
            pg_conn.close()
        return extraction_result

    @task
    def analyze_staging(load_info: dict[str, Any]) -> dict[str, Any]:
        """Освежает planner-статистику для raw-таблиц после bulk-загрузки.

        Без этого PostgreSQL planner использует pg_class.reltuples=0 после первичного
        COPY и выбирает seq-scan по exchangelog_raw даже когда функциональные индексы
        (msgid_norm, document_id_norm) уже существуют.
        Autovacuum не запустит ANALYZE, пока не накопится достаточно изменений после
        bulk-загрузки — на спокойном пайплайне это могут быть дни, и к тому моменту
        запросы Metabase уже виснут на 8-16 минут. Свежий ANALYZE на каждом батче
        дешёвый (~1с sample scan) и гарантирует адекватные планы.
        """
        if load_info["count"] <= 0:
            return load_info

        pg_conn = _dwh_connection()
        try:
            # ANALYZE нельзя выполнять внутри транзакции — выходим в autocommit.
            pg_conn.set_session(autocommit=True)
            with pg_conn.cursor() as cur:
                cur.execute("ANALYZE public.exchangelog_raw")
            log.info("ANALYZE done for staging tables touched in this batch.")
        finally:
            pg_conn.close()
        return load_info

    @task
    def transform_data(load_info: dict[str, Any]) -> dict[str, Any]:
        if int(load_info.get("cursor_logid", 0)) <= int(load_info.get("last_logid", 0)):
            return {**load_info, "transformed": 0}

        pg_conn = _dwh_connection()
        try:
            transformed = transform_raw_to_facts(
                pg_conn,
                from_logid=int(load_info.get("last_logid", 0)),
                to_logid=int(load_info["cursor_logid"]),
            )
            if transformed > 0:
                with pg_conn.cursor() as cur:
                    cur.execute("ANALYZE public.fact_egisz_transactions")
                    cur.execute("ANALYZE public.fact_egisz_documents")
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

        # v_egisz_documents_enriched_ui — persistent-таблица, которую инкрементально
        # сопровождает egisz_transform_raw_to_facts по затронутым document_key, поэтому
        # её больше не нужно полностью пересчитывать здесь (это была O(архив) операция на
        # каждом 5-минутном цикле). Остаётся обновить дневной rollup поверх неё.
        pg_conn = _dwh_connection()
        try:
            with pg_conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.v_egisz_documents_daily_ui")
                cur.execute("ANALYZE public.v_egisz_documents_enriched_ui")
                cur.execute("ANALYZE public.v_egisz_documents_daily_ui")
            pg_conn.commit()
            log.info("Maintained document mart (incremental) and refreshed daily rollup.")
        finally:
            pg_conn.close()
        return load_info

    @task
    def update_watermark(load_info: dict[str, Any]) -> None:
        cursor_logid = int(load_info.get("cursor_logid", 0))
        if cursor_logid <= int(load_info.get("last_logid", 0)):
            return

        pg_conn = _dwh_connection()
        try:
            update_cursors(pg_conn, PIPELINE, logid=cursor_logid)
            log.info(
                "Updated %s watermark to LOGID=%s.",
                PIPELINE,
                cursor_logid,
            )
        finally:
            pg_conn.close()

    dimensions = sync_dimensions()
    extraction = extract_cursor_batches()
    loading = load_to_dwh(extraction)
    analyzed = analyze_staging(loading)
    transformed = transform_data(analyzed)
    refreshed = refresh_materialized_views(transformed)
    watermark = update_watermark(refreshed)

    dimensions >> extraction >> loading >> analyzed >> transformed >> refreshed >> watermark


egisz_elt_pipeline()
