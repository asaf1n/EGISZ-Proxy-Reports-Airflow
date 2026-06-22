from __future__ import annotations

import logging
import time
from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from egisz_elt.common import (
    DWH_CONN_ID,
    PIPELINE,
    PROXY_CONN_ID,
    BatchMetadata,
    PipelineBatchInfo,
    connect_fb,
    connect_pg,
    get_cursors,
    load_raw_logs,
    transform_raw_to_facts,
    update_cursors,
)
from egisz_elt.extract import fetch_exchangelog_after_cursor

log = logging.getLogger(__name__)


def _dwh_connection():
    return connect_pg(BaseHook.get_connection(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(BaseHook.get_connection(PROXY_CONN_ID))


@dag(
    dag_id="egisz_extract_dag",
    schedule=Variable.get("egisz_extract_schedule", default_var="*/5 * * * *"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "extract"],
)
def egisz_extract_pipeline() -> None:
    @task
    def load_exchangelog_batch() -> BatchMetadata:
        batch_size = int(Variable.get("egisz_batch_size", default_var=50000))
        max_rounds = int(Variable.get("egisz_max_load_rounds", default_var=200))

        pg_conn = _dwh_connection()
        try:
            last_logid = int(get_cursors(pg_conn, PIPELINE).get("last_logid", 0))
            cursor_logid = last_logid
            total_loaded = 0
            rounds = 0

            while rounds < max_rounds:
                fb_conn = _proxy_connection()
                try:
                    started_at = time.monotonic()
                    log_rows = fetch_exchangelog_after_cursor(
                        fb_conn,
                        after_logid=cursor_logid,
                        limit=batch_size,
                    )
                    log.info(
                        "Fetched %s EXCHANGELOG row(s) after LOGID=%s in %.2fs (round %s).",
                        len(log_rows),
                        cursor_logid,
                        time.monotonic() - started_at,
                        rounds + 1,
                    )
                finally:
                    fb_conn.close()

                if not log_rows:
                    break

                load_raw_logs(pg_conn, log_rows)
                total_loaded += len(log_rows)
                cursor_logid = max(int(row["logid"]) for row in log_rows)
                rounds += 1

                if len(log_rows) < batch_size:
                    break

            if total_loaded > 0:
                # Refresh planner statistics in the same step that loaded the batch: after a bulk
                # COPY pg_class.reltuples stays 0, the planner picks seq-scans over the functional
                # indexes, and Metabase queries stall for minutes. Autovacuum won't ANALYZE a quiet
                # pipeline for days. A per-batch ANALYZE is cheap (~1s sample scan). ANALYZE cannot
                # run inside a transaction — switch to autocommit. See README.md §«ELT-конвейер».
                pg_conn.set_session(autocommit=True)
                with pg_conn.cursor() as cur:
                    cur.execute("ANALYZE public.exchangelog_raw")
                pg_conn.set_session(autocommit=False)
                log.info(
                    "ANALYZE done for exchangelog_raw after %s row(s) in %s round(s).",
                    total_loaded,
                    rounds,
                )

            log.info(
                "Load complete: %s row(s), next LOGID cursor=%s.",
                total_loaded,
                cursor_logid,
            )
            return {
                "count": total_loaded,
                "last_logid": last_logid,
                "cursor_logid": cursor_logid,
            }
        finally:
            pg_conn.close()

    @task
    def build_document_facts(load_info: BatchMetadata) -> PipelineBatchInfo:
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
    def refresh_materialized_views(load_info: PipelineBatchInfo) -> PipelineBatchInfo:
        if load_info.get("transformed", 0) <= 0:
            log.info("Skipping MV refresh: transform produced 0 rows.")
            return load_info

        # v_egisz_documents_enriched_ui is a persistent table maintained incrementally by
        # egisz_transform_raw_to_facts per touched document_key, so it is no longer fully
        # rebuilt here (that was an O(archive) op every 5-minute cycle). Only the daily rollup
        # on top of it needs refreshing.
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
    def advance_logid_watermark(load_info: BatchMetadata) -> None:
        cursor_logid = int(load_info.get("cursor_logid", 0))
        if cursor_logid <= int(load_info.get("last_logid", 0)):
            return

        pg_conn = _dwh_connection()
        try:
            update_cursors(pg_conn, PIPELINE, logid=cursor_logid)
            log.info("Updated %s watermark to LOGID=%s.", PIPELINE, cursor_logid)
        finally:
            pg_conn.close()

    loaded = load_exchangelog_batch()
    facts = build_document_facts(loaded)
    refreshed = refresh_materialized_views(facts)
    advance_logid_watermark(refreshed)


egisz_extract_pipeline()
