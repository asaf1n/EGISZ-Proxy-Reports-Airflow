from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

from egisz_elt.fb_client import connect_fb, fetch_exchangelog_after_cursor, fetch_organizations
from egisz_elt.pg_client import (
    connect_pg,
    ensure_tables,
    get_cursors,
    load_raw_logs,
    sync_directory,
    transform_raw_to_facts,
    update_cursors,
)

log = logging.getLogger(__name__)

PIPELINE = "egisz"
BATCH_SIZE = 500
DWH_CONN_ID = "dwh_egisz_pg"
PROXY_CONN_ID = "proxy_egisz_fb"


def _dwh_connection():
    return connect_pg(BaseHook.get_connection(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(BaseHook.get_connection(PROXY_CONN_ID))


@dag(
    dag_id="egisz_elt_dag",
    schedule="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
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
            rows = fetch_organizations(fb_conn)
            sync_directory(pg_conn, "dim_organizations", rows)
            log.info("Synced %s organization row(s) into dim_organizations.", len(rows))
        finally:
            fb_conn.close()
            pg_conn.close()

    @task
    def extract_from_proxy() -> dict[str, Any]:
        pg_conn = _dwh_connection()
        try:
            last_log_id, _ = get_cursors(pg_conn, PIPELINE)
        finally:
            pg_conn.close()

        fb_conn = _proxy_connection()
        try:
            rows = fetch_exchangelog_after_cursor(
                fb_conn,
                after_log_id=last_log_id,
                limit=BATCH_SIZE,
            )
        finally:
            fb_conn.close()

        max_id = max((int(row["logid"]) for row in rows), default=last_log_id)
        log.info("Extracted %s EXCHANGELOG row(s), max LOGID=%s.", len(rows), max_id)
        return {"count": len(rows), "max_id": max_id, "rows": rows}

    @task
    def load_to_dwh(extraction_result: dict[str, Any]) -> dict[str, Any]:
        if extraction_result["count"] <= 0:
            return extraction_result

        pg_conn = _dwh_connection()
        try:
            load_raw_logs(pg_conn, extraction_result["rows"])
            log.info("Loaded %s raw row(s) into egisz_raw.", extraction_result["count"])
        finally:
            pg_conn.close()
        return extraction_result

    @task
    def transform_data(load_info: dict[str, Any]) -> dict[str, Any]:
        if load_info["max_id"] <= 0:
            return {**load_info, "transformed": 0}

        pg_conn = _dwh_connection()
        try:
            transformed = transform_raw_to_facts(pg_conn, int(load_info["max_id"]))
            log.info("Transformed %s row(s) into fact_egisz_transactions.", transformed)
        finally:
            pg_conn.close()
        return {**load_info, "transformed": transformed}

    @task
    def update_watermark(load_info: dict[str, Any]) -> None:
        if load_info["max_id"] <= 0:
            return

        pg_conn = _dwh_connection()
        try:
            update_cursors(pg_conn, PIPELINE, log_id=int(load_info["max_id"]))
            log.info("Updated %s watermark to LOGID=%s.", PIPELINE, load_info["max_id"])
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
