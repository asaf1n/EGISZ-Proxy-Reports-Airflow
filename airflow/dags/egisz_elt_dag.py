from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

from egisz_elt.fb_client import connect_fb
from egisz_elt.pg_client import (
    connect_pg,
    ensure_tables,
    list_missing_dwh_objects,
    load_raw_logs,
    sync_directory,
    transform_raw_to_facts,
    update_cursors,
)

log = logging.getLogger(__name__)
PIPELINE = "main"
BATCH_SIZE = 1000


def _to_xcom_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _to_xcom_row(row: tuple[Any, ...]) -> list[Any]:
    return [_to_xcom_value(value) for value in row]


@dag(
    dag_id="egisz_elt_dag",
    schedule=os.getenv("EGISZ_ELT_SCHEDULE", "@hourly"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def egisz_elt():
    @task
    def inspect_and_init_dwh() -> dict[str, Any]:
        pg_conn = connect_pg(BaseHook.get_connection("dwh_egisz_pg"))
        try:
            metadata_issues = sorted(list_missing_dwh_objects(pg_conn))
            if metadata_issues:
                log.info("Initializing DWH objects after metadata inspection: %s", ", ".join(metadata_issues))
                ensure_tables(pg_conn)
            else:
                log.info("DWH metadata is up to date")
            return {"initialized": bool(metadata_issues), "metadata_issues_before": metadata_issues}
        finally:
            pg_conn.close()

    @task(task_id="sync_dimensions")
    def sync_dimensions() -> None:
        pg_conn = connect_pg(BaseHook.get_connection("dwh_egisz_pg"))
        fb_conn = connect_fb(BaseHook.get_connection("proxy_egisz_fb"))
        try:
            with fb_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT JID, JNAME, JINN, COALESCE(NULLIF(FACTADDR, ''), JADDR)
                    FROM JPERSONS
                    """
                )
                sync_directory(pg_conn, "dim_organizations", cur.fetchall())
            with fb_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ID, SERVICE_TYPE, JID, MO_UID, MO_DOMEN, BDATE, FDATE, KIND, MODIFYDATE
                    FROM EGISZ_LICENSES
                    """
                )
                sync_directory(pg_conn, "dim_licenses", cur.fetchall())
        finally:
            fb_conn.close()
            pg_conn.close()

    @task
    def extract_from_proxy() -> dict[str, Any]:
        pg_conn = connect_pg(BaseHook.get_connection("dwh_egisz_pg"))
        try:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT last_log_id FROM elt_state WHERE pipeline = %s", (PIPELINE,))
                row = cur.fetchone()
                last_log_id = row[0] if row else 0
        finally:
            pg_conn.close()

        fb_conn = connect_fb(BaseHook.get_connection("proxy_egisz_fb"))
        try:
            with fb_conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT FIRST {BATCH_SIZE} LOGID, LOGDATE, MSGID, LOGSTATE, LOGTEXT, MSGTEXT
                    FROM EXCHANGELOG
                    WHERE LOGID > ?
                    ORDER BY LOGID
                    """,
                    (last_log_id,),
                )
                rows = [_to_xcom_row(row) for row in cur.fetchall()]
        finally:
            fb_conn.close()

        return {
            "rows": rows,
            "count": len(rows),
            "max_id": max((r[0] for r in rows), default=last_log_id),
        }

    @task
    def load_to_dwh(raw_info: dict[str, Any]) -> dict[str, int]:
        if raw_info["count"] == 0:
            return {"count": 0, "max_id": int(raw_info["max_id"])}

        pg_conn = connect_pg(BaseHook.get_connection("dwh_egisz_pg"))
        try:
            load_raw_logs(pg_conn, raw_info["rows"])
        finally:
            pg_conn.close()

        return {"count": int(raw_info["count"]), "max_id": int(raw_info["max_id"])}

    @task
    def transform_data(load_info: dict[str, int]) -> dict[str, int]:
        if load_info["count"] == 0:
            return {"max_id": int(load_info["max_id"]), "transformed": 0}

        pg_conn = connect_pg(BaseHook.get_connection("dwh_egisz_pg"))
        try:
            transformed = transform_raw_to_facts(pg_conn, int(load_info["max_id"]))
        finally:
            pg_conn.close()

        return {"max_id": int(load_info["max_id"]), "transformed": transformed}

    @task
    def update_watermark(cursor_info: dict[str, int]) -> None:
        pg_conn = connect_pg(BaseHook.get_connection("dwh_egisz_pg"))
        try:
            update_cursors(pg_conn, PIPELINE, log_id=cursor_info["max_id"], egmid=0)
        finally:
            pg_conn.close()

    bootstrap_task = inspect_and_init_dwh()
    sync_dims_task = sync_dimensions()
    raw_info = extract_from_proxy()
    load_info = load_to_dwh(raw_info)
    cursor_info = transform_data(load_info)
    watermark_task = update_watermark(cursor_info)

    bootstrap_task >> sync_dims_task >> raw_info >> load_info >> cursor_info >> watermark_task


dag_instance = egisz_elt()
