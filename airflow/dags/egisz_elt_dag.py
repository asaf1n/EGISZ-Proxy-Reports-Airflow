from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

from egisz_elt.fb_client import connect_fb
from egisz_elt.normalize import normalize_exchange_row
from egisz_elt.pg_client import (
    connect_pg,
    ensure_tables,
    load_raw_logs,
    sync_directory,
    update_cursors,
    upsert_facts,
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
    def setup_db() -> None:
        pg_conn = connect_pg(BaseHook.get_connection("dwh_egisz_pg"))
        try:
            ensure_tables(pg_conn)
        finally:
            pg_conn.close()

    @task
    def sync_dims() -> None:
        pg_conn = connect_pg(BaseHook.get_connection("dwh_egisz_pg"))
        fb_conn = connect_fb(BaseHook.get_connection("proxy_egisz_fb"))
        try:
            with fb_conn.cursor() as cur:
                cur.execute("SELECT JID, NAME, INN, ADDRESS FROM JPERSONS")
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
            return {"max_id": int(load_info["max_id"])}

        pg_conn = connect_pg(BaseHook.get_connection("dwh_egisz_pg"))
        try:
            with pg_conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        logid AS "LOGID",
                        logdate AS "LOGDATE",
                        msgid AS "MSGID",
                        logstate AS "LOGSTATE",
                        logtext AS "LOGTEXT",
                        msgtext AS "MSGTEXT"
                    FROM egisz_raw r
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM fact_egisz_transactions f
                        WHERE f.exchangelog_log_id = r.logid
                    )
                    LIMIT %s
                    """,
                    (BATCH_SIZE,),
                )
                raw_rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

            fact_rows = [norm for row in raw_rows if (norm := normalize_exchange_row(row))]
            upsert_facts(pg_conn, fact_rows)
        finally:
            pg_conn.close()

        return {"max_id": int(load_info["max_id"])}

    @task
    def update_watermark(cursor_info: dict[str, int]) -> None:
        pg_conn = connect_pg(BaseHook.get_connection("dwh_egisz_pg"))
        try:
            update_cursors(pg_conn, PIPELINE, log_id=cursor_info["max_id"], egmid=0)
        finally:
            pg_conn.close()

    setup_db_task = setup_db()
    sync_dims_task = sync_dims()
    raw_info = extract_from_proxy()
    load_info = load_to_dwh(raw_info)
    cursor_info = transform_data(load_info)
    watermark_task = update_watermark(cursor_info)

    setup_db_task >> sync_dims_task >> raw_info >> load_info >> cursor_info >> watermark_task


dag_instance = egisz_elt()
