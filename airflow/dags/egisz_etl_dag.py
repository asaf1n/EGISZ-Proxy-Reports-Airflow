from __future__ import annotations
import logging
from typing import Any
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException

# Импорты ваших модулей (убедитесь, что пути соответствуют вашей структуре)
from egisz_etl.fb_client import connect_fb, fetch_rows_after_cursor
from egisz_etl.pg_client import (
    connect_pg, ensure_tables, get_last_cursor, set_last_cursors, 
    upsert_rows_and_facts
)

log = logging.getLogger(__name__)

# Constants
PIPELINE = "egisz_sync_v1"
LOG_SOURCE_TABLE = "EXCHANGELOG"
MSG_SOURCE_TABLE = "EGISZ_MESSAGES"
LOG_TARGET_TABLE = "proxy_reports_raw"
FACT_TARGET_TABLE = "fact_egisz_transactions"
BATCH_SIZE = 500


def _normalize_document_identifier(value: Any) -> str | None:
    """Normalize document identifier per EGISZ spec."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) == 36 and s.count('-') == 4:
        return s.lower()
    return s


def _extract_document_identity(row: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    """Extract document identifiers from row with priority."""
    relates_to = row.get("RELATESTOMESSAGE") or row.get("relates_to_message")
    local_uid = row.get("DOCUMENTID") or row.get("local_uid") or row.get("localuid")
    emdr_id = row.get("EMDR_ID") or row.get("emdr_id")
    
    return (
        _normalize_document_identifier(relates_to),
        _normalize_document_identifier(local_uid),
        _normalize_document_identifier(emdr_id)
    )


def _resolve_document_key(relates_to: str | None, local_uid: str | None, emdr_id: str | None) -> str | None:
    """Resolve effective document identity key per egisz-monitor-corp logic."""
    if relates_to:
        return relates_to
    if local_uid:
        return local_uid
    if emdr_id:
        return emdr_id
    return None


@dag(
    dag_id="egisz_etl_dag",
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["egisz"],
    description="ETL from proxy_egisz (Firebird) to dwh_egisz (PostgreSQL)"
)
def egisz_etl():

    @task(multiple_outputs=True)
    def get_cursors() -> dict[str, str]:
        """Extract last_log_id and last_egmid from etl_state in PostgreSQL."""
        conn = BaseHook.get_connection('dwh_egisz_pg')
        pg_con = connect_pg(conn)
        
        try:
            ensure_tables(pg_con, target_table=LOG_TARGET_TABLE, fact_table=FACT_TARGET_TABLE)
            last_log_id = get_last_cursor(pg_con, pipeline=PIPELINE, cursor_name="last_log_id")
            last_egmid = get_last_cursor(pg_con, pipeline=PIPELINE, cursor_name="last_egmid")
            
            log.info(f"Retrieved cursors: log_id={last_log_id!r}, egmid={last_egmid!r}")
            return {
                "last_log_id": last_log_id,
                "last_egmid": last_egmid
            }
        finally:
            pg_con.close()

    @task(multiple_outputs=True)
    def extract_from_proxy(cursors: dict[str, str]) -> dict[str, list[dict[str, Any]]]:
        """Extract batches from EXCHANGELOG and EGISZ_MESSAGES based on cursors."""
        conn = BaseHook.get_connection('proxy_egisz_fb')
        fb_con = connect_fb(conn)
        
        try:
            log.info(f"Fetching up to {BATCH_SIZE} log rows after {cursors['last_log_id']}")
            raw_log_rows = fetch_rows_after_cursor(
                fb_con,
                source_table=LOG_SOURCE_TABLE,
                cursor_column="LOGID",
                after_cursor=cursors["last_log_id"],
                limit=BATCH_SIZE,
            )
            
            log.info(f"Fetching up to {BATCH_SIZE} message rows after {cursors['last_egmid']}")
            raw_msg_rows = fetch_rows_after_cursor(
                fb_con,
                source_table=MSG_SOURCE_TABLE,
                cursor_column="EGMID",
                after_cursor=cursors["last_egmid"],
                limit=BATCH_SIZE,
            )
            
            log.info(f"Fetched {len(raw_log_rows)} logs and {len(raw_msg_rows)} messages.")
            
            if not raw_log_rows and not raw_msg_rows:
                raise AirflowSkipException("No new rows found. Skipping the rest of the DAG.")
            
            return {
                "raw_log_rows": raw_log_rows,
                "raw_msg_rows": raw_msg_rows
            }
        finally:
            fb_con.close()

    @task(multiple_outputs=True)
    def transform_and_resolve_identity(extract_data: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
        """Index messages, unite logs, and apply Document Identity Resolution."""
        raw_log_rows = extract_data.get("raw_log_rows", [])
        raw_msg_rows = extract_data.get("raw_msg_rows", [])

        # Index messages for fast lookup
        msg_by_msgid: dict[str, dict[str, Any]] = {}
        for msg_row in raw_msg_rows:
            msgid = msg_row.get("MSGID")
            if msgid:
                msg_by_msgid[str(msgid)] = msg_row

        fact_rows: list[dict[str, Any]] = []
        
        for log_row in raw_log_rows:
            msgid = log_row.get("MSGID")
            msg_row = msg_by_msgid.get(str(msgid)) if msgid else None

            # Extract identifiers
            relates_to, local_uid, emdr_id = _extract_document_identity(log_row)
            if msg_row:
                msg_relates_to, msg_local_uid, msg_emdr_id = _extract_document_identity(msg_row)
                relates_to = relates_to or msg_relates_to
                local_uid = local_uid or msg_local_uid
                emdr_id = emdr_id or msg_emdr_id

            # Resolve document key
            doc_key = _resolve_document_key(relates_to, local_uid, emdr_id)
            if not doc_key:
                continue

            fact_rows.append({
                "relates_to_id": relates_to,
                "local_uid_semd": local_uid,
                "emdr_id": emdr_id,
                "jid": msg_row.get("JID") if msg_row else None,
                "status": "unknown",
                "kind_code": msg_row.get("KIND") if msg_row else None,
                "kind_name": None,
                "errors_json": [],
                "org_oid": msg_row.get("ORGANIZATION_OID") if msg_row else None,
                "registration_date": msg_row.get("REGISTRATION_DATE") if msg_row else None,
                "semd_creation_at": msg_row.get("CREATED_AT") if msg_row else None,
                "processed_at": log_row.get("LOGDATE") if log_row else None,
                "exchangelog_log_id": log_row.get("LOGID"),
                "egisz_messages_egmid": msg_row.get("EGMID") if msg_row else None,
                "journal_msgid": msgid,
                "jid_from_license": None,
                "jid_from_gost_log": None,
                "jid_from_gost_reply": None,
                "gost_token_logtext": None,
                "gost_token_replyto": None,
                "jid_sources_mismatch": False,
            })
            
        log.info(f"Built {len(fact_rows)} fact rows.")
        return {
            "raw_log_rows": raw_log_rows,
            "fact_rows": fact_rows
        }

    @task(multiple_outputs=True)
    def load_to_dwh(transformed_data: dict[str, list[dict[str, Any]]]) -> dict[str, str | None]:
        """Perform UPSERT into proxy_reports_raw and fact_egisz_transactions."""
        raw_log_rows = transformed_data.get("raw_log_rows", [])
        fact_rows = transformed_data.get("fact_rows", [])
        
        if not raw_log_rows:
            return {"new_log_cursor": None, "new_egmid_cursor": None}

        conn = BaseHook.get_connection('dwh_egisz_pg')
        pg_con = connect_pg(conn)
        
        try:
            new_log_cursor, new_msg_cursor = upsert_rows_and_facts(
                pg_con,
                target_table=LOG_TARGET_TABLE,
                fact_table=FACT_TARGET_TABLE,
                raw_rows=raw_log_rows,
                fact_rows=fact_rows,
                cursor_column="LOGID",
                pipeline=PIPELINE,
            )
            log.info(f"Upserted. New cursors: log_id={new_log_cursor}, egmid={new_msg_cursor}")
            return {
                "new_log_cursor": new_log_cursor,
                "new_egmid_cursor": new_msg_cursor
            }
        finally:
            pg_con.close()

    @task()
    def update_watermarks(old_cursors: dict[str, str], new_cursors: dict[str, str | None]):
        """Update cursors in etl_state table if new cursors exist."""
        new_log_cursor = new_cursors.get("new_log_cursor")
        new_egmid_cursor = new_cursors.get("new_egmid_cursor")
        
        if not new_log_cursor and not new_egmid_cursor:
            log.info("No new cursors to update.")
            return
            
        final_log_id = new_log_cursor or old_cursors.get("last_log_id") or ""
        final_egmid = new_egmid_cursor or old_cursors.get("last_egmid") or ""

        conn = BaseHook.get_connection('dwh_egisz_pg')
        pg_con = connect_pg(conn)
        
        try:
            set_last_cursors(
                pg_con,
                pipeline=PIPELINE,
                last_log_id=final_log_id,
                last_egmid=final_egmid,
            )
            log.info(f"Successfully updated watermarks: log_id={final_log_id}, egmid={final_egmid}")
        finally:
            pg_con.close()

    # Define task dependencies
    cursors = get_cursors()
    extracted = extract_from_proxy(cursors)
    transformed = transform_and_resolve_identity(extracted)
    loaded_cursors = load_to_dwh(transformed)
    update_watermarks(cursors, loaded_cursors)

dag_instance = egisz_etl()