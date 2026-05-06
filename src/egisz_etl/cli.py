from __future__ import annotations

import sys
import logging
from typing import Any

from egisz_etl.config import FirebirdConfig, PostgresConfig, ETLConfig
from egisz_etl.fb_client import connect_fb, fetch_rows_after_cursor, ping_fb
from egisz_etl.pg_client import (
    connect_pg, ensure_tables, get_last_cursor, set_last_cursors, 
    upsert_rows_and_facts, ping_pg
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)


def _normalize_document_identifier(value: Any) -> str | None:
    """
    Normalize document identifier per EGISZ spec:
    UUID → lowercase, other identifiers kept as-is.
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Simple UUID lowercase normalization
    if len(s) == 36 and s.count('-') == 4:  # UUID format
        return s.lower()
    return s


def _extract_document_identity(row: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    """
    Extract document identifiers from row with priority:
    1. relatesToMessage (correlation ID from SOAP callback)
    2. DOCUMENTID or local_uid (document identifier)
    3. emdrId (federal registry ID)
    """
    relates_to = row.get("RELATESTOMESSAGE") or row.get("relates_to_message")
    local_uid = row.get("DOCUMENTID") or row.get("local_uid") or row.get("localuid")
    emdr_id = row.get("EMDR_ID") or row.get("emdr_id")
    
    # Normalize
    relates_to = _normalize_document_identifier(relates_to)
    local_uid = _normalize_document_identifier(local_uid)
    emdr_id = _normalize_document_identifier(emdr_id)
    
    return relates_to, local_uid, emdr_id


def _resolve_document_key(
    relates_to: str | None,
    local_uid: str | None,
    emdr_id: str | None,
) -> str | None:
    """
    Resolve effective document identity key per egisz-monitor-corp logic:
    Priority: relatesToMessage → localUid/DOCUMENTID → emdrId
    """
    if relates_to:
        return relates_to
    if local_uid:
        return local_uid
    if emdr_id:
        return emdr_id
    return None


def sync() -> None:
    """Run incremental ETL sync: Firebird EXCHANGELOG/EGISZ_MESSAGES → PostgreSQL DWH with dual cursors."""
    fb_cfg = FirebirdConfig.from_env()
    pg_cfg = PostgresConfig.from_env()
    etl_cfg = ETLConfig.from_env()

    log.info(f"Starting ETL sync: {etl_cfg.pipeline}")
    log.info(f"  Source (Firebird): {fb_cfg.dsn}")
    log.info(f"  Target (Postgres): {pg_cfg.dsn}")
    log.info(f"  Log table: {etl_cfg.log_source_table} (cursor: {etl_cfg.log_cursor_column})")
    log.info(f"  Message table: {etl_cfg.msg_source_table} (cursor: {etl_cfg.msg_cursor_column})")
    log.info(f"  Batch size: {etl_cfg.batch_size}")

    # Connect to both databases
    fb_con = connect_fb(fb_cfg)
    pg_con = connect_pg(pg_cfg)

    try:
        # Ping both databases
        log.info("Pinging Firebird...")
        ping_fb(fb_con)
        log.info("Firebird OK")

        log.info("Pinging PostgreSQL...")
        ping_pg(pg_con)
        log.info("PostgreSQL OK")

        # Ensure target schema exists
        log.info(f"Ensuring tables in PostgreSQL...")
        ensure_tables(
            pg_con,
            target_table=etl_cfg.log_target_table,
            fact_table=etl_cfg.fact_target_table
        )
        log.info("Tables OK")

        # Get last cursors
        last_log_id = get_last_cursor(pg_con, pipeline=etl_cfg.pipeline, cursor_name="last_log_id")
        last_egmid = get_last_cursor(pg_con, pipeline=etl_cfg.pipeline, cursor_name="last_egmid")
        log.info(f"Last cursors: log_id={last_log_id!r}, egmid={last_egmid!r}")

        # Build SQL for dual fetch
        log_sql = f"{etl_cfg.log_source_table}"
        msg_sql = f"{etl_cfg.msg_source_table}"

        # Fetch batches from both sources
        log.info(f"Fetching batch of {etl_cfg.batch_size} rows from {etl_cfg.log_source_table}...")
        raw_log_rows = fetch_rows_after_cursor(
            fb_con,
            source_table=etl_cfg.log_source_table,
            cursor_column=etl_cfg.log_cursor_column,
            after_cursor=last_log_id,
            limit=etl_cfg.batch_size,
        )
        log.info(f"Fetched {len(raw_log_rows)} log rows")

        log.info(f"Fetching batch of {etl_cfg.batch_size} rows from {etl_cfg.msg_source_table}...")
        raw_msg_rows = fetch_rows_after_cursor(
            fb_con,
            source_table=etl_cfg.msg_source_table,
            cursor_column=etl_cfg.msg_cursor_column,
            after_cursor=last_egmid,
            limit=etl_cfg.batch_size,
        )
        log.info(f"Fetched {len(raw_msg_rows)} message rows")

        if not raw_log_rows and not raw_msg_rows:
            log.info("No new rows; sync complete")
            return

        # Index messages for fast lookup
        msg_by_msgid: dict[str, dict[str, Any]] = {}
        for msg_row in raw_msg_rows:
            msgid = msg_row.get("MSGID")
            if msgid:
                msg_by_msgid[str(msgid)] = msg_row

        # Build fact rows from log rows enriched with message metadata
        log.info("Building fact rows with document identity resolution...")
        fact_rows: list[dict[str, Any]] = []
        
        for log_row in raw_log_rows:
            msgid = log_row.get("MSGID")
            msg_row = msg_by_msgid.get(str(msgid)) if msgid else None

            # Extract identifiers
            relates_to, local_uid, emdr_id = _extract_document_identity(log_row)
            if msg_row:
                msg_relates_to, msg_local_uid, msg_emdr_id = _extract_document_identity(msg_row)
                # Merge with priority: log message > log row (for document identity)
                relates_to = relates_to or msg_relates_to
                local_uid = local_uid or msg_local_uid
                emdr_id = emdr_id or msg_emdr_id

            # Resolve document key
            doc_key = _resolve_document_key(relates_to, local_uid, emdr_id)
            
            if not doc_key:
                log.debug(f"Skipping row without document identity: logid={log_row.get('LOGID')}")
                continue

            fact_row = {
                "relates_to_id": relates_to,
                "local_uid_semd": local_uid,
                "emdr_id": emdr_id,
                "jid": msg_row.get("JID") if msg_row else None,
                "status": "unknown",  # Would be parsed from SOAP XML in full implementation
                "kind_code": msg_row.get("KIND") if msg_row else None,
                "kind_name": None,  # Would look up from semd_dictionary
                "errors_json": [],
                "org_oid": msg_row.get("ORGANIZATION_OID") if msg_row else None,
                "registration_date": msg_row.get("REGISTRATION_DATE") if msg_row else None,
                "semd_creation_at": msg_row.get("CREATED_AT") if msg_row else None,
                "processed_at": log_row.get("LOGDATE") if log_row else None,
                "exchangelog_log_id": log_row.get("LOGID"),
                "egisz_messages_egmid": msg_row.get("EGMID") if msg_row else None,
                "journal_msgid": msgid,
                "jid_from_license": None,  # Would resolve from EGISZ_LICENSES
                "jid_from_gost_log": None,  # Would extract from LOGTEXT
                "jid_from_gost_reply": None,  # Would extract from REPLYTO
                "gost_token_logtext": None,
                "gost_token_replyto": None,
                "jid_sources_mismatch": False,
            }
            fact_rows.append(fact_row)

        log.info(f"Built {len(fact_rows)} fact rows")

        # Upsert into PostgreSQL
        log.info("Upserting rows and facts into PostgreSQL...")
        new_log_cursor, new_msg_cursor = upsert_rows_and_facts(
            pg_con,
            target_table=etl_cfg.log_target_table,
            fact_table=etl_cfg.fact_target_table,
            raw_rows=raw_log_rows,
            fact_rows=fact_rows,
            cursor_column=etl_cfg.log_cursor_column,
            pipeline=etl_cfg.pipeline,
        )
        
        # Update both cursors
        if new_log_cursor or new_msg_cursor:
            set_last_cursors(
                pg_con,
                pipeline=etl_cfg.pipeline,
                last_log_id=new_log_cursor or last_log_id,
                last_egmid=new_msg_cursor or last_egmid,
            )
            log.info(f"Upserted {len(raw_log_rows)} log rows, {len(fact_rows)} facts")
            log.info(f"Cursors updated: log_id={new_log_cursor!r}, egmid={new_msg_cursor!r}")
        
        log.info("Sync complete")

    finally:
        fb_con.close()
        pg_con.close()


def main() -> None:
    """CLI entry point."""
    if len(sys.argv) > 1:
        command = sys.argv[1]
    else:
        command = "sync"

    if command == "sync":
        sync()
    else:
        log.error(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
