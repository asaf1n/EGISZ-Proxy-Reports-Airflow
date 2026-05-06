from __future__ import annotations

from datetime import date, datetime
from typing import Any
import re
import json

import psycopg2
from psycopg2.extras import execute_values


_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str, *, what: str) -> str:
    s = str(name or "").strip()
    if not s or not _IDENT_RE.fullmatch(s):
        raise ValueError(f"Invalid {what}: {name!r} (allowed: letters/digits/underscore, must not start with digit)")
    return s


def _make_json_safe(obj: Any) -> Any:
    """Recursively convert datetime/date objects to ISO format strings for JSON serialization."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: _make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [_make_json_safe(item) for item in obj]
    return obj


def connect_pg(conn: Any):
    """Connect to PostgreSQL DWH using Airflow Connection object."""
    port = conn.port if conn.port else 5432
    dsn = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{port}/{conn.schema}"
    con = psycopg2.connect(dsn, connect_timeout=10)
    con.autocommit = False
    return con


def ensure_tables(con, *, target_table: str, fact_table: str = "fact_egisz_transactions") -> None:
    """
    Create schema for dual-cursor ETL:
      - etl_state: stores cursors (last_log_id, last_egmid) per pipeline
      - target_table: raw EXCHANGELOG rows
      - fact_table: normalized EGISZ transaction facts (after parsing SOAP XML)
    """
    tbl = _validate_identifier(target_table, what="target table")
    fact_tbl = _validate_identifier(fact_table, what="fact table")
    
    with con.cursor() as cur:
        # State table for dual cursors
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_state (
              pipeline text PRIMARY KEY,
              last_log_id text NOT NULL DEFAULT '',
              last_egmid text NOT NULL DEFAULT '',
              updated_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )
        
        # Raw EXCHANGELOG staging
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
              cursor_value text PRIMARY KEY,
              row_data jsonb NOT NULL,
              loaded_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )
        
        # Fact table: normalized EGISZ transactions (SOAP callback parsing)
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {fact_tbl} (
              relates_to_id text PRIMARY KEY,
              local_uid_semd text,
              jid integer,
              gost_jid_token text,
              org_oid text,
              kind_code text,
              kind_name text,
              status text NOT NULL DEFAULT 'unknown',
              emdr_id text,
              errors_json jsonb DEFAULT '[]'::jsonb,
              registration_date timestamptz,
              semd_creation_at timestamptz,
              processed_at timestamptz NOT NULL DEFAULT now(),
              exchangelog_log_id bigint,
              egisz_messages_egmid bigint,
              journal_msgid text,
              jid_from_license integer,
              jid_from_gost_log integer,
              jid_from_gost_reply integer,
              gost_token_logtext text,
              gost_token_replyto text,
              jid_sources_mismatch boolean DEFAULT false,
              loaded_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )
        
        # Indexes for fact table
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{fact_tbl}_jid ON {fact_tbl} (jid);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{fact_tbl}_status ON {fact_tbl} (status);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{fact_tbl}_local_uid ON {fact_tbl} (local_uid_semd);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{fact_tbl}_emdr_id ON {fact_tbl} (emdr_id);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{fact_tbl}_processed ON {fact_tbl} (processed_at);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{fact_tbl}_log_id ON {fact_tbl} (exchangelog_log_id);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{fact_tbl}_egmid ON {fact_tbl} (egisz_messages_egmid);")
        
        # Analytic views
        cur.execute(
            f"""
            CREATE OR REPLACE VIEW v_egisz_transactions_by_status AS
            SELECT
              status,
              count(*) AS count,
              count(*) FILTER (WHERE jid IS NOT NULL) AS with_jid,
              max(processed_at) AS latest_processed
            FROM {fact_tbl}
            GROUP BY status;
            """
        )
        
        cur.execute(
            f"""
            CREATE OR REPLACE VIEW v_egisz_transactions_by_clinic AS
            SELECT
              jid,
              gost_jid_token,
              count(*) AS total_count,
              count(*) FILTER (WHERE status = 'success') AS success_count,
              count(*) FILTER (WHERE status = 'error') AS error_count,
              max(processed_at) AS latest_processed
            FROM {fact_tbl}
            WHERE jid IS NOT NULL
            GROUP BY jid, gost_jid_token
            ORDER BY total_count DESC;
            """
        )
        
        cur.execute(
            f"""
            CREATE OR REPLACE VIEW v_egisz_documents_identity_key AS
            SELECT
              relates_to_id,
              COALESCE(
                NULLIF(relates_to_id, ''),
                NULLIF(local_uid_semd, ''),
                NULLIF(emdr_id, '')
              ) AS document_identity_key,
              status,
              jid,
              processed_at
            FROM {fact_tbl};
            """
        )
        
        cur.execute(
            f"""
            CREATE OR REPLACE VIEW v_egisz_errors_detail AS
            SELECT
              relates_to_id,
              local_uid_semd,
              jid,
              kind_code,
              status,
              errors_json,
              processed_at
            FROM {fact_tbl}
            WHERE status = 'error' AND errors_json IS NOT NULL AND jsonb_array_length(errors_json) > 0;
            """
        )
    
    con.commit()


def get_last_cursor(con, *, pipeline: str, cursor_name: str = "last_log_id") -> str:
    """Get the last cursor value (last_log_id or last_egmid) for incremental sync."""
    with con.cursor() as cur:
        cur.execute(f"SELECT {cursor_name} FROM etl_state WHERE pipeline = %s", (pipeline,))
        row = cur.fetchone()
    if not row:
        return ""
    return str(row[0] or "")


def set_last_cursors(con, *, pipeline: str, last_log_id: str, last_egmid: str) -> None:
    """Update both cursors after successful sync."""
    with con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_state (pipeline, last_log_id, last_egmid, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (pipeline) DO UPDATE
            SET last_log_id = EXCLUDED.last_log_id, 
                last_egmid = EXCLUDED.last_egmid,
                updated_at = now();
            """,
            (pipeline, last_log_id, last_egmid),
        )
    con.commit()


def _to_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return bool(value)


def upsert_rows_and_facts(
    con,
    *,
    target_table: str,
    fact_table: str,
    raw_rows: list[dict[str, Any]],
    fact_rows: list[dict[str, Any]],
    cursor_column: str,
    pipeline: str,
) -> tuple[str | None, str | None]:
    """
    Upsert raw logs and normalized facts.
    Returns (last_log_id, last_egmid) for cursor update.
    """
    if not raw_rows:
        return None, None
    
    tbl = _validate_identifier(target_table, what="target table")
    fact_tbl = _validate_identifier(fact_table, what="fact table")
    
    max_cursor: str | None = None
    max_egmid: str | None = None
    tuples: list[tuple[str, str]] = []
    
    for r in raw_rows:
        if cursor_column not in r:
            raise ValueError(f"Cursor column {cursor_column!r} missing in row")
        c = r[cursor_column]
        c_text = "" if c is None else str(c).strip()
        if not c_text:
            continue
        
        json_safe_row = _make_json_safe(r)
        tuples.append((c_text, json.dumps(json_safe_row)))
        
        # Track max cursors
        if max_cursor is None:
            max_cursor = c_text
        else:
            try:
                if int(c_text) > int(max_cursor):
                    max_cursor = c_text
            except Exception:
                if c_text > max_cursor:
                    max_cursor = c_text
        
        # Track EGMID if present
        egmid = r.get("EGMID")
        if egmid:
            egmid_text = str(egmid).strip()
            if max_egmid is None:
                max_egmid = egmid_text
            else:
                try:
                    if int(egmid_text) > int(max_egmid):
                        max_egmid = egmid_text
                except Exception:
                    if egmid_text > max_egmid:
                        max_egmid = egmid_text
    
    if not tuples:
        return None, None
    
    # Upsert raw rows
    try:
        with con.cursor() as cur:
            for c_text, json_str in tuples:
                cur.execute(
                    f"""
                    INSERT INTO {tbl} (cursor_value, row_data)
                    VALUES (%s, %s::jsonb)
                    ON CONFLICT (cursor_value) DO UPDATE
                    SET row_data = EXCLUDED.row_data, loaded_at = now();
                    """,
                    (c_text, json_str),
                )
            
            # Upsert fact rows
            if fact_rows:
                fact_tuples = []
                for row in fact_rows:
                    fact_tuples.append((
                        row.get("relates_to_id"),
                        _to_text(row.get("local_uid_semd")),
                        _to_int(row.get("jid")),
                        _to_text(row.get("gost_jid_token")),
                        _to_text(row.get("org_oid")),
                        _to_text(row.get("kind_code")),
                        _to_text(row.get("kind_name")),
                        _to_text(row.get("status")) or "unknown",
                        _to_text(row.get("emdr_id")),
                        json.dumps(row.get("errors_json") or []),
                        row.get("registration_date"),
                        row.get("semd_creation_at"),
                        row.get("processed_at"),
                        _to_int(row.get("exchangelog_log_id")),
                        _to_int(row.get("egisz_messages_egmid")),
                        _to_text(row.get("journal_msgid")),
                        _to_int(row.get("jid_from_license")),
                        _to_int(row.get("jid_from_gost_log")),
                        _to_int(row.get("jid_from_gost_reply")),
                        _to_text(row.get("gost_token_logtext")),
                        _to_text(row.get("gost_token_replyto")),
                        _to_bool(row.get("jid_sources_mismatch", False)),
                    ))
                
                execute_values(
                    cur,
                    f"""
                    INSERT INTO {fact_tbl} (
                      relates_to_id, local_uid_semd, jid, gost_jid_token, org_oid,
                      kind_code, kind_name, status, emdr_id, errors_json,
                      registration_date, semd_creation_at, processed_at,
                      exchangelog_log_id, egisz_messages_egmid, journal_msgid,
                      jid_from_license, jid_from_gost_log, jid_from_gost_reply,
                      gost_token_logtext, gost_token_replyto, jid_sources_mismatch
                    )
                    VALUES %s
                    ON CONFLICT (relates_to_id) DO UPDATE SET
                      local_uid_semd = EXCLUDED.local_uid_semd,
                      jid = EXCLUDED.jid,
                      gost_jid_token = EXCLUDED.gost_jid_token,
                      org_oid = EXCLUDED.org_oid,
                      kind_code = EXCLUDED.kind_code,
                      kind_name = EXCLUDED.kind_name,
                      status = EXCLUDED.status,
                      emdr_id = EXCLUDED.emdr_id,
                      errors_json = EXCLUDED.errors_json,
                      registration_date = EXCLUDED.registration_date,
                      semd_creation_at = EXCLUDED.semd_creation_at,
                      processed_at = EXCLUDED.processed_at,
                      exchangelog_log_id = EXCLUDED.exchangelog_log_id,
                      egisz_messages_egmid = EXCLUDED.egisz_messages_egmid,
                      journal_msgid = EXCLUDED.journal_msgid,
                      jid_from_license = EXCLUDED.jid_from_license,
                      jid_from_gost_log = EXCLUDED.jid_from_gost_log,
                      jid_from_gost_reply = EXCLUDED.jid_from_gost_reply,
                      gost_token_logtext = EXCLUDED.gost_token_logtext,
                      gost_token_replyto = EXCLUDED.gost_token_replyto,
                      jid_sources_mismatch = EXCLUDED.jid_sources_mismatch,
                      loaded_at = now();
                    """,
                    fact_tuples,
                )
        con.commit()
    except Exception:
        con.rollback()
        raise
    
    return max_cursor, max_egmid


def ping_pg(con) -> None:
    with con.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
