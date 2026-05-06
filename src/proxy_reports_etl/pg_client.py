from __future__ import annotations

from datetime import date, datetime
from typing import Any
import re

import psycopg2
from psycopg2.extras import Json, execute_values

from proxy_reports_etl.config import PostgresConfig


_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str, *, what: str) -> str:
    s = str(name or "").strip()
    if not s or not _IDENT_RE.fullmatch(s):
        raise ValueError(f"Invalid {what}: {name!r} (allowed: letters/digits/underscore, must not start with digit)")
    return s


def connect_pg(cfg: PostgresConfig):
    con = psycopg2.connect(cfg.dsn, connect_timeout=10)
    con.autocommit = False
    return con


def ensure_tables(con, *, target_table: str) -> None:
    """
    Create minimal schema:
      - etl_state: stores last_cursor per pipeline
      - target_table: raw rows as jsonb keyed by cursor_value
    """
    tbl = _validate_identifier(target_table, what="target table")
    with con.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_state (
              pipeline text PRIMARY KEY,
              last_cursor text NOT NULL DEFAULT '',
              updated_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
              cursor_value text PRIMARY KEY,
              row_data jsonb NOT NULL,
              loaded_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS fact_proxy_exchange (
              log_id bigint PRIMARY KEY,
              log_date timestamptz,
              log_type integer,
              log_state integer,
              log_mode integer,
              msg_id text,
              group_id text,
              created_at timestamptz,
              modified_at timestamptz,
              repl_id text,
              repl_group_id text,
              endpoint text,
              method text,
              uri text,
              action text,
              parent_log_id bigint,
              message_id text,
              relates_to text,
              local_uid text,
              organization_oid text,
              document_kind text,
              status_category text NOT NULL,
              error_text text,
              clean_error_text text,
              loaded_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_proxy_exchange_log_date ON fact_proxy_exchange (log_date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_proxy_exchange_msg_id ON fact_proxy_exchange (msg_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_proxy_exchange_status ON fact_proxy_exchange (status_category);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_proxy_exchange_action ON fact_proxy_exchange (action);")
        cur.execute(
            """
            CREATE OR REPLACE VIEW v_proxy_exchange_detail AS
            SELECT
              log_id,
              log_date,
              created_at,
              modified_at,
              msg_id,
              message_id,
              relates_to,
              local_uid,
              organization_oid,
              document_kind,
              status_category,
              clean_error_text,
              error_text,
              endpoint,
              method,
              uri,
              action,
              log_type,
              log_state,
              log_mode,
              parent_log_id
            FROM fact_proxy_exchange;
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE VIEW v_proxy_exchange_daily AS
            SELECT
              date_trunc('day', COALESCE(log_date, created_at, modified_at))::date AS day,
              count(*)::bigint AS total_rows,
              count(*) FILTER (WHERE status_category = 'success')::bigint AS success_rows,
              count(*) FILTER (WHERE status_category = 'error')::bigint AS error_rows,
              count(*) FILTER (WHERE status_category = 'unknown')::bigint AS unknown_rows
            FROM fact_proxy_exchange
            GROUP BY 1;
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE VIEW v_proxy_exchange_service_summary AS
            SELECT
              COALESCE(NULLIF(action, ''), NULLIF(method, ''), NULLIF(uri, ''), '[unknown]') AS service_action,
              count(*)::bigint AS total_rows,
              count(*) FILTER (WHERE status_category = 'success')::bigint AS success_rows,
              count(*) FILTER (WHERE status_category = 'error')::bigint AS error_rows,
              max(COALESCE(log_date, created_at, modified_at)) AS last_seen_at
            FROM fact_proxy_exchange
            GROUP BY 1;
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE VIEW v_proxy_exchange_error_summary AS
            SELECT
              COALESCE(NULLIF(clean_error_text, ''), '[no_error_text]') AS clean_error_text,
              count(*)::bigint AS total_rows,
              max(COALESCE(log_date, created_at, modified_at)) AS last_seen_at
            FROM fact_proxy_exchange
            WHERE status_category = 'error'
            GROUP BY 1;
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE VIEW v_proxy_exchange_latest AS
            SELECT *
            FROM v_proxy_exchange_detail
            ORDER BY COALESCE(log_date, created_at, modified_at) DESC NULLS LAST, log_id DESC
            LIMIT 500;
            """
        )
    con.commit()


def get_last_cursor(con, *, pipeline: str) -> str:
    with con.cursor() as cur:
        cur.execute("SELECT last_cursor FROM etl_state WHERE pipeline = %s", (pipeline,))
        row = cur.fetchone()
    if not row:
        return ""
    return str(row[0] or "")


def set_last_cursor(con, *, pipeline: str, last_cursor: str) -> None:
    with con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_state (pipeline, last_cursor, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (pipeline) DO UPDATE
            SET last_cursor = EXCLUDED.last_cursor, updated_at = now();
            """,
            (pipeline, last_cursor),
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


def _to_datetime(value: Any) -> datetime | date | None:
    if isinstance(value, (datetime, date)):
        return value
    return None


def _fact_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _to_int(row.get("log_id")),
        _to_datetime(row.get("log_date")),
        _to_int(row.get("log_type")),
        _to_int(row.get("log_state")),
        _to_int(row.get("log_mode")),
        _to_text(row.get("msg_id")),
        _to_text(row.get("group_id")),
        _to_datetime(row.get("created_at")),
        _to_datetime(row.get("modified_at")),
        _to_text(row.get("repl_id")),
        _to_text(row.get("repl_group_id")),
        _to_text(row.get("endpoint")),
        _to_text(row.get("method")),
        _to_text(row.get("uri")),
        _to_text(row.get("action")),
        _to_int(row.get("parent_log_id")),
        _to_text(row.get("message_id")),
        _to_text(row.get("relates_to")),
        _to_text(row.get("local_uid")),
        _to_text(row.get("organization_oid")),
        _to_text(row.get("document_kind")),
        _to_text(row.get("status_category")) or "unknown",
        _to_text(row.get("error_text")),
        _to_text(row.get("clean_error_text")),
    )


def upsert_rows_and_state(
    con,
    *,
    target_table: str,
    raw_rows: list[dict[str, Any]],
    fact_rows: list[dict[str, Any]],
    cursor_column: str,
    pipeline: str,
) -> str | None:
    """
    Upsert rows into target_table by cursor_value; store entire row as jsonb.
    Returns max cursor_value (as text) seen in the batch.
    """
    if not raw_rows:
        return None
    tbl = _validate_identifier(target_table, what="target table")
    # Determine max cursor as string, preserving natural ordering for numbers if possible.
    max_cursor: str | None = None
    tuples: list[tuple[str, Any]] = []
    for r in raw_rows:
        if cursor_column not in r:
            raise ValueError(f"Cursor column {cursor_column!r} missing in row")
        c = r[cursor_column]
        c_text = "" if c is None else str(c).strip()
        if not c_text:
            continue
        tuples.append((c_text, Json(r)))
        if max_cursor is None:
            max_cursor = c_text
        else:
            # Try numeric compare first
            try:
                if int(c_text) > int(max_cursor):
                    max_cursor = c_text
            except Exception:
                if c_text > max_cursor:
                    max_cursor = c_text

    if not tuples:
        return None
    fact_tuples = [_fact_tuple(row) for row in fact_rows if _to_int(row.get("log_id")) is not None]
    try:
        with con.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {tbl} (cursor_value, row_data)
                VALUES %s
                ON CONFLICT (cursor_value) DO UPDATE
                SET row_data = EXCLUDED.row_data, loaded_at = now();
                """,
                tuples,
                template="(%s, %s)",
            )
            if fact_tuples:
                execute_values(
                    cur,
                    """
                    INSERT INTO fact_proxy_exchange (
                      log_id, log_date, log_type, log_state, log_mode, msg_id, group_id,
                      created_at, modified_at, repl_id, repl_group_id, endpoint, method,
                      uri, action, parent_log_id, message_id, relates_to, local_uid,
                      organization_oid, document_kind, status_category, error_text, clean_error_text
                    )
                    VALUES %s
                    ON CONFLICT (log_id) DO UPDATE SET
                      log_date = EXCLUDED.log_date,
                      log_type = EXCLUDED.log_type,
                      log_state = EXCLUDED.log_state,
                      log_mode = EXCLUDED.log_mode,
                      msg_id = EXCLUDED.msg_id,
                      group_id = EXCLUDED.group_id,
                      created_at = EXCLUDED.created_at,
                      modified_at = EXCLUDED.modified_at,
                      repl_id = EXCLUDED.repl_id,
                      repl_group_id = EXCLUDED.repl_group_id,
                      endpoint = EXCLUDED.endpoint,
                      method = EXCLUDED.method,
                      uri = EXCLUDED.uri,
                      action = EXCLUDED.action,
                      parent_log_id = EXCLUDED.parent_log_id,
                      message_id = EXCLUDED.message_id,
                      relates_to = EXCLUDED.relates_to,
                      local_uid = EXCLUDED.local_uid,
                      organization_oid = EXCLUDED.organization_oid,
                      document_kind = EXCLUDED.document_kind,
                      status_category = EXCLUDED.status_category,
                      error_text = EXCLUDED.error_text,
                      clean_error_text = EXCLUDED.clean_error_text,
                      loaded_at = now();
                    """,
                    fact_tuples,
                )
            cur.execute(
                """
                INSERT INTO etl_state (pipeline, last_cursor, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (pipeline) DO UPDATE
                SET last_cursor = EXCLUDED.last_cursor, updated_at = now();
                """,
                (pipeline, max_cursor),
            )
        con.commit()
    except Exception:
        con.rollback()
        raise
    return max_cursor


def upsert_raw_rows(con, *, target_table: str, rows: list[dict[str, Any]], cursor_column: str) -> str | None:
    return upsert_rows_and_state(
        con,
        target_table=target_table,
        raw_rows=rows,
        fact_rows=[],
        cursor_column=cursor_column,
        pipeline="_legacy_raw_only",
    )


def ping_pg(con) -> None:
    with con.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
