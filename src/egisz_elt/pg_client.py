from __future__ import annotations

import logging
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

ALLOWED_SYNC_TABLES = {"dim_organizations", "dim_licenses"}
DIRECTORY_COLUMNS = {
    "dim_organizations": ("jid", "name", "inn", "address"),
    "dim_licenses": ("id", "service_type", "jid", "mo_uid", "mo_domen", "bdate", "fdate", "kind", "modifydate"),
}
DIRECTORY_PK_COLUMNS = {
    "dim_organizations": ("jid",),
    "dim_licenses": ("id",),
}

RAW_LOG_COLUMNS = ("logid", "logdate", "createdate", "msgid", "logstate", "logtext", "msgtext")
RAW_MESSAGE_COLUMNS = ("egmid", "jid", "kind", "created_at", "msgid", "reply_to", "document_id", "msgtext")
DIRECTORY_SYNC_LOCK_TIMEOUT = "15s"
DIRECTORY_SYNC_STATEMENT_TIMEOUT = "5min"
DIRECTORY_SYNC_PAGE_SIZE = 1000


def normalize_message_id(value: Any) -> Any:
    """Normalize EGISZ UUID wrappers while preserving empty/null values."""
    if value is None:
        return None
    text = str(value).strip()
    if text.startswith("<") and text.endswith(">"):
        text = text[1:-1].strip()
    if text.lower().startswith("urn:uuid:"):
        text = text[len("urn:uuid:") :]
    return text or None


def connect_pg(conn_params: Any) -> psycopg2.extensions.connection:
    if isinstance(conn_params, str):
        return psycopg2.connect(conn_params)
    return psycopg2.connect(
        host=conn_params.host,
        port=conn_params.port,
        user=conn_params.login,
        password=conn_params.password,
        database=conn_params.schema,
    )


def get_cursors(con: psycopg2.extensions.connection, pipeline: str) -> tuple[int, int]:
    """Read the last processed Firebird cursors for a pipeline."""
    with con.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(last_log_id), MAX(last_egmid)
            FROM elt_state
            WHERE pipeline IN (%s, 'main')
            """,
            (pipeline,),
        )
        row = cur.fetchone()
    if row is None:
        return (0, 0)
    return (int(row[0] or 0), int(row[1] or 0))


def load_raw_logs(con: psycopg2.extensions.connection, rows: list[dict[str, Any]] | list[tuple[Any, ...]]) -> None:
    """Load EXCHANGELOG rows into exchangelog_raw without transforming them in Python."""
    values: list[tuple[Any, ...]] = []
    for row in rows:
        if isinstance(row, dict):
            missing_columns = [column for column in RAW_LOG_COLUMNS if column not in row]
            if missing_columns:
                raise ValueError(f"Raw EXCHANGELOG row is missing required column(s): {', '.join(missing_columns)}")
            values.append(tuple(row[column] for column in RAW_LOG_COLUMNS))
        else:
            values.append(tuple(row))

    if not values:
        return

    with con.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO exchangelog_raw (logid, logdate, createdate, msgid, logstate, logtext, msgtext)
            VALUES %s
            ON CONFLICT (logid) DO UPDATE SET
                logdate = EXCLUDED.logdate,
                createdate = EXCLUDED.createdate,
                msgid = EXCLUDED.msgid,
                logstate = EXCLUDED.logstate,
                logtext = EXCLUDED.logtext,
                msgtext = EXCLUDED.msgtext,
                loaded_at = now()
            """,
            values,
        )
    con.commit()


def load_raw_messages(con: psycopg2.extensions.connection, rows: list[dict[str, Any]]) -> None:
    """Load EGISZ_MESSAGES rows into egisz_messages_raw."""
    values: list[tuple[Any, ...]] = []
    for row in rows:
        missing_columns = [column for column in RAW_MESSAGE_COLUMNS if column not in row]
        if missing_columns:
            raise ValueError(f"Raw EGISZ_MESSAGES row is missing required column(s): {', '.join(missing_columns)}")
        normalized = dict(row)
        normalized["msgid"] = normalize_message_id(normalized.get("msgid"))
        normalized["reply_to"] = normalize_message_id(normalized.get("reply_to"))
        values.append(tuple(normalized[column] for column in RAW_MESSAGE_COLUMNS))

    if not values:
        return

    with con.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO egisz_messages_raw (egmid, jid, kind, created_at, msgid, reply_to, document_id, msgtext)
            VALUES %s
            ON CONFLICT (egmid) DO UPDATE SET
                jid = EXCLUDED.jid,
                kind = EXCLUDED.kind,
                created_at = EXCLUDED.created_at,
                msgid = EXCLUDED.msgid,
                reply_to = EXCLUDED.reply_to,
                document_id = EXCLUDED.document_id,
                msgtext = EXCLUDED.msgtext,
                loaded_at = now()
            """,
            values,
        )
    con.commit()


def transform_raw_to_facts(
    con: psycopg2.extensions.connection,
    *,
    min_log_id: int,
    max_log_id: int,
    min_egmid: int = 0,
    max_egmid: int = 0,
) -> int:
    """Run the database-side ELT transform and refresh EGISZ materialized views if present."""
    with con.cursor() as cur:
        cur.execute(
            "SELECT public.egisz_transform_raw_to_facts(%s, %s, %s, %s)",
            (min_log_id, max_log_id, min_egmid, max_egmid),
        )
        transformed = int(cur.fetchone()[0] or 0)
        cur.execute(
            """
            SELECT schemaname, matviewname
            FROM pg_matviews
            WHERE schemaname = 'public'
              AND matviewname LIKE 'mv_egisz_%'
            """
        )
        for schema_name, view_name in cur.fetchall():
            cur.execute(f"REFRESH MATERIALIZED VIEW {schema_name}.{view_name}")
    con.commit()
    return transformed


def sync_directory(con: psycopg2.extensions.connection, table_name: str, rows: list[tuple[Any, ...]]) -> None:
    if table_name not in ALLOWED_SYNC_TABLES:
        raise ValueError(f"Unsupported directory table: {table_name}")
    columns = DIRECTORY_COLUMNS[table_name]
    column_sql = ", ".join(columns)
    pk_columns = DIRECTORY_PK_COLUMNS[table_name]
    conflict_sql = ", ".join(pk_columns)
    update_sql = ", ".join(
        f"{column_name} = EXCLUDED.{column_name}"
        for column_name in columns
        if column_name not in pk_columns
    )
    with con.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = %s", (DIRECTORY_SYNC_LOCK_TIMEOUT,))
        cur.execute("SET LOCAL statement_timeout = %s", (DIRECTORY_SYNC_STATEMENT_TIMEOUT,))
        if rows:
            execute_values(
                cur,
                f"""
                INSERT INTO {table_name} ({column_sql})
                VALUES %s
                ON CONFLICT ({conflict_sql}) DO UPDATE SET
                    {update_sql},
                    updated_at = now()
                """,
                rows,
                page_size=DIRECTORY_SYNC_PAGE_SIZE,
            )
    con.commit()


def update_cursors(
    con: psycopg2.extensions.connection,
    pipeline: str,
    log_id: int = 0,
    egmid: int = 0,
) -> None:
    with con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO elt_state (pipeline, last_log_id, last_egmid)
            VALUES (%s, %s, %s)
            ON CONFLICT (pipeline) DO UPDATE SET
                last_log_id = GREATEST(elt_state.last_log_id, EXCLUDED.last_log_id),
                last_egmid = GREATEST(elt_state.last_egmid, EXCLUDED.last_egmid),
                updated_at = now();
            """,
            (pipeline, log_id, egmid),
        )
    con.commit()
