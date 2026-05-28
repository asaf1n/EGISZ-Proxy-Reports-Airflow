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
MESSAGE_COLUMNS = ("egmid", "created_at", "msgid", "reply_to", "document_id")
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
            SELECT last_log_id, last_egmid
            FROM elt_state
            WHERE pipeline = %s
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


def load_messages(con: psycopg2.extensions.connection, rows: list[dict[str, Any]]) -> None:
    """Load structured Firebird EGISZ_MESSAGES rows into the staging layer."""
    values: list[tuple[Any, ...]] = []
    for row in rows:
        missing_columns = [column for column in MESSAGE_COLUMNS if column not in row]
        if missing_columns:
            raise ValueError(f"EGISZ_MESSAGES row is missing required column(s): {', '.join(missing_columns)}")
        normalized = dict(row)
        normalized["msgid"] = normalize_message_id(normalized.get("msgid"))
        normalized["reply_to"] = normalize_message_id(normalized.get("reply_to"))
        values.append(tuple(normalized[column] for column in MESSAGE_COLUMNS))

    if not values:
        return

    with con.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO stg_egisz_messages (
                egmid, created_at, msgid, reply_to, document_id,
                msgid_norm, document_id_norm, document_key, reply_to_jid, reply_to_host
            )
            SELECT
                v.egmid,
                v.created_at::timestamptz,
                v.msgid,
                v.reply_to,
                v.document_id,
                public.egisz_normalize_message_id(v.msgid),
                lower(NULLIF(btrim(v.document_id), '')),
                public.egisz_document_key(v.document_id, v.document_id),
                NULLIF(public.egisz_extract_jid_from_endpoint(v.reply_to), '')::integer,
                public.egisz_clean_host(v.reply_to)
            FROM (VALUES %s) AS v(egmid, created_at, msgid, reply_to, document_id)
            ON CONFLICT (egmid) DO UPDATE SET
                created_at = EXCLUDED.created_at,
                msgid = EXCLUDED.msgid,
                reply_to = EXCLUDED.reply_to,
                document_id = EXCLUDED.document_id,
                msgid_norm = EXCLUDED.msgid_norm,
                document_id_norm = EXCLUDED.document_id_norm,
                document_key = EXCLUDED.document_key,
                reply_to_jid = EXCLUDED.reply_to_jid,
                reply_to_host = EXCLUDED.reply_to_host,
                updated_at = now()
            """,
            values,
        )
    con.commit()


def get_related_message_identifiers(
    con: psycopg2.extensions.connection,
    *,
    from_logid: int,
    to_logid: int,
) -> dict[str, set[str]]:
    """Parse the just-loaded EXCHANGELOG batch in PostgreSQL and return related message keys."""
    if to_logid <= from_logid:
        return {"msgids": set(), "document_ids": set()}

    with con.cursor() as cur:
        cur.execute(
            """
            WITH parsed AS (
                SELECT
                    public.egisz_xml_text(msgtext, 'messageId') AS message_id,
                    public.egisz_xml_text(msgtext, 'relatesToMessage') AS relates_to_message,
                    public.egisz_xml_text(msgtext, 'relatesTo') AS relates_to,
                    public.egisz_xml_text(logtext, 'messageId') AS log_message_id,
                    public.egisz_xml_text(logtext, 'relatesToMessage') AS log_relates_to_message,
                    public.egisz_xml_text(logtext, 'relatesTo') AS log_relates_to,
                    public.egisz_xml_text(msgtext, 'localUid') AS local_uid,
                    public.egisz_xml_text(msgtext, 'DOCUMENTID') AS document_id,
                    public.egisz_xml_text(logtext, 'localUid') AS log_local_uid,
                    public.egisz_xml_text(logtext, 'DOCUMENTID') AS log_document_id,
                    msgid
                FROM public.exchangelog_raw
                WHERE logid > %s
                  AND logid <= %s
            ),
            msgids AS (
                SELECT public.egisz_normalize_message_id(v) AS value
                FROM parsed
                CROSS JOIN LATERAL (
                    VALUES (msgid), (message_id), (relates_to_message), (relates_to),
                           (log_message_id), (log_relates_to_message), (log_relates_to)
                ) AS x(v)
            ),
            document_ids AS (
                SELECT NULLIF(btrim(v), '') AS value
                FROM parsed
                CROSS JOIN LATERAL (VALUES (local_uid), (document_id), (log_local_uid), (log_document_id)) AS x(v)
            )
            SELECT 'msgid' AS key_type, value
            FROM msgids
            WHERE value IS NOT NULL
            UNION ALL
            SELECT 'document_id' AS key_type, value
            FROM document_ids
            WHERE value IS NOT NULL
            """,
            (from_logid, to_logid),
        )
        rows = cur.fetchall()

    related: dict[str, set[str]] = {"msgids": set(), "document_ids": set()}
    for key_type, value in rows:
        if key_type == "msgid":
            related["msgids"].add(str(value))
        elif key_type == "document_id":
            related["document_ids"].add(str(value))
    return related


def transform_raw_to_facts(
    con: psycopg2.extensions.connection,
    *,
    from_logid: int,
    to_logid: int,
    from_egmid: int = 0,
    to_egmid: int = 0,
) -> int:
    """Run the database-side ELT transform and refresh EGISZ materialized views if present."""
    with con.cursor() as cur:
        cur.execute(
            "SELECT public.egisz_transform_raw_to_facts(%s, %s, %s, %s)",
            (from_logid, to_logid, from_egmid, to_egmid),
        )
        transformed = int(cur.fetchone()[0] or 0)
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
    logid: int = 0,
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
            (pipeline, logid, egmid),
        )
    con.commit()
