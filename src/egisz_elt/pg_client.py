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


def get_cursors(con: psycopg2.extensions.connection, pipeline: str) -> dict[str, Any]:
    """Read pipeline cursor state including optional source lower bound."""
    with con.cursor() as cur:
        cur.execute(
            """
            SELECT last_logid, source_min_created_at
            FROM elt_state
            WHERE pipeline = %s
            """,
            (pipeline,),
        )
        row = cur.fetchone()
    if row is None:
        return {"last_logid": 0, "source_min_created_at": None}
    return {
        "last_logid": int(row[0] or 0),
        "source_min_created_at": row[1],
    }


def get_raw_logids(con: psycopg2.extensions.connection) -> set[int]:
    """Return every LOGID already present in exchangelog_raw (for reconcile set-diff)."""
    with con.cursor() as cur:
        cur.execute("SELECT logid FROM exchangelog_raw")
        return {int(row[0]) for row in cur.fetchall()}


def load_raw_logs(con: psycopg2.extensions.connection, rows: list[dict[str, Any]] | list[tuple[Any, ...]]) -> None:
    """Load EXCHANGELOG rows into exchangelog_raw without transforming them in Python."""
    values: list[tuple[Any, ...]] = []
    for row in rows:
        if isinstance(row, dict):
            missing_columns = [column for column in RAW_LOG_COLUMNS if column not in row]
            if missing_columns:
                raise ValueError(f"Raw EXCHANGELOG row is missing required column(s): {', '.join(missing_columns)}")
            normalized_row = dict(row)
            if normalized_row.get("createdate") is None:
                normalized_row["createdate"] = normalized_row.get("logdate")
            values.append(tuple(normalized_row[column] for column in RAW_LOG_COLUMNS))
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
            ON CONFLICT (logid, createdate) DO UPDATE SET
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


def transform_raw_to_facts(
    con: psycopg2.extensions.connection,
    *,
    from_logid: int,
    to_logid: int,
) -> int:
    """Run the database-side ELT transform for the requested LOGID window."""
    with con.cursor() as cur:
        cur.execute(
            "SELECT public.egisz_transform_raw_to_facts(%s, %s)",
            (from_logid, to_logid),
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
) -> None:
    with con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO elt_state (pipeline, last_logid)
            VALUES (%s, %s)
            ON CONFLICT (pipeline) DO UPDATE SET
                last_logid = GREATEST(elt_state.last_logid, EXCLUDED.last_logid),
                updated_at = now();
            """,
            (pipeline, logid),
        )
    con.commit()
