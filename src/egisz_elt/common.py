from __future__ import annotations

import logging
from typing import Any, TypedDict

import psycopg2
from firebird.driver import connect
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

PIPELINE = "egisz"
DWH_CONN_ID = "dwh_egisz_pg"
PROXY_CONN_ID = "proxy_egisz_fb"

RAW_LOG_COLUMNS = ("logid", "logdate", "createdate", "msgid", "logstate", "logtext", "msgtext")

# Serialized EXCHANGELOG row shape shared by forward fetch and reconcile fetch — keeps both
# paths feeding load_raw_logs through one column contract. See README.md §«Источник».
EXCHANGELOG_SELECT_COLUMNS = ("LOGID", "LOGDATE", "CREATEDATE", "MSGID", "LOGSTATE", "LOGTEXT", "MSGTEXT")


class BatchMetadata(TypedDict):
    count: int
    last_logid: int
    cursor_logid: int


class PipelineBatchInfo(BatchMetadata, total=False):
    transformed: int


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


def connect_fb(conn: Any):
    """Connect to Firebird proxy database using Airflow Connection object."""
    if conn.host and conn.port:
        dsn = f"{conn.host}/{conn.port}:{conn.schema}"
    elif conn.host:
        dsn = f"{conn.host}:{conn.schema}"
    else:
        dsn = conn.schema
    charset = conn.extra_dejson.get("charset", "UTF8") if conn.extra_dejson else "UTF8"
    return connect(database=dsn, user=conn.login, password=conn.password, charset=charset)


def _serialize_firebird_text(value: Any) -> Any:
    """Convert Firebird BLOB/text reader values into plain Python strings."""
    if value is None or isinstance(value, str):
        return value
    read = getattr(value, "read", None)
    if callable(read):
        data = read()
        if isinstance(data, bytes):
            return data.decode("utf-8", errors="replace")
        if data is None:
            return None
        return str(data)
    return value


def serialize_exchangelog_row(
    logid: Any,
    logdate: Any,
    createdate: Any,
    msgid: Any,
    logstate: Any,
    logtext: Any,
    msgtext: Any,
) -> dict[str, Any]:
    """Serialize one EXCHANGELOG tuple into the metadata-only dict load_raw_logs consumes."""
    return {
        "logid": int(logid),
        "logdate": logdate.isoformat() if logdate is not None else None,
        "createdate": createdate.isoformat() if createdate is not None else None,
        "msgid": msgid,
        "logstate": logstate,
        "logtext": _serialize_firebird_text(logtext),
        "msgtext": _serialize_firebird_text(msgtext),
    }


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


def get_cursors(con: psycopg2.extensions.connection, pipeline: str) -> dict[str, Any]:
    """Read pipeline watermark state (``last_logid``)."""
    with con.cursor() as cur:
        cur.execute(
            "SELECT last_logid FROM elt_state WHERE pipeline = %s",
            (pipeline,),
        )
        row = cur.fetchone()
    if row is None:
        return {"last_logid": 0}
    return {"last_logid": int(row[0] or 0)}


def update_cursors(
    con: psycopg2.extensions.connection,
    pipeline: str,
    logid: int = 0,
) -> None:
    """Advance the watermark through ``GREATEST`` — never rolls back. Only the extract DAG writes here."""
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


def reconcile_enriched_ui(con: psycopg2.extensions.connection) -> int:
    """Refresh enriched mart rows that drifted from ``v_egisz_documents_enriched_src``.

    Covers dimension sync (clinic names), status changes on late callbacks already in facts,
    and any other display fields derived from facts + reference tables.
    """
    with con.cursor() as cur:
        cur.execute("SELECT public.egisz_reconcile_enriched_ui()")
        refreshed = int(cur.fetchone()[0] or 0)
    con.commit()
    return refreshed
