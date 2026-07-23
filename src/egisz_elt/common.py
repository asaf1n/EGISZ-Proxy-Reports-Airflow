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


def pending_transform_tail(
    con: psycopg2.extensions.connection,
    last_logid: int,
) -> tuple[int, int]:
    """Return (row_count, max_logid) of raw rows above the extract watermark."""
    with con.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::bigint, COALESCE(MAX(logid), %s)::bigint
            FROM public.exchangelog_raw
            WHERE logid > %s
            """,
            (last_logid, last_logid),
        )
        pending_rows, pending_max = cur.fetchone()
    return int(pending_rows or 0), int(pending_max or last_logid)


def bounded_transform_to_logid(
    con: psycopg2.extensions.connection,
    *,
    last_logid: int,
    cursor_logid: int,
    raw_rows: int,
) -> int:
    """Upper LOGID bound for the next transform chunk (at most ``raw_rows`` raw rows)."""
    if cursor_logid <= last_logid or raw_rows <= 0:
        return last_logid
    with con.cursor() as cur:
        cur.execute(
            """
            SELECT COALESCE(MAX(logid), %s)::bigint
            FROM (
                SELECT logid
                FROM public.exchangelog_raw
                WHERE logid > %s AND logid <= %s
                ORDER BY logid
                LIMIT %s
            ) bounded
            """,
            (last_logid, last_logid, cursor_logid, raw_rows),
        )
        row = cur.fetchone()
    return int(row[0] if row else last_logid)


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
    lookback_logids: int = 0,
) -> int:
    """Run the database-side ELT transform for the requested LOGID window.

    ``lookback_logids=0`` lets SQL derive lookback from ``to_logid - from_logid``.
    Reconcile passes the window low LOGID explicitly for prefix chain resolution.
    """
    with con.cursor() as cur:
        cur.execute(
            "SELECT public.transform_raw_to_facts(%s, %s, %s)",
            (from_logid, to_logid, lookback_logids),
        )
        transformed = int(cur.fetchone()[0] or 0)
    con.commit()
    return transformed


def reconcile_document_attributes_ui(con: psycopg2.extensions.connection) -> int:
    """Refresh document_attributes rows that drifted from documents + dimensions."""
    with con.cursor() as cur:
        cur.execute("SELECT public.reconcile_document_attributes_ui()")
        refreshed = int(cur.fetchone()[0] or 0)
    con.commit()
    return refreshed


def _refresh_matview(con: psycopg2.extensions.connection, qualified_name: str) -> None:
    """Refresh a materialized view after facts change.

    CONCURRENTLY (needs the unique index + a populated matview) keeps dashboard reads
    unblocked during the ~seconds-long rebuild; falls back to a plain refresh if the
    matview was never populated. Runs in autocommit — REFRESH CONCURRENTLY cannot run
    inside a transaction block.
    """
    con.commit()
    previous_autocommit = con.autocommit
    con.set_session(autocommit=True)
    try:
        with con.cursor() as cur:
            try:
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {qualified_name}")
            except psycopg2.Error as exc:
                log.warning(
                    "CONCURRENTLY refresh of %s failed (%s); falling back to plain refresh",
                    qualified_name,
                    exc,
                )
                cur.execute(f"REFRESH MATERIALIZED VIEW {qualified_name}")
    finally:
        con.set_session(autocommit=previous_autocommit)


def refresh_error_breakdown(con: psycopg2.extensions.connection) -> None:
    """Refresh the rpt_error_breakdown materialized view after facts change."""
    _refresh_matview(con, "public.rpt_error_breakdown")


def refresh_weekly_reports(con: psycopg2.extensions.connection) -> None:
    """Refresh the weekly dynamics marts.

    rpt_error_breakdown_weekly reads rpt_error_breakdown — call AFTER
    refresh_error_breakdown().
    """
    _refresh_matview(con, "public.rpt_documents_weekly")
    _refresh_matview(con, "public.rpt_error_breakdown_weekly")


def run_analyze(con: psycopg2.extensions.connection, *statements: str) -> None:
    """Run ANALYZE outside a transaction (PostgreSQL forbids ANALYZE inside one).

    Read-only SELECTs leave psycopg2 in an open transaction; commit first so
    set_session(autocommit=True) is legal.
    """
    if not statements:
        return
    con.commit()
    previous_autocommit = con.autocommit
    con.set_session(autocommit=True)
    try:
        with con.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
    finally:
        con.set_session(autocommit=previous_autocommit)
