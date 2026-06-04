from __future__ import annotations

import logging
import time
from typing import Any

from firebird.driver import connect

log = logging.getLogger(__name__)


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


def fetch_rows_after_cursor(
    con,
    *,
    source_table: str,
    cursor_column: str,
    after_cursor: Any,
    limit: int,
) -> list[dict[str, Any]]:
    """
    Fetch batch from Firebird using keyset pagination by cursor_column.
    """
    if limit <= 0:
        return []

    # Firebird: ROWS <n> is supported; we interpolate an int limit (validated).
    # Parameter style for firebird-driver is qmark (?).
    # Handle empty cursor: convert to 0 for numeric columns
    cursor_value = after_cursor
    if cursor_value == "" or cursor_value is None:
        cursor_value = 0  # Default for numeric cursor columns
    else:
        # Try to convert to int if it's numeric
        try:
            cursor_value = int(cursor_value)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"Cursor value for {source_table}.{cursor_column} must be numeric") from exc
    
    q = (
        f"SELECT * FROM {source_table} "
        f"WHERE {cursor_column} > ? "
        f"ORDER BY {cursor_column} "
        f"ROWS {int(limit)}"
    )
    cur = con.cursor()
    try:
        cur.execute(q, (cursor_value,))
        cols = [d[0] for d in cur.description]
        out: list[dict[str, Any]] = []
        for row in cur.fetchall():
            out.append({cols[i]: row[i] for i in range(len(cols))})
        return out
    finally:
        cur.close()


def ping_fb(con) -> None:
    cur = con.cursor()
    try:
        cur.execute("SELECT 1 AS ok FROM RDB$DATABASE")
        cur.fetchone()
    finally:
        cur.close()


def fetch_exchangelog_after_cursor(
    con,
    *,
    after_logid: int,
    limit: int,
) -> list[dict[str, Any]]:
    """Fetch an EXCHANGELOG batch for in-process E+L load (not for XCom)."""
    if limit <= 0:
        return []

    cur = con.cursor()
    try:
        query = """
            SELECT
                LOGID,
                LOGDATE,
                CREATEDATE,
                MSGID,
                LOGSTATE,
                LOGTEXT,
                MSGTEXT
            FROM EXCHANGELOG
            WHERE LOGID > ?
            ORDER BY LOGID
            ROWS ?
            """
        cur.execute(query, (int(after_logid or 0), int(limit)))
        rows: list[dict[str, Any]] = []
        for logid, logdate, createdate, msgid, logstate, logtext, msgtext in cur.fetchall():
            rows.append(
                {
                    "logid": int(logid),
                    "logdate": logdate.isoformat() if logdate is not None else None,
                    "createdate": createdate.isoformat() if createdate is not None else None,
                    "msgid": msgid,
                    "logstate": logstate,
                    "logtext": _serialize_firebird_text(logtext),
                    "msgtext": _serialize_firebird_text(msgtext),
                }
            )
        return rows
    finally:
        cur.close()


def fetch_exchangelog_logids_in_band(
    con,
    *,
    low_logid: int,
    high_logid: int,
) -> set[int]:
    """Return EXCHANGELOG LOGIDs in the band ``(low_logid, high_logid]``.

    The proxy journal materializes rows out of LOGID order: async СЭМД callbacks and
    gateway backfills appear *below* an already-advanced watermark, where the forward
    cursor (`LOGID > last_logid`) never re-reads them. Reconcile set-diffs this band — a
    fixed window just below the watermark — against exchangelog_raw to find the gaps. The
    scan is LOGID-bounded (no date), so it stays cheap and bounded regardless of journal
    size; the band must be wide enough to cover the out-of-order span. See CLAUDE.md §2.
    """
    if high_logid <= low_logid:
        return set()

    cur = con.cursor()
    try:
        cur.execute(
            "SELECT LOGID FROM EXCHANGELOG WHERE LOGID > ? AND LOGID <= ?",
            (int(low_logid), int(high_logid)),
        )
        return {int(row[0]) for row in cur.fetchall()}
    finally:
        cur.close()


def fetch_exchangelog_by_logids(
    con,
    logids: list[int] | set[int],
    *,
    chunk_size: int = 1000,
) -> list[dict[str, Any]]:
    """Fetch full EXCHANGELOG rows for an explicit set of LOGIDs (chunked IN-lists).

    Serialization matches fetch_exchangelog_after_cursor so reconciled rows load
    through the same load_raw_logs path.
    """
    ids = [int(value) for value in logids]
    if not ids:
        return []

    cur = con.cursor()
    rows: list[dict[str, Any]] = []
    try:
        for start in range(0, len(ids), chunk_size):
            chunk = ids[start : start + chunk_size]
            placeholders = ", ".join("?" * len(chunk))
            cur.execute(
                "SELECT LOGID, LOGDATE, CREATEDATE, MSGID, LOGSTATE, LOGTEXT, MSGTEXT "
                f"FROM EXCHANGELOG WHERE LOGID IN ({placeholders})",
                tuple(chunk),
            )
            for logid, logdate, createdate, msgid, logstate, logtext, msgtext in cur.fetchall():
                rows.append(
                    {
                        "logid": int(logid),
                        "logdate": logdate.isoformat() if logdate is not None else None,
                        "createdate": createdate.isoformat() if createdate is not None else None,
                        "msgid": msgid,
                        "logstate": logstate,
                        "logtext": _serialize_firebird_text(logtext),
                        "msgtext": _serialize_firebird_text(msgtext),
                    }
                )
        return rows
    finally:
        cur.close()


def fetch_organizations(con) -> list[tuple[Any, ...]]:
    """Fetch organization directory rows from JPERSONS."""
    cur = con.cursor()
    try:
        cur.execute(
            """
            SELECT
                JID,
                JNAME,
                JINN,
                JADDR
            FROM JPERSONS
            WHERE JID IS NOT NULL
            """
        )
        return [tuple(row) for row in cur.fetchall()]
    finally:
        cur.close()


def fetch_licenses(con) -> list[tuple[Any, ...]]:
    """Fetch license/service rows used to resolve clinic and SЭMD kind."""
    cur = con.cursor()
    try:
        cur.execute(
            """
            SELECT
                ID,
                SERVICE_TYPE,
                JID,
                MO_UID,
                MO_DOMEN,
                BDATE,
                FDATE,
                KIND,
                MODIFYDATE
            FROM EGISZ_LICENSES
            WHERE ID IS NOT NULL
            """
        )
        return [tuple(row) for row in cur.fetchall()]
    finally:
        cur.close()
