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
    after_log_id: int,
    limit: int,
) -> list[dict[str, Any]]:
    """Fetch a JSON-serializable EXCHANGELOG batch for Airflow XComs."""
    if limit <= 0:
        return []

    cur = con.cursor()
    try:
        cur.execute(
            """
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
            """,
            (int(after_log_id or 0), int(limit)),
        )
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


def fetch_egisz_messages_after_cursor(
    con,
    *,
    after_egmid: int,
    limit: int,
) -> list[dict[str, Any]]:
    """Fetch a JSON-serializable EGISZ_MESSAGES batch for Airflow XComs."""
    if limit <= 0:
        return []

    cur = con.cursor()
    try:
        cur.execute(
            """
            SELECT
                EGMID,
                CREATEDATE,
                MSGID,
                REPLYTO,
                DOCUMENTID
            FROM EGISZ_MESSAGES
            WHERE EGMID > ?
            ORDER BY EGMID
            ROWS ?
            """,
            (int(after_egmid or 0), int(limit)),
        )
        return _serialize_egisz_message_rows(cur.fetchall())
    finally:
        cur.close()


def _serialize_egisz_message_rows(rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for egmid, created, msgid, reply_to, document_id in rows:
        serialized.append(
            {
                "egmid": int(egmid),
                "created_at": created.isoformat() if created is not None else None,
                "msgid": msgid,
                "reply_to": reply_to,
                "document_id": document_id,
            }
        )
    return serialized


def fetch_egisz_messages_by_identifiers(
    con,
    *,
    msgids: set[str],
    document_ids: set[str],
    chunk_size: int = 2000,
) -> list[dict[str, Any]]:
    """Fetch EGISZ_MESSAGES rows referenced by the current EXCHANGELOG batch.

    Прежняя реализация чанковала параллельно msgids и document_ids в одних
    запросах с OR и итерировала по max(len) — при асимметричных множествах это
    давало 2-4× избыточных round-trip'ов к Firebird. Теперь множества
    обрабатываются раздельно крупными IN-чанками (Firebird 5 спокойно держит
    несколько тысяч).
    """
    identifiers = sorted({value for value in msgids if value})
    documents = sorted({value for value in document_ids if value})
    if not identifiers and not documents:
        return []

    started_at = time.monotonic()
    by_egmid: dict[int, dict[str, Any]] = {}
    round_trips = 0
    cur = con.cursor()
    try:
        for column, values in (("MSGID", identifiers), ("DOCUMENTID", documents)):
            for start in range(0, len(values), chunk_size):
                chunk = values[start : start + chunk_size]
                if not chunk:
                    continue
                placeholders = ", ".join("?" for _ in chunk)
                cur.execute(
                    f"""
                    SELECT
                        EGMID,
                        CREATEDATE,
                        MSGID,
                        REPLYTO,
                        DOCUMENTID
                    FROM EGISZ_MESSAGES
                    WHERE {column} IN ({placeholders})
                    """,
                    tuple(chunk),
                )
                round_trips += 1
                for row in _serialize_egisz_message_rows(cur.fetchall()):
                    by_egmid[int(row["egmid"])] = row
        log.info(
            "fetch_egisz_messages_by_identifiers: msgids=%s, document_ids=%s, "
            "round_trips=%s, unique_rows=%s in %.2fs.",
            len(identifiers),
            len(documents),
            round_trips,
            len(by_egmid),
            time.monotonic() - started_at,
        )
        return list(by_egmid.values())
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
