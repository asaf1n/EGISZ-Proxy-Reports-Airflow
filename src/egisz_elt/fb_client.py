from __future__ import annotations

from typing import Any

from firebird.driver import connect


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
                    "logtext": logtext,
                    "msgtext": msgtext,
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
                CAST(NULL AS INTEGER) AS JID,
                CAST(NULL AS VARCHAR(64)) AS KIND,
                CREATEDATE,
                MSGID,
                REPLYTO,
                DOCUMENTID,
                CAST(NULL AS VARCHAR(8191)) AS MSGTEXT
            FROM EGISZ_MESSAGES
            WHERE EGMID > ?
            ORDER BY EGMID
            ROWS ?
            """,
            (int(after_egmid or 0), int(limit)),
        )
        rows: list[dict[str, Any]] = []
        for egmid, jid, kind, created, msgid, reply_to, document_id, msgtext in cur.fetchall():
            rows.append(
                {
                    "egmid": int(egmid),
                    "jid": int(jid) if jid is not None else None,
                    "kind": kind,
                    "created_at": created.isoformat() if created is not None else None,
                    "msgid": msgid,
                    "reply_to": reply_to,
                    "document_id": document_id,
                    "msgtext": msgtext,
                }
            )
        return rows
    finally:
        cur.close()


def _serialize_egisz_message_rows(rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for egmid, jid, kind, created, msgid, reply_to, document_id, msgtext in rows:
        serialized.append(
            {
                "egmid": int(egmid),
                "jid": int(jid) if jid is not None else None,
                "kind": kind,
                "created_at": created.isoformat() if created is not None else None,
                "msgid": msgid,
                "reply_to": reply_to,
                "document_id": document_id,
                "msgtext": msgtext,
            }
        )
    return serialized


def fetch_egisz_messages_by_identifiers(
    con,
    *,
    msgids: set[str],
    document_ids: set[str],
    chunk_size: int = 500,
) -> list[dict[str, Any]]:
    """Fetch EGISZ_MESSAGES rows referenced by the current EXCHANGELOG batch."""
    identifiers = sorted({value for value in msgids if value})
    documents = sorted({value for value in document_ids if value})
    if not identifiers and not documents:
        return []

    rows: list[dict[str, Any]] = []
    cur = con.cursor()
    try:
        for start in range(0, max(len(identifiers), len(documents), 1), chunk_size):
            msgid_chunk = identifiers[start : start + chunk_size]
            document_chunk = documents[start : start + chunk_size]
            clauses: list[str] = []
            params: list[str] = []
            if msgid_chunk:
                clauses.append(f"MSGID IN ({', '.join('?' for _ in msgid_chunk)})")
                params.extend(msgid_chunk)
            if document_chunk:
                clauses.append(f"DOCUMENTID IN ({', '.join('?' for _ in document_chunk)})")
                params.extend(document_chunk)
            if not clauses:
                continue
            cur.execute(
                f"""
                SELECT
                    EGMID,
                    CAST(NULL AS INTEGER) AS JID,
                    CAST(NULL AS VARCHAR(64)) AS KIND,
                    CREATEDATE,
                    MSGID,
                    REPLYTO,
                    DOCUMENTID,
                    CAST(NULL AS VARCHAR(8191)) AS MSGTEXT
                FROM EGISZ_MESSAGES
                WHERE {' OR '.join(clauses)}
                """,
                tuple(params),
            )
            rows.extend(_serialize_egisz_message_rows(cur.fetchall()))
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
