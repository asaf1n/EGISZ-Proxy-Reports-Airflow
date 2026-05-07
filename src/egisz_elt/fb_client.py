from __future__ import annotations

from typing import Any

from firebird.driver import connect

def connect_fb(conn: Any):
    """Connect to Firebird proxy database using Airflow Connection object."""
    dsn = f"{conn.host}:{conn.schema}" if conn.host else conn.schema
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
        except (ValueError, TypeError):
            pass  # Keep as-is for string cursors
    
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
