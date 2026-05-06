from __future__ import annotations

from typing import Any, Iterable

from firebird.driver import connect

from proxy_reports_etl.config import FirebirdConfig


def connect_fb(cfg: FirebirdConfig):
    # firebird-driver uses embedded/remote client depending on DSN;
    # client library presence is handled by the container image (libfbclient).
    return connect(database=cfg.dsn, user=cfg.user, password=cfg.password, charset=cfg.charset)


def fetch_rows_after_cursor(
    con,
    *,
    source_sql: str,
    cursor_column: str,
    after_cursor: Any,
    limit: int,
) -> list[dict[str, Any]]:
    """
    Fetch batch from Firebird using keyset pagination by cursor_column.

    We wrap the provided SQL as a subquery, so the user can pass any SELECT.
    """
    if limit <= 0:
        return []

    # Firebird: ROWS <n> is supported; we interpolate an int limit (validated).
    # Parameter style for firebird-driver is qmark (?).
    q = (
        "SELECT * FROM ("
        + source_sql
        + f") q WHERE q.{cursor_column} > ? ORDER BY q.{cursor_column} ROWS {int(limit)}"
    )
    cur = con.cursor()
    try:
        cur.execute(q, (after_cursor,))
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
