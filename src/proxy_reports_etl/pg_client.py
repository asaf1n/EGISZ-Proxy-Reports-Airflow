from __future__ import annotations

from typing import Any
import re

import psycopg2
from psycopg2.extras import Json, execute_values

from proxy_reports_etl.config import PostgresConfig


_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str, *, what: str) -> str:
    s = str(name or "").strip()
    if not s or not _IDENT_RE.fullmatch(s):
        raise ValueError(f"Invalid {what}: {name!r} (allowed: letters/digits/underscore, must not start with digit)")
    return s


def connect_pg(cfg: PostgresConfig):
    con = psycopg2.connect(cfg.dsn, connect_timeout=10)
    con.autocommit = False
    return con


def ensure_tables(con, *, target_table: str) -> None:
    """
    Create minimal schema:
      - etl_state: stores last_cursor per pipeline
      - target_table: raw rows as jsonb keyed by cursor_value
    """
    tbl = _validate_identifier(target_table, what="target table")
    with con.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS etl_state (
              pipeline text PRIMARY KEY,
              last_cursor text NOT NULL DEFAULT '',
              updated_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
              cursor_value text PRIMARY KEY,
              row_data jsonb NOT NULL,
              loaded_at timestamptz NOT NULL DEFAULT now()
            );
            """
        )
    con.commit()


def get_last_cursor(con, *, pipeline: str) -> str:
    with con.cursor() as cur:
        cur.execute("SELECT last_cursor FROM etl_state WHERE pipeline = %s", (pipeline,))
        row = cur.fetchone()
    if not row:
        return ""
    return str(row[0] or "")


def set_last_cursor(con, *, pipeline: str, last_cursor: str) -> None:
    with con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_state (pipeline, last_cursor, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (pipeline) DO UPDATE
            SET last_cursor = EXCLUDED.last_cursor, updated_at = now();
            """,
            (pipeline, last_cursor),
        )
    con.commit()


def upsert_raw_rows(con, *, target_table: str, rows: list[dict[str, Any]], cursor_column: str) -> str | None:
    """
    Upsert rows into target_table by cursor_value; store entire row as jsonb.
    Returns max cursor_value (as text) seen in the batch.
    """
    if not rows:
        return None
    tbl = _validate_identifier(target_table, what="target table")
    # Determine max cursor as string, preserving natural ordering for numbers if possible.
    max_cursor: str | None = None
    tuples: list[tuple[str, Any]] = []
    for r in rows:
        if cursor_column not in r:
            raise ValueError(f"Cursor column {cursor_column!r} missing in row")
        c = r[cursor_column]
        c_text = "" if c is None else str(c).strip()
        if not c_text:
            continue
        tuples.append((c_text, Json(r)))
        if max_cursor is None:
            max_cursor = c_text
        else:
            # Try numeric compare first
            try:
                if int(c_text) > int(max_cursor):
                    max_cursor = c_text
            except Exception:
                if c_text > max_cursor:
                    max_cursor = c_text

    if not tuples:
        return None
    with con.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {tbl} (cursor_value, row_data)
            VALUES %s
            ON CONFLICT (cursor_value) DO UPDATE
            SET row_data = EXCLUDED.row_data, loaded_at = now();
            """,
            tuples,
            template="(%s, %s)",
        )
    con.commit()
    return max_cursor


def ping_pg(con) -> None:
    with con.cursor() as cur:
        cur.execute("SELECT 1")
        cur.fetchone()
