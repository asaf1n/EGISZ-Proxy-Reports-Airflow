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
DIRECTORY_SYNC_LOCK_TIMEOUT = "15s"
DIRECTORY_SYNC_STATEMENT_TIMEOUT = "5min"
DIRECTORY_SYNC_PAGE_SIZE = 1000


def fetch_organizations(con: Any) -> list[tuple[Any, ...]]:
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


def fetch_licenses(con: Any) -> list[tuple[Any, ...]]:
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
