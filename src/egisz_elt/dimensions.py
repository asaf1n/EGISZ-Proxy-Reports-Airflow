from __future__ import annotations

import logging
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

ALLOWED_SYNC_TABLES = {"dim_organizations", "dim_licenses"}
DIRECTORY_COLUMNS = {
    "dim_organizations": ("jid", "name", "inn", "address", "fir_oid"),
    "dim_licenses": ("id", "service_type", "jid", "mo_uid", "mo_domen", "bdate", "fdate", "kind", "modifydate"),
}
DIRECTORY_PK_COLUMNS = {
    "dim_organizations": ("jid",),
    "dim_licenses": ("id",),
}
DIRECTORY_SYNC_LOCK_TIMEOUT = "15s"
DIRECTORY_SYNC_STATEMENT_TIMEOUT = "5min"
DIRECTORY_SYNC_PAGE_SIZE = 5000


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
                JADDR,
                FIR_OID
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


def sync_directories(
    con: psycopg2.extensions.connection,
    organization_rows: list[tuple[Any, ...]],
    license_rows: list[tuple[Any, ...]],
) -> int:
    """Upsert both dimension tables in one transaction."""
    changed = 0
    changed += sync_directory(con, "dim_organizations", organization_rows, commit=False)
    changed += sync_directory(con, "dim_licenses", license_rows, commit=False)
    con.commit()
    return changed


def sync_directory(
    con: psycopg2.extensions.connection,
    table_name: str,
    rows: list[tuple[Any, ...]],
    *,
    commit: bool = True,
) -> int:
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
    change_predicate = " OR ".join(
        f"{table_name}.{column_name} IS DISTINCT FROM EXCLUDED.{column_name}"
        for column_name in columns
        if column_name not in pk_columns
    )
    with con.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = %s", (DIRECTORY_SYNC_LOCK_TIMEOUT,))
        cur.execute("SET LOCAL statement_timeout = %s", (DIRECTORY_SYNC_STATEMENT_TIMEOUT,))
        if not rows:
            return 0

        execute_values(
            cur,
            f"""
            INSERT INTO {table_name} ({column_sql})
            VALUES %s
            ON CONFLICT ({conflict_sql}) DO UPDATE SET
                {update_sql},
                updated_at = now()
            WHERE {change_predicate}
            """,
            rows,
            page_size=DIRECTORY_SYNC_PAGE_SIZE,
        )
        changed = cur.rowcount
    if commit:
        con.commit()
    return int(changed or 0)
