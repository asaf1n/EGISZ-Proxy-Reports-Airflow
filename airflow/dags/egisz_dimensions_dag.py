"""Самодостаточный DAG: справочники прокси (JPERSONS, EGISZ_LICENSES) → dim_* DWH.

Канонический исходник — этот файл: он разворачивается на целевые контуры как есть,
без установки дополнительных пакетов. Общие функции (подключения, watermark, витрины)
сознательно продублированы в соседних egisz_*_dag.py; идентичность копий контролирует
tests/test_dag_selfcontainment.py — правки общих функций вносить синхронно во все файлы.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import psycopg2
from airflow.sdk import BaseHook, Variable, dag, task
from firebird.driver import connect
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

DWH_CONN_ID = "dwh_egisz_pg"
PROXY_CONN_ID = "proxy_egisz_fb"
DWH_POOL = "dwh_postgres"

# Keep in sync with k8s/airflow/egisz-variables.json (UI import / up.ps1 provisioning).
DEFAULTS: dict[str, str | int] = {
    "extract_schedule": "*/5 * * * *",
    "extract_raw_rows": 1000,
    "extract_raw_rounds": 3,
    "transform_rows": 3000,
    "transform_rounds": 6,
    "dimensions_schedule": "@hourly",
    "reconcile_schedule": "@hourly",
    "reconcile_lookback_days": 30,
    "reconcile_max_logids": 20000000,
}


def _variable_or_default(key: str) -> str | int:
    """Read an Airflow Variable, falling back to DEFAULTS when the metadata DB is unreachable.

    Настройки читаются при импорте DAG-файла (schedule), а файл импортируют и тесты,
    и парсер вне кластера — импорт не должен требовать ни метабазы Airflow, ни Connections.
    """
    default = DEFAULTS[key]
    try:
        return Variable.get(key, default_var=default)
    except Exception:
        log.warning("Airflow Variable %r unavailable; using default %r.", key, default)
        return default


def get_str(key: str) -> str:
    return str(_variable_or_default(key))


def connect_pg(conn_params: Any) -> psycopg2.extensions.connection:
    try:
        if isinstance(conn_params, str):
            return psycopg2.connect(conn_params)
        return psycopg2.connect(
            host=conn_params.host,
            port=conn_params.port,
            user=conn_params.login,
            password=conn_params.password,
            database=conn_params.schema,
        )
    except UnicodeDecodeError as exc:
        # Русифицированный PostgreSQL на Windows отвечает на отказ подключения текстом
        # в кодировке сервера (cp1251), а psycopg2 ждёт UTF-8 — реальная причина отказа
        # (неверный пароль/база, правило pg_hba) прячется за UnicodeDecodeError.
        detail = bytes(exc.object).decode("cp1251", errors="replace")
        raise psycopg2.OperationalError(
            f"PostgreSQL rejected the connection; server message: {detail}"
        ) from exc


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


# Витрины отчётного слоя динамики (вкладки «Динамика по неделям/месяцам»).
REPORT_MARTS = (
    "public.rpt_documents_weekly",
    "public.rpt_error_breakdown_weekly",
    "public.rpt_documents_monthly",
    "public.rpt_error_breakdown_monthly",
)


def refresh_report_marts(con: psycopg2.extensions.connection) -> None:
    """Refresh the periodic reporting marts.

    rpt_error_breakdown_* читают rpt_error_breakdown — вызывать ПОСЛЕ
    refresh_error_breakdown().
    """
    for matview in REPORT_MARTS:
        _refresh_matview(con, matview)


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


def _dwh_connection():
    return connect_pg(BaseHook.get_connection(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(BaseHook.get_connection(PROXY_CONN_ID))


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


@dag(
    dag_id="egisz_dimensions_dag",
    schedule=get_str("dimensions_schedule"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "dimensions"],
)
def egisz_dimensions_pipeline() -> None:
    # Ретраи гасят транзиентный DeadlockDetected: maintenance-прогоны схемы пересекаются
    # с reconcile_document_attributes_ui по блокировкам document_attributes; upsert
    # справочников и reconcile идемпотентны — повтор безопасен.
    @task(pool=DWH_POOL, retries=2, retry_delay=timedelta(minutes=1))
    def sync_dimensions() -> dict[str, int]:
        """Sync proxy directories into DWH; refresh enriched mart only when rows changed.

        ``reconcile_document_attributes_ui()`` scans the archive for display drift (clinic names,
        license-resolved JID). It is skipped when UPSERT touched zero rows.
        """
        fb_conn = _proxy_connection()
        pg_conn = _dwh_connection()
        try:
            organization_rows = fetch_organizations(fb_conn)
            license_rows = fetch_licenses(fb_conn)
            log.info(
                "Fetched %s organization and %s license row(s) from proxy.",
                len(organization_rows),
                len(license_rows),
            )

            changed = sync_directories(pg_conn, organization_rows, license_rows)
            refreshed = 0
            if changed <= 0:
                log.info("Dimension directories unchanged; skipping enriched mart reconcile.")
            else:
                log.info("%s dimension row(s) changed; reconciling enriched mart.", changed)
                refreshed = reconcile_document_attributes_ui(pg_conn)
                if refreshed > 0:
                    run_analyze(pg_conn, "ANALYZE public.document_attributes")
                    # Имена клиник в matview разбивки ошибок могли измениться.
                    refresh_error_breakdown(pg_conn)
                    refresh_report_marts(pg_conn)
                    log.info("Enriched mart reconcile refreshed %s row(s).", refreshed)
                else:
                    log.info("Enriched mart reconcile: no drift detected.")

            return {
                "organizations": len(organization_rows),
                "licenses": len(license_rows),
                "changed": changed,
                "refreshed": refreshed,
            }
        finally:
            fb_conn.close()
            pg_conn.close()

    sync_dimensions()


egisz_dimensions_pipeline()
