"""Самодостаточный DAG: EXCHANGELOG → exchangelog_raw → факты DWH → витрины динамики.

Канонический исходник — этот файл: он разворачивается на целевые контуры как есть,
без установки дополнительных пакетов. Общие функции (подключения, watermark, витрины)
сознательно продублированы в соседних egisz_*_dag.py; идентичность копий контролирует
tests/test_dag_selfcontainment.py — правки общих функций вносить синхронно во все файлы.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any, TypedDict

import psycopg2
from airflow.sdk import BaseHook, Variable, dag, task
from firebird.driver import connect
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

PIPELINE = "egisz"
DWH_CONN_ID = "dwh_egisz_pg"
PROXY_CONN_ID = "proxy_egisz_fb"
DWH_POOL = "dwh_postgres"

RAW_LOG_COLUMNS = ("logid", "logdate", "createdate", "msgid", "logstate", "logtext", "msgtext")

# Keep in sync with k8s/airflow/egisz-variables.json (UI import / up.ps1 provisioning).
DEFAULTS: dict[str, str | int] = {
    "extract_schedule": "*/5 * * * *",
    "extract_raw_rows": 2000,
    "extract_raw_rounds": 3,
    "transform_rows": 5000,
    "transform_rounds": 6,
    "dimensions_schedule": "@hourly",
    "reconcile_schedule": "@daily",
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


def get_int(key: str) -> int:
    return int(_variable_or_default(key))


class BatchMetadata(TypedDict):
    count: int
    last_logid: int
    cursor_logid: int


class PipelineBatchInfo(BatchMetadata, total=False):
    transformed: int


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


def fetch_exchangelog_after_cursor(
    con: Any,
    *,
    after_logid: int,
    limit: int,
) -> list[dict[str, Any]]:
    """Fetch EXCHANGELOG rows via keyset pagination by LOGID.

    Firebird supports ``WHERE LOGID > ? ORDER BY LOGID ROWS ?``; ``LIMIT/OFFSET`` is not used on
    this dialect. See README.md §«Источник».
    """
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
        return [serialize_exchangelog_row(*row) for row in cur.fetchall()]
    finally:
        cur.close()


def _analyze_exchangelog_raw(pg_conn: psycopg2.extensions.connection) -> None:
    run_analyze(pg_conn, "ANALYZE public.exchangelog_raw")


def _analyze_exchangelog_documents(pg_conn: psycopg2.extensions.connection) -> None:
    run_analyze(
        pg_conn,
        "ANALYZE public.transactions",
        "ANALYZE public.documents",
        "ANALYZE public.document_attributes",
    )


def extract_exchangelog_batch(
    pg_conn: psycopg2.extensions.connection,
    fb_conn: Any,
    *,
    raw_rows: int,
    raw_rounds: int,
) -> BatchMetadata:
    """EXCHANGELOG → exchangelog_raw."""
    last_logid = int(get_cursors(pg_conn, PIPELINE).get("last_logid", 0))
    cursor_logid = last_logid
    total_loaded = 0
    rounds = 0

    pending_rows, pending_max = pending_transform_tail(pg_conn, last_logid)
    if pending_rows > 0:
        log.info(
            "%s row(s) in exchangelog_raw above watermark LOGID=%s; deferring EXCHANGELOG fetch.",
            pending_rows,
            last_logid,
        )
        return {
            "count": 0,
            "last_logid": last_logid,
            "cursor_logid": pending_max,
        }

    while rounds < raw_rounds:
        started_at = time.monotonic()
        log_rows = fetch_exchangelog_after_cursor(
            fb_conn,
            after_logid=cursor_logid,
            limit=raw_rows,
        )
        log.info(
            "Fetched %s EXCHANGELOG row(s) after LOGID=%s in %.2fs (round %s).",
            len(log_rows),
            cursor_logid,
            time.monotonic() - started_at,
            rounds + 1,
        )

        if not log_rows:
            break

        load_raw_logs(pg_conn, log_rows)
        total_loaded += len(log_rows)
        cursor_logid = max(int(row["logid"]) for row in log_rows)
        rounds += 1

        if len(log_rows) < raw_rows:
            break

    if total_loaded > 0:
        _analyze_exchangelog_raw(pg_conn)
        log.info(
            "ANALYZE done for exchangelog_raw after %s row(s) in %s round(s).",
            total_loaded,
            rounds,
        )

    _, pending_max = pending_transform_tail(pg_conn, last_logid)
    cursor_logid = max(cursor_logid, pending_max)
    log.info(
        "Extract complete: %s row(s), exchangelog_raw tail LOGID=%s (watermark=%s).",
        total_loaded,
        cursor_logid,
        last_logid,
    )
    return {
        "count": total_loaded,
        "last_logid": last_logid,
        "cursor_logid": cursor_logid,
    }


def transform_exchangelog_batch(
    pg_conn: psycopg2.extensions.connection,
    load_info: BatchMetadata,
    *,
    transform_rows: int,
    transform_rounds: int,
) -> PipelineBatchInfo:
    """exchangelog_raw → documents/transactions; advance elt_state watermark."""
    watermark = int(load_info.get("last_logid", 0))
    tail_logid = int(load_info.get("cursor_logid", watermark))
    if tail_logid <= watermark:
        log.info("No exchangelog_raw above watermark LOGID=%s; skipping transform.", watermark)
        return {**load_info, "transformed": 0}

    if int(load_info.get("count", 0)) == 0:
        log.info(
            "No new EXCHANGELOG rows; transforming exchangelog_raw up to LOGID=%s.",
            tail_logid,
        )

    total_transformed = 0
    for iteration in range(transform_rounds):
        pending_rows, tail_logid = pending_transform_tail(pg_conn, watermark)
        if pending_rows == 0:
            log.info("exchangelog_raw cleared above watermark LOGID=%s.", watermark)
            break

        to_logid = bounded_transform_to_logid(
            pg_conn,
            last_logid=watermark,
            cursor_logid=tail_logid,
            raw_rows=transform_rows,
        )
        if to_logid <= watermark:
            break

        started_at = time.monotonic()
        transformed = transform_raw_to_facts(
            pg_conn,
            from_logid=watermark,
            to_logid=to_logid,
        )
        elapsed = time.monotonic() - started_at
        log.info(
            "Transformed %s row(s) for LOGID (%s, %s] in %.1fs (iteration %s).",
            transformed,
            watermark,
            to_logid,
            elapsed,
            iteration + 1,
        )
        total_transformed += transformed

        update_cursors(pg_conn, PIPELINE, logid=to_logid)
        watermark = to_logid

        remaining, remaining_tail = pending_transform_tail(pg_conn, watermark)
        if remaining > 0:
            log.info(
                "%s row(s) remain in exchangelog_raw above watermark LOGID=%s (tail=%s).",
                remaining,
                watermark,
                remaining_tail,
            )
        else:
            log.info("Updated %s watermark to LOGID=%s.", PIPELINE, watermark)

    if total_transformed > 0:
        _analyze_exchangelog_documents(pg_conn)
        # Витрина разбивки ошибок — matview; обновляем после смены фактов, чтобы
        # карточки «Анализ ошибок» отражали свежие документы (свежесть = у фактов).
        refresh_error_breakdown(pg_conn)

    return {**load_info, "last_logid": watermark, "transformed": total_transformed}


@dag(
    dag_id="egisz_extract_dag",
    schedule=get_str("extract_schedule"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "extract"],
)
def egisz_extract_pipeline() -> None:
    @task
    def extract_exchangelog() -> BatchMetadata:
        pg_conn = _dwh_connection()
        fb_conn = _proxy_connection()
        try:
            return extract_exchangelog_batch(
                pg_conn,
                fb_conn,
                raw_rows=get_int("extract_raw_rows"),
                raw_rounds=get_int("extract_raw_rounds"),
            )
        finally:
            fb_conn.close()
            pg_conn.close()

    # Ретраи гасят транзиентный DeadlockDetected: maintenance-прогоны схемы (полный
    # reconcile_document_attributes / recompute в 90-й части) пересекаются с 5-минутным
    # батчем по блокировкам documents/document_attributes; откат + повтор безопасны
    # (transform идемпотентен, watermark двигается только после успеха).
    @task(pool=DWH_POOL, retries=2, retry_delay=timedelta(minutes=1))
    def transform_exchangelog(load_info: BatchMetadata) -> PipelineBatchInfo:
        pg_conn = _dwh_connection()
        try:
            return transform_exchangelog_batch(
                pg_conn,
                load_info,
                transform_rows=get_int("transform_rows"),
                transform_rounds=get_int("transform_rounds"),
            )
        finally:
            pg_conn.close()

    # Витрины динамики отделены от transform: их refresh не должен ретраить
    # парсинг батча, а гейт transformed > 0 повторяет условие inline-refresh
    # rpt_error_breakdown (см. transform_exchangelog_batch). task_id задан явно:
    # локальное имя не должно затенять одноимённую модульную функцию.
    @task(task_id="refresh_report_marts", pool=DWH_POOL, retries=2, retry_delay=timedelta(minutes=1))
    def refresh_report_marts_task(batch: PipelineBatchInfo) -> None:
        if int(batch.get("transformed", 0) or 0) <= 0:
            return
        pg_conn = _dwh_connection()
        try:
            refresh_report_marts(pg_conn)
        finally:
            pg_conn.close()

    extracted = extract_exchangelog()
    transformed = transform_exchangelog(extracted)
    refresh_report_marts_task(transformed)


egisz_extract_pipeline()
