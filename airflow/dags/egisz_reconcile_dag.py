"""Самодостаточный DAG: полная сверка source↔raw по LOGID и дозагрузка опоздавших строк.

Канонический исходник — этот файл: он разворачивается на целевые контуры как есть,
без установки дополнительных пакетов. Общие функции (подключения, watermark, витрины)
сознательно продублированы в соседних egisz_*_dag.py; идентичность копий контролирует
tests/test_dag_selfcontainment.py — правки общих функций вносить синхронно во все файлы.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
from airflow.sdk import BaseHook, Variable, dag, task
from airflow.sdk.exceptions import AirflowSkipException
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


def get_int(key: str) -> int:
    return int(_variable_or_default(key))


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


class ReconcileWindowVolumeError(RuntimeError):
    """Source row count inside the lookback window exceeds reconcile_max_logids."""


def reconcile_window_since(
    lookback_days: int,
    *,
    now: datetime | None = None,
) -> datetime:
    anchor = now or datetime.now(timezone.utc)
    return anchor - timedelta(days=lookback_days)


def fetch_reconcile_window_sets(
    pg_conn: psycopg2.extensions.connection,
    fb_conn: Any,
    *,
    lookback_days: int,
    max_logids: int,
    now: datetime | None = None,
) -> tuple[datetime, set[int], set[int], int]:
    """Set-diff source↔raw LOGIDs inside the lookback window.

    Both ``reconcile_lookback_days`` and ``reconcile_max_logids`` apply together:
    only rows with ``COALESCE(date) >= since`` are counted and diffed; if that
    count exceeds ``max_logids``, raises ``ReconcileWindowVolumeError``.
    """
    since = reconcile_window_since(lookback_days, now=now)
    source_count = count_exchangelog_rows(fb_conn, since=since)
    if source_count > max_logids:
        raise ReconcileWindowVolumeError(
            f"Reconcile aborted: source has {source_count} LOGID(s) in the "
            f"{lookback_days}-day window > guard {max_logids}. "
            "Raise reconcile_max_logids, narrow reconcile_lookback_days, "
            "or implement batched diff."
        )

    source_logids = fetch_exchangelog_logids(fb_conn, since=since)
    raw_logids = get_all_raw_logids(pg_conn, since=since)
    return since, source_logids, raw_logids, source_count


def count_exchangelog_rows(con: Any, *, since: datetime | None = None) -> int:
    """Count EXCHANGELOG rows in the proxy journal, optionally within a date window.

    Cheap COUNT used as a memory guard before the LOGID set is pulled into the worker.
    ``since`` filters on ``COALESCE(LOGDATE, CREATEDATE)``.
    """
    cur = con.cursor()
    try:
        if since is None:
            cur.execute("SELECT COUNT(*) FROM EXCHANGELOG")
        else:
            cur.execute(
                "SELECT COUNT(*) FROM EXCHANGELOG "
                "WHERE COALESCE(LOGDATE, CREATEDATE) >= ?",
                (since,),
            )
        row = cur.fetchone()
        return int(row[0] or 0)
    finally:
        cur.close()


def fetch_exchangelog_logids(con: Any, *, since: datetime | None = None) -> set[int]:
    """Return EXCHANGELOG LOGIDs from the proxy journal, optionally within a date window.

    Reconcile set-diffs this set against ``exchangelog_raw`` so rows that landed out of LOGID
    order below the watermark are still found. ``since`` filters on ``COALESCE(LOGDATE, CREATEDATE)``.
    """
    cur = con.cursor()
    try:
        if since is None:
            cur.execute("SELECT LOGID FROM EXCHANGELOG")
        else:
            cur.execute(
                "SELECT LOGID FROM EXCHANGELOG "
                "WHERE COALESCE(LOGDATE, CREATEDATE) >= ?",
                (since,),
            )
        return {int(row[0]) for row in cur.fetchall()}
    finally:
        cur.close()


def fetch_exchangelog_by_logids(
    con: Any,
    logids: list[int] | set[int],
    *,
    chunk_size: int = 1000,
) -> list[dict[str, Any]]:
    """Fetch full EXCHANGELOG rows for an explicit set of LOGIDs (chunked IN-lists).

    Serialization matches fetch_exchangelog_after_cursor so reconciled rows load through the
    same load_raw_logs path.
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
            rows.extend(serialize_exchangelog_row(*row) for row in cur.fetchall())
        return rows
    finally:
        cur.close()


def get_all_raw_logids(
    con: psycopg2.extensions.connection,
    *,
    since: datetime | None = None,
) -> set[int]:
    """Return LOGIDs present in exchangelog_raw, optionally within a date window.

    Postgres side of the reconcile set-diff. ``since`` filters on ``COALESCE(createdate, logdate)``.
    """
    with con.cursor() as cur:
        if since is None:
            cur.execute("SELECT logid FROM exchangelog_raw")
        else:
            cur.execute(
                "SELECT logid FROM exchangelog_raw "
                "WHERE COALESCE(createdate, logdate) >= %s",
                (since,),
            )
        return {int(row[0]) for row in cur.fetchall()}


def coalesce_logid_windows(
    logids: list[int] | set[int],
    *,
    max_gap: int = 0,
) -> list[tuple[int, int]]:
    """Group LOGIDs into ``(lo, hi)`` windows, merging runs separated by ``<= max_gap``.

    Default ``max_gap=0`` merges only consecutive LOGIDs so distant missing rows do not
    re-transform the whole span between them.
    """
    ordered = sorted({int(value) for value in logids})
    windows: list[tuple[int, int]] = []
    for logid in ordered:
        # max_gap counts missing LOGIDs between window end and the next id;
        # +1 keeps consecutive runs merged when max_gap=0.
        if windows and logid - windows[-1][1] <= max_gap + 1:
            windows[-1] = (windows[-1][0], logid)
        else:
            windows.append((logid, logid))
    return windows


def transform_missing_windows(
    con: psycopg2.extensions.connection,
    missing: list[int] | set[int],
    *,
    max_gap: int = 0,
) -> int:
    """Run transform over each dense LOGID window of ``missing``.

    Each window uses prefix lookback ``lo`` so a late callback deep in the journal can still
    resolve its getDocumentFile chain from earlier LOGIDs.
    """
    total = 0
    for lo, hi in coalesce_logid_windows(missing, max_gap=max_gap):
        total += transform_raw_to_facts(
            con,
            from_logid=lo - 1,
            to_logid=hi,
            lookback_logids=lo,
        )
    return total


@dag(
    dag_id="egisz_reconcile_dag",
    schedule=get_str("reconcile_schedule"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "reconcile"],
)
def egisz_reconcile_pipeline() -> None:
    # Ретраи гасят транзиентный DeadlockDetected: maintenance-прогоны схемы пересекаются
    # с transform по блокировкам documents/document_attributes; сверка и дозагрузка
    # идемпотентны, watermark задача не двигает — повтор безопасен.
    @task(pool=DWH_POOL, retries=2, retry_delay=timedelta(minutes=1))
    def reconcile_proxy_raw() -> int:
        """Set-diff source↔raw for EXCHANGELOG LOGIDs within the configured lookback window."""
        max_logids = get_int("reconcile_max_logids")
        lookback_days = get_int("reconcile_lookback_days")

        pg_conn = _dwh_connection()
        try:
            last_logid = int(get_cursors(pg_conn, PIPELINE).get("last_logid", 0))
            if last_logid <= 0:
                log.info("Reconcile: watermark not advanced yet; nothing to reconcile.")
                return 0

            pending_rows, pending_max = pending_transform_tail(pg_conn, last_logid)
            if pending_rows > 0:
                raise AirflowSkipException(
                    f"Reconcile deferred: extract backlog has {pending_rows} raw row(s) "
                    f"above watermark LOGID={last_logid} (tail={pending_max}). "
                    "Run extract transform first."
                )

            fb_conn = _proxy_connection()
            try:
                since, source_logids, raw_logids, source_count = fetch_reconcile_window_sets(
                    pg_conn,
                    fb_conn,
                    lookback_days=lookback_days,
                    max_logids=max_logids,
                )
                missing = sorted(source_logids - raw_logids)
                if not missing:
                    log.info(
                        "Reconcile: all %s source LOGID(s) present in raw (%s-day window).",
                        source_count,
                        lookback_days,
                    )
                    return 0
                log.info(
                    "Reconcile: %s row(s) missing from raw in %s-day window, span %s..%s.",
                    len(missing),
                    lookback_days,
                    missing[0],
                    missing[-1],
                )
                late_rows = fetch_exchangelog_by_logids(fb_conn, missing)
            finally:
                fb_conn.close()

            load_raw_logs(pg_conn, late_rows)

            run_analyze(pg_conn, "ANALYZE public.exchangelog_raw")

            reconciled = transform_missing_windows(pg_conn, missing)

            run_analyze(pg_conn, "ANALYZE public.documents")
            refreshed = reconcile_document_attributes_ui(pg_conn)
            if reconciled > 0:
                refresh_error_breakdown(pg_conn)
                refresh_report_marts(pg_conn)
            log.info(
                "Reconcile: recovered %s late chain message(s); "
                "refreshed %s enriched row(s).",
                reconciled,
                refreshed,
            )
            return reconciled
        finally:
            pg_conn.close()

    reconcile_proxy_raw()


egisz_reconcile_pipeline()
