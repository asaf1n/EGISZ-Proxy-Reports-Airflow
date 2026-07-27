"""Самодостаточный DAG: суточное обслуживание контура.

Страховочная проверка полноты журнала и обслуживание партиций. Полнота обеспечивается
самой выгрузкой — отметка идёт только по непрерывному участку LOGID, — поэтому проверка
при исправной работе завершается пропуском и служит подтверждением, а не ремонтом.

Канонический исходник — этот файл: он разворачивается на целевые контуры как есть,
без установки дополнительных пакетов. Общие функции (подключения, курсоры, витрины)
сознательно продублированы в соседних egisz_*_dag.py; идентичность копий контролирует
tests/test_dag_selfcontainment.py — правки общих функций вносить синхронно во все файлы.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2
from airflow.exceptions import AirflowSkipException
from airflow.sdk import Connection, dag, task
from firebird.driver import connect
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

PIPELINE = "egisz"
DWH_CONN_ID = "dwh_egisz_pg"
PROXY_CONN_ID = "proxy_egisz_fb"
DWH_POOL = "dwh_postgres"

RAW_LOG_COLUMNS = ("logid", "logdate", "createdate", "msgid", "logstate", "logtext", "msgtext", "uri")

# Дефолты настроек DAG; переопределяются переменной окружения EGISZ_<KEY> (env, не Airflow Variables).
DEFAULTS: dict[str, str | int] = {
    "maintenance_schedule": "@daily",
    # Окно проверки шире любого наблюдавшегося опоздания строки в источнике.
    "consistency_lookback_days": 7,
}


def _setting(key: str) -> str:
    """Настройка DAG из переменной окружения EGISZ_<KEY>, иначе из DEFAULTS.

    Читается и при парсинге (расписание), и внутри задач — без обращения к метабазе Airflow.
    Значения фиксированы в DEFAULTS и переопределяются переменной окружения процессов Airflow
    (см. deploy/README.md). Airflow Variables (метабаза) не используются —
    на Airflow 3 их чтение при парсинге в воркере подвешивало DAG.
    """
    return os.environ.get("EGISZ_" + key.upper(), str(DEFAULTS[key]))


def get_int(key: str) -> int:
    return int(_setting(key))


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
    uri: Any,
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
        "uri": uri,
    }


def get_cursors(con: psycopg2.extensions.connection, pipeline: str) -> dict[str, int]:
    """Read pipeline cursors: extract position in the gateway journal, transform position in raw."""
    with con.cursor() as cur:
        cur.execute(
            "SELECT extract_logid_cursor, transform_logid_cursor, extract_egmid_cursor "
            "FROM etl_state WHERE pipeline = %s",
            (pipeline,),
        )
        row = cur.fetchone()
    if row is None:
        return {
            "extract_logid_cursor": 0,
            "transform_logid_cursor": 0,
            "extract_egmid_cursor": 0,
        }
    return {
        "extract_logid_cursor": int(row[0] or 0),
        "transform_logid_cursor": int(row[1] or 0),
        "extract_egmid_cursor": int(row[2] or 0),
    }


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
            INSERT INTO exchangelog_raw (logid, logdate, createdate, msgid, logstate, logtext, msgtext, uri)
            VALUES %s
            ON CONFLICT (logid, createdate) DO UPDATE SET
                logdate = EXCLUDED.logdate,
                createdate = EXCLUDED.createdate,
                msgid = EXCLUDED.msgid,
                logstate = EXCLUDED.logstate,
                logtext = EXCLUDED.logtext,
                msgtext = EXCLUDED.msgtext,
                uri = EXCLUDED.uri,
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
) -> dict[str, int]:
    """Run the database-side ELT transform for the requested LOGID window.

    Возвращает счётчики батча: перенесённые строки, несвязанные ответы и отправки,
    которым не удалось определить клинику.
    """
    with con.cursor() as cur:
        cur.execute(
            "SELECT public.transform_raw_to_facts(%s, %s)",
            (from_logid, to_logid),
        )
        result = cur.fetchone()[0] or {}
    con.commit()
    return {str(key): int(value or 0) for key, value in dict(result).items()}


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
    return connect_pg(Connection.get(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(Connection.get(PROXY_CONN_ID))


def source_window_low(con: Any, *, since: datetime) -> int:
    """Наименьший LOGID источника в окне проверки."""
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT MIN(LOGID) FROM EXCHANGELOG WHERE COALESCE(LOGDATE, CREATEDATE) >= ?",
            (since,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
    return int(row[0]) if row and row[0] is not None else 0


def count_source_logids(con: Any, *, low: int, high: int) -> int:
    """Число строк источника в диапазоне [low, high]."""
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) FROM EXCHANGELOG WHERE LOGID >= ? AND LOGID <= ?",
            (int(low), int(high)),
        )
        row = cur.fetchone()
    finally:
        cur.close()
    return int(row[0] or 0) if row else 0


def count_raw_logids(con: psycopg2.extensions.connection, *, low: int, high: int) -> int:
    """Число строк exchangelog_raw в том же диапазоне."""
    with con.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM public.exchangelog_raw WHERE logid >= %s AND logid <= %s",
            (int(low), int(high)),
        )
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def fetch_source_logids_range(con: Any, *, low: int, high: int) -> set[int]:
    """LOGID источника в диапазоне [low, high]."""
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT LOGID FROM EXCHANGELOG WHERE LOGID >= ? AND LOGID <= ?",
            (int(low), int(high)),
        )
        return {int(row[0]) for row in cur.fetchall()}
    finally:
        cur.close()


def fetch_raw_logids_range(
    con: psycopg2.extensions.connection,
    *,
    low: int,
    high: int,
) -> set[int]:
    """LOGID, уже присутствующие в exchangelog_raw, в том же диапазоне."""
    with con.cursor() as cur:
        cur.execute(
            "SELECT logid FROM public.exchangelog_raw WHERE logid >= %s AND logid <= %s",
            (int(low), int(high)),
        )
        return {int(row[0]) for row in cur.fetchall()}


def fetch_exchangelog_by_logids(
    con: Any,
    logids: list[int] | set[int],
    *,
    chunk_size: int = 1000,
) -> list[dict[str, Any]]:
    """Fetch full EXCHANGELOG rows for an explicit set of LOGIDs (chunked IN-lists)."""
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
                "SELECT LOGID, LOGDATE, CREATEDATE, MSGID, LOGSTATE, LOGTEXT, MSGTEXT, URI "
                f"FROM EXCHANGELOG WHERE LOGID IN ({placeholders})",
                tuple(chunk),
            )
            rows.extend(serialize_exchangelog_row(*row) for row in cur.fetchall())
        return rows
    finally:
        cur.close()


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
) -> dict[str, int]:
    """Прогнать transform по каждому плотному диапазону догруженных LOGID."""
    totals = {"transformed": 0, "unlinked": 0, "sends_without_clinic": 0}
    for lo, hi in coalesce_logid_windows(missing, max_gap=max_gap):
        batch = transform_raw_to_facts(con, from_logid=lo - 1, to_logid=hi)
        for key in totals:
            totals[key] += int(batch.get(key, 0))
    return totals


def check_journal_window(
    pg_conn: psycopg2.extensions.connection,
    fb_conn: Any,
    *,
    lookback_days: int,
    now: datetime | None = None,
) -> dict[str, int]:
    """Сверка числа строк источника и exchangelog_raw в окне проверки.

    Сравниваются счётчики, а не множества: при исправной выгрузке они совпадают, и задача
    завершается пропуском. Разность множеств — только по расхождению, чтобы найти
    конкретные строки и догрузить их.

    Верхняя граница — отметка выгрузки: выше неё строк в raw закономерно может не быть.
    """
    high = int(get_cursors(pg_conn, PIPELINE)["extract_logid_cursor"])
    if high <= 0:
        raise AirflowSkipException("Отметка выгрузки ещё не двигалась.")

    since = (now or datetime.now(timezone.utc)) - timedelta(days=lookback_days)
    low = source_window_low(fb_conn, since=since)
    if low <= 0 or low > high:
        raise AirflowSkipException(
            f"Ниже отметки {high} строк источника за {lookback_days} сут. нет."
        )

    source_rows = count_source_logids(fb_conn, low=low, high=high)
    raw_rows = count_raw_logids(pg_conn, low=low, high=high)
    if source_rows == raw_rows:
        raise AirflowSkipException(
            f"Журнал полон в LOGID [{low}, {high}]: {source_rows} строк с обеих сторон."
        )

    missing = sorted(
        fetch_source_logids_range(fb_conn, low=low, high=high)
        - fetch_raw_logids_range(pg_conn, low=low, high=high)
    )
    if not missing:
        raise AirflowSkipException(
            f"В LOGID [{low}, {high}] источник отдал {source_rows} строк против {raw_rows} "
            "в raw, недостающих нет — строки удалены на стороне шлюза."
        )

    log.warning(
        "Догрузка %s строк(и) журнала в LOGID [%s, %s].", len(missing), low, high
    )
    load_raw_logs(pg_conn, fetch_exchangelog_by_logids(fb_conn, missing))
    run_analyze(pg_conn, "ANALYZE public.exchangelog_raw")
    batch = transform_missing_windows(pg_conn, missing)
    return {"missing": len(missing), **batch}


@dag(
    dag_id="egisz_maintenance_dag",
    schedule=_setting("maintenance_schedule"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "maintenance"],
)
def egisz_maintenance_pipeline() -> None:
    # Ретраи гасят транзиентный DeadlockDetected: обслуживание пересекается с приёмом
    # по блокировкам documents/document_attributes; догрузка идемпотентна, отметку
    # задача не двигает — повтор безопасен.
    @task(pool=DWH_POOL, retries=2, retry_delay=timedelta(minutes=1))
    def consistency_check() -> dict[str, int]:
        pg_conn = _dwh_connection()
        try:
            fb_conn = _proxy_connection()
            try:
                return check_journal_window(
                    pg_conn,
                    fb_conn,
                    lookback_days=get_int("consistency_lookback_days"),
                )
            finally:
                fb_conn.close()
        finally:
            pg_conn.close()

    @task(pool=DWH_POOL)
    def maintain_partitions() -> int:
        """Создание месячных партиций на предстоящий период.

        DEFAULT-партиции нет намеренно: осевшая в ней строка запрещает последующее
        создание партиции своего месяца. Окно партиций поддерживается здесь, а не
        накатом схемы, чтобы не зависеть от частоты развёртываний.
        """
        pg_conn = _dwh_connection()
        try:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT public.ensure_time_partitions(12, 24)")
                created = int(cur.fetchone()[0] or 0)
            pg_conn.commit()
            log.info("Partition maintenance created %s partition(s).", created)
            return created
        finally:
            pg_conn.close()

    consistency_check()
    maintain_partitions()


egisz_maintenance_pipeline()
