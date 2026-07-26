"""Самодостаточный DAG: EXCHANGELOG и реестр подач → exchangelog_raw → факты DWH.

Канонический исходник — этот файл: он разворачивается на целевые контуры как есть,
без установки дополнительных пакетов. Общие функции (подключения, курсоры, витрины)
сознательно продублированы в соседних egisz_*_dag.py; идентичность копий контролирует
tests/test_dag_selfcontainment.py — правки общих функций вносить синхронно во все файлы.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, TypedDict

import psycopg2
from airflow.sdk import Asset, Connection, dag, task
from firebird.driver import connect
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

PIPELINE = "egisz"
DWH_CONN_ID = "dwh_egisz_pg"
PROXY_CONN_ID = "proxy_egisz_fb"
DWH_POOL = "dwh_postgres"

# Активы связывают производителей фактов с DAG обновления витрин: REFRESH выполняется
# в одном месте (egisz_marts_dag), которое запускается по публикации актива.
ASSET_FACTS = Asset("egisz://facts")
ASSET_DICTIONARIES = Asset("egisz://dictionaries")

RAW_LOG_COLUMNS = ("logid", "logdate", "createdate", "msgid", "logstate", "logtext", "msgtext", "uri")

# Дефолты настроек DAG; переопределяются переменной окружения EGISZ_<KEY> (env, не Airflow Variables).
DEFAULTS: dict[str, str | int] = {
    "etl_schedule": "*/5 * * * *",
    "extract_raw_rows": 1000,
    "extract_raw_rounds": 3,
    "registry_rows": 5000,
    "registry_rounds": 3,
    "transform_rows": 3000,
    "transform_rounds": 6,
    # Отметка не поднимается вплотную к хвосту журнала: шлюз наполняет EXCHANGELOG не
    # строго по возрастанию LOGID, и строка, опоздавшая на несколько позиций, иначе
    # осталась бы ниже отметки навсегда. Запас на порядок больше наблюдаемого разброса.
    "etl_lag_logids": 1000,
    "dictionaries_interval_minutes": 60,
}


def _setting(key: str) -> str:
    """Настройка DAG из переменной окружения EGISZ_<KEY>, иначе из DEFAULTS.

    Читается и при парсинге (расписание), и внутри задач — без обращения к метабазе Airflow.
    Значения фиксированы в DEFAULTS и переопределяются переменной окружения процессов Airflow
    (см. deploy/external-airflow/README). Airflow Variables (метабаза) не используются —
    на Airflow 3 их чтение при парсинге в воркере подвешивало DAG.
    """
    return os.environ.get("EGISZ_" + key.upper(), str(DEFAULTS[key]))


def get_int(key: str) -> int:
    return int(_setting(key))


class BatchMetadata(TypedDict):
    count: int
    last_logid: int
    cursor_logid: int


class PipelineBatchInfo(BatchMetadata, total=False):
    transformed: int
    unlinked: int
    sends_without_clinic: int


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


def normalize_registry_key(value: Any) -> str | None:
    """Канонический ключ реестра подач — тот же, что даёт public.message_registry_key.

    Шлюз и ЕГИСЗ передают идентификатор сообщения в разных написаниях (с дефисами и без,
    с префиксом urn:uuid:, в разном регистре); ключ приводится к одному виду на обеих
    сторонах — при загрузке реестра и при поиске по relatesToMessage вердикта.
    """
    text = str(value or "").strip().strip("<>").strip()
    if text.lower().startswith("urn:uuid:"):
        text = text[len("urn:uuid:") :]
    text = text.replace("-", "").upper()
    return text or None


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


def safe_transform_ceiling(*, watermark: int, tail_logid: int, lag_logids: int) -> int:
    """Верхняя граница transform с защитным отставанием от хвоста журнала.

    Строки журнала появляются не строго по возрастанию LOGID. Пока отметка держится
    на ``lag_logids`` ниже хвоста, опоздавшая строка попадает в обычный прямой ход;
    сверка нужна лишь для того, что отстало больше запаса.
    """
    ceiling = tail_logid - max(lag_logids, 0)
    return ceiling if ceiling > watermark else watermark


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
    """Read pipeline cursors: journal LOGID and message-registry EGMID."""
    with con.cursor() as cur:
        cur.execute(
            "SELECT last_logid, last_egmid FROM elt_state WHERE pipeline = %s",
            (pipeline,),
        )
        row = cur.fetchone()
    if row is None:
        return {"last_logid": 0, "last_egmid": 0}
    return {"last_logid": int(row[0] or 0), "last_egmid": int(row[1] or 0)}


def update_cursors(
    con: psycopg2.extensions.connection,
    pipeline: str,
    logid: int = 0,
    egmid: int = 0,
) -> None:
    """Advance cursors through ``GREATEST`` — they never roll back. Only the ETL DAG writes here."""
    with con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO elt_state (pipeline, last_logid, last_egmid)
            VALUES (%s, %s, %s)
            ON CONFLICT (pipeline) DO UPDATE SET
                last_logid = GREATEST(elt_state.last_logid, EXCLUDED.last_logid),
                last_egmid = GREATEST(elt_state.last_egmid, EXCLUDED.last_egmid),
                updated_at = now();
            """,
            (pipeline, logid, egmid),
        )
    con.commit()


def should_run_now(
    con: psycopg2.extensions.connection,
    job: str,
    min_interval_minutes: int,
) -> bool:
    """True, если с прошлого выполнения ``job`` прошло не меньше указанного интервала.

    Гейт каденции для задач, которые живут внутри более частого DAG: справочники
    и витрины динамики не нуждаются в пятиминутной свежести.
    """
    if min_interval_minutes <= 0:
        return True
    with con.cursor() as cur:
        cur.execute(
            "SELECT ran_at <= now() - make_interval(mins => %s) FROM elt_job_runs WHERE job = %s",
            (min_interval_minutes, job),
        )
        row = cur.fetchone()
    return True if row is None else bool(row[0])


def mark_job_run(con: psycopg2.extensions.connection, job: str) -> None:
    """Записать момент выполнения задачи с собственной каденцией."""
    with con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO elt_job_runs (job, ran_at)
            VALUES (%s, now())
            ON CONFLICT (job) DO UPDATE SET ran_at = now();
            """,
            (job,),
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


def load_message_registry(
    con: psycopg2.extensions.connection,
    rows: list[tuple[Any, ...]],
) -> int:
    """Load EGISZ_MESSAGES rows into dim_message_document.

    Ключ приводится к каноническому виду, идентификатор документа — к нижнему регистру,
    чтобы он совпадал с documents.dwh_id без приведения на чтении.
    """
    values: list[tuple[Any, ...]] = []
    for egmid, msgid, reply_to, document_uid, created_at in rows:
        key = normalize_registry_key(msgid)
        uid = str(document_uid or "").strip().lower()
        if not key or not uid:
            continue
        values.append((key, uid, reply_to, int(egmid), created_at))

    if not values:
        return 0

    with con.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_message_document (msgid, document_uid, reply_to, source_egmid, created_at)
            VALUES %s
            ON CONFLICT (msgid) DO UPDATE SET
                document_uid = EXCLUDED.document_uid,
                reply_to = EXCLUDED.reply_to,
                source_egmid = EXCLUDED.source_egmid,
                created_at = EXCLUDED.created_at,
                loaded_at = now()
            """,
            values,
        )
    con.commit()
    return len(values)


def transform_raw_to_facts(
    con: psycopg2.extensions.connection,
    *,
    from_logid: int,
    to_logid: int,
) -> dict[str, int]:
    """Run the database-side ELT transform for the requested LOGID window.

    Возвращает счётчики батча: перенесённые строки, несвязанные вердикты и отправки,
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
                MSGTEXT,
                URI
            FROM EXCHANGELOG
            WHERE LOGID > ?
            ORDER BY LOGID
            ROWS ?
            """
        cur.execute(query, (int(after_logid or 0), int(limit)))
        return [serialize_exchangelog_row(*row) for row in cur.fetchall()]
    finally:
        cur.close()


def fetch_message_registry_after_cursor(
    con: Any,
    *,
    after_egmid: int,
    limit: int,
) -> list[tuple[Any, ...]]:
    """Fetch EGISZ_MESSAGES rows via keyset pagination by EGMID.

    Реестр подач связывает идентификатор исходящего сообщения с localUid документа —
    единственный ключ, по которому асинхронный вердикт ЕГИСЗ находит свой документ.
    """
    if limit <= 0:
        return []

    cur = con.cursor()
    try:
        cur.execute(
            """
            SELECT
                EGMID,
                MSGID,
                REPLYTO,
                DOCUMENTID,
                CREATEDATE
            FROM EGISZ_MESSAGES
            WHERE EGMID > ?
              AND MSGID IS NOT NULL
              AND DOCUMENTID IS NOT NULL
            ORDER BY EGMID
            ROWS ?
            """,
            (int(after_egmid or 0), int(limit)),
        )
        return [tuple(row) for row in cur.fetchall()]
    finally:
        cur.close()


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
    """Fetch license/service rows used to resolve clinic and SEMD kind."""
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


def extract_message_registry_batch(
    pg_conn: psycopg2.extensions.connection,
    fb_conn: Any,
    *,
    registry_rows: int,
    registry_rounds: int,
) -> int:
    """EGISZ_MESSAGES → dim_message_document."""
    cursor_egmid = int(get_cursors(pg_conn, PIPELINE).get("last_egmid", 0))
    total_loaded = 0

    for round_index in range(registry_rounds):
        started_at = time.monotonic()
        rows = fetch_message_registry_after_cursor(
            fb_conn,
            after_egmid=cursor_egmid,
            limit=registry_rows,
        )
        log.info(
            "Fetched %s EGISZ_MESSAGES row(s) after EGMID=%s in %.2fs (round %s).",
            len(rows),
            cursor_egmid,
            time.monotonic() - started_at,
            round_index + 1,
        )
        if not rows:
            break

        total_loaded += load_message_registry(pg_conn, rows)
        cursor_egmid = max(int(row[0]) for row in rows)
        update_cursors(pg_conn, PIPELINE, egmid=cursor_egmid)

        if len(rows) < registry_rows:
            break

    if total_loaded > 0:
        run_analyze(pg_conn, "ANALYZE public.dim_message_document")
    log.info("Message registry complete: %s row(s), EGMID=%s.", total_loaded, cursor_egmid)
    return total_loaded


def transform_exchangelog_batch(
    pg_conn: psycopg2.extensions.connection,
    load_info: BatchMetadata,
    *,
    transform_rows: int,
    transform_rounds: int,
    lag_logids: int,
) -> PipelineBatchInfo:
    """exchangelog_raw → documents/transactions; advance elt_state watermark."""
    watermark = int(load_info.get("last_logid", 0))
    tail_logid = int(load_info.get("cursor_logid", watermark))
    ceiling = safe_transform_ceiling(
        watermark=watermark,
        tail_logid=tail_logid,
        lag_logids=lag_logids,
    )
    if ceiling <= watermark:
        log.info(
            "exchangelog_raw tail LOGID=%s within the %s-LOGID safety lag above watermark %s; "
            "nothing to transform yet.",
            tail_logid,
            lag_logids,
            watermark,
        )
        return {**load_info, "transformed": 0, "unlinked": 0, "sends_without_clinic": 0}

    totals = {"transformed": 0, "unlinked": 0, "sends_without_clinic": 0}
    for iteration in range(transform_rounds):
        pending_rows, tail_logid = pending_transform_tail(pg_conn, watermark)
        if pending_rows == 0:
            log.info("exchangelog_raw cleared above watermark LOGID=%s.", watermark)
            break

        ceiling = safe_transform_ceiling(
            watermark=watermark,
            tail_logid=tail_logid,
            lag_logids=lag_logids,
        )
        to_logid = bounded_transform_to_logid(
            pg_conn,
            last_logid=watermark,
            cursor_logid=ceiling,
            raw_rows=transform_rows,
        )
        if to_logid <= watermark:
            break

        started_at = time.monotonic()
        batch = transform_raw_to_facts(
            pg_conn,
            from_logid=watermark,
            to_logid=to_logid,
        )
        elapsed = time.monotonic() - started_at
        for key in totals:
            totals[key] += int(batch.get(key, 0))
        log.info(
            "Transformed %s row(s) for LOGID (%s, %s] in %.1fs (iteration %s); "
            "unlinked verdicts: %s, sends without clinic: %s.",
            batch.get("transformed", 0),
            watermark,
            to_logid,
            elapsed,
            iteration + 1,
            batch.get("unlinked", 0),
            batch.get("sends_without_clinic", 0),
        )

        update_cursors(pg_conn, PIPELINE, logid=to_logid)
        watermark = to_logid
        log.info("Updated %s watermark to LOGID=%s.", PIPELINE, watermark)

    if totals["transformed"] > 0:
        _analyze_exchangelog_documents(pg_conn)

    return {**load_info, "last_logid": watermark, **totals}


@dag(
    dag_id="egisz_etl_dag",
    schedule=_setting("etl_schedule"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "etl"],
)
def egisz_etl_pipeline() -> None:
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

    # Реестр подач наполняется ДО transform: без него асинхронный вердикт ЕГИСЗ
    # не с чем связать, и исход отправки был бы потерян.
    @task(pool=DWH_POOL)
    def extract_message_registry() -> int:
        pg_conn = _dwh_connection()
        fb_conn = _proxy_connection()
        try:
            return extract_message_registry_batch(
                pg_conn,
                fb_conn,
                registry_rows=get_int("registry_rows"),
                registry_rounds=get_int("registry_rounds"),
            )
        finally:
            fb_conn.close()
            pg_conn.close()

    # Справочники живут в приёме, но со своей каденцией: выборка из Firebird занимает
    # порядка десяти секунд и пятиминутной свежести не требует. Проход по архиву
    # документов относится к обслуживанию и здесь не выполняется.
    @task(pool=DWH_POOL)
    def sync_dictionaries() -> dict[str, int]:
        pg_conn = _dwh_connection()
        try:
            interval_minutes = get_int("dictionaries_interval_minutes")
            if not should_run_now(pg_conn, "sync_dictionaries", interval_minutes):
                log.info("Dictionaries synced less than %s minute(s) ago; skipping.", interval_minutes)
                return {"organizations": 0, "licenses": 0, "changed": 0}

            fb_conn = _proxy_connection()
            try:
                organization_rows = fetch_organizations(fb_conn)
                license_rows = fetch_licenses(fb_conn)
            finally:
                fb_conn.close()

            log.info(
                "Fetched %s organization and %s license row(s) from proxy.",
                len(organization_rows),
                len(license_rows),
            )
            changed = sync_directories(pg_conn, organization_rows, license_rows)
            mark_job_run(pg_conn, "sync_dictionaries")
            log.info("%s dictionary row(s) changed.", changed)
            return {
                "organizations": len(organization_rows),
                "licenses": len(license_rows),
                "changed": changed,
            }
        finally:
            pg_conn.close()

    # Ретраи гасят транзиентный DeadlockDetected: суточное обслуживание пересекается
    # с пятиминутным батчем по блокировкам documents/document_attributes; откат
    # и повтор безопасны — transform идемпотентен, отметка двигается только после успеха.
    @task(pool=DWH_POOL, retries=2, retry_delay=timedelta(minutes=1), outlets=[ASSET_FACTS])
    def transform_exchangelog(load_info: BatchMetadata) -> PipelineBatchInfo:
        pg_conn = _dwh_connection()
        try:
            return transform_exchangelog_batch(
                pg_conn,
                load_info,
                transform_rows=get_int("transform_rows"),
                transform_rounds=get_int("transform_rounds"),
                lag_logids=get_int("etl_lag_logids"),
            )
        finally:
            pg_conn.close()

    extracted = extract_exchangelog()
    registry = extract_message_registry()
    dictionaries = sync_dictionaries()
    transformed = transform_exchangelog(extracted)

    extracted >> registry >> dictionaries >> transformed


egisz_etl_pipeline()
