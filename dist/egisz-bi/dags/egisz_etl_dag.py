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

# Порядок обязателен: недельный и месячный слои читают rpt_error_breakdown.
REPORT_MARTS = (
    "public.rpt_error_breakdown",
    "public.rpt_documents_weekly",
    "public.rpt_error_breakdown_weekly",
    "public.rpt_documents_monthly",
    "public.rpt_error_breakdown_monthly",
)

ALLOWED_SYNC_TABLES = {"dim_organizations", "dim_licenses"}
DIRECTORY_COLUMNS = {
    # fir_oid intentionally stays out of the JPERSONS sync: it is filled from NSI.
    "dim_organizations": ("jid", "name", "inn", "address"),
    "dim_licenses": ("id", "service_type", "jid", "mo_uid", "mo_domen", "bdate", "fdate", "kind", "modifydate"),
}
DIRECTORY_PK_COLUMNS = {
    "dim_organizations": ("jid",),
    "dim_licenses": ("id",),
}
DIRECTORY_SYNC_LOCK_TIMEOUT = "15s"
DIRECTORY_SYNC_STATEMENT_TIMEOUT = "5min"
DIRECTORY_SYNC_PAGE_SIZE = 5000

# Дефолты настроек DAG; переопределяются переменной окружения EGISZ_<KEY> (env, не Airflow Variables).
DEFAULTS: dict[str, str | int] = {
    "etl_schedule": "*/5 * * * *",
    "extract_raw_rows": 1000,
    "extract_raw_rounds": 3,
    "registry_rows": 5000,
    "registry_rounds": 3,
    # Глубина приёма по CREATEDATE источника; 0 отключает ограничение.
    "extract_depth_days": 30,
    "transform_rows": 3000,
    "transform_rounds": 6,
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


class ExtractResult(TypedDict):
    count: int
    extract_logid_cursor: int


class TransformResult(TypedDict):
    transformed: int
    unlinked: int
    sends_without_clinic: int
    transform_logid_cursor: int
    dictionary_changes: int


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
    сторонах — при загрузке реестра и при поиске по relatesToMessage ответа.
    """
    text = str(value or "").strip().strip("<>").strip()
    if text.lower().startswith("urn:uuid:"):
        text = text[len("urn:uuid:") :]
    text = text.replace("-", "").upper()
    return text or None


def is_iemk_reply_to(reply_to: Any) -> bool:
    """ИЭМК endpoint: порт 9921."""
    text = str(reply_to or "")
    marker = ":9921"
    pos = text.find(marker)
    if pos < 0:
        return False
    end = pos + len(marker)
    return end == len(text) or not text[end].isdigit()


def contiguous_prefix_end(logids: list[int], *, after: int) -> int:
    """Последний LOGID непрерывного участка страницы источника, начинающегося за ``after``.

    Отметка выгрузки не переступает разрыв LOGID. Пока идентификаторы идут подряд,
    отметка равна последнему из них; на первом пропуске она останавливается.

    ``after <= 0`` задаёт первую строку источника как начало участка.
    """
    if not logids:
        return after
    end = logids[0] - 1 if after <= 0 else after
    for logid in logids:
        if logid != end + 1:
            break
        end = logid
    return end


def bounded_transform_to_logid(
    con: psycopg2.extensions.connection,
    *,
    from_logid: int,
    to_logid: int,
    raw_rows: int,
) -> int:
    """Upper LOGID bound for the next transform chunk (at most ``raw_rows`` raw rows)."""
    if to_logid <= from_logid or raw_rows <= 0:
        return from_logid
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
            (from_logid, from_logid, to_logid, raw_rows),
        )
        row = cur.fetchone()
    return int(row[0] if row else from_logid)


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


def update_cursors(
    con: psycopg2.extensions.connection,
    pipeline: str,
    *,
    extract_logid: int = 0,
    transform_logid: int = 0,
    extract_egmid: int = 0,
) -> None:
    """Advance cursors through ``GREATEST`` — they never roll back. Only the ETL DAG writes here."""
    with con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO etl_state (
                pipeline, extract_logid_cursor, transform_logid_cursor, extract_egmid_cursor
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (pipeline) DO UPDATE SET
                extract_logid_cursor = GREATEST(etl_state.extract_logid_cursor, EXCLUDED.extract_logid_cursor),
                transform_logid_cursor = GREATEST(etl_state.transform_logid_cursor, EXCLUDED.transform_logid_cursor),
                extract_egmid_cursor = GREATEST(etl_state.extract_egmid_cursor, EXCLUDED.extract_egmid_cursor),
                updated_at = now();
            """,
            (pipeline, extract_logid, transform_logid, extract_egmid),
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

    EGMID задаёт строку реестра. ИЭМК не использует localUid, поэтому DOCUMENTID
    не переносится в document_uid для endpoint :9921.
    """
    values: list[tuple[Any, ...]] = []
    for egmid, msgid, reply_to, document_uid, created_at in rows:
        if egmid is None:
            continue
        key = normalize_registry_key(msgid)
        uid = None if is_iemk_reply_to(reply_to) else str(document_uid or "").strip().lower() or None
        values.append((int(egmid), key, uid, reply_to, created_at))

    if not values:
        return 0

    with con.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dim_message_document (source_egmid, msgid, document_uid, reply_to, created_at)
            VALUES %s
            ON CONFLICT (source_egmid) DO UPDATE SET
                msgid = EXCLUDED.msgid,
                document_uid = EXCLUDED.document_uid,
                reply_to = EXCLUDED.reply_to,
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


def recompute_document_jids(con: psycopg2.extensions.connection) -> int:
    """Re-resolve stored document JID after organization/endpoint dictionaries changed."""
    with con.cursor() as cur:
        cur.execute("SELECT public.recompute_document_jids(NULL::text[])")
        row = cur.fetchone()
    con.commit()
    return int((row or [0])[0] or 0)


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


def _dwh_connection():
    return connect_pg(Connection.get(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(Connection.get(PROXY_CONN_ID))


# Нижняя граница окна приёма по каждому источнику. Отбор по дате живёт здесь, а
# постраничная keyset-пагинация идёт по идентификатору.
#
# `probe` — дата строки сразу за отметкой (чтение по индексу первичного ключа), `floor` —
# граница окна (скан по диапазону дат). Тяжёлый запрос выполняется только когда проба
# показала, что отметка ещё не дошла до окна: на прод-объёме он стоит около трёх минут,
# и в установившемся режиме платить за него каждый запуск незачем.
DEPTH_FLOOR_SQL: dict[str, dict[str, str]] = {
    "exchangelog": {
        "probe": "SELECT CREATEDATE FROM EXCHANGELOG WHERE LOGID > ? ORDER BY LOGID ROWS 1",
        "floor": "SELECT MIN(LOGID) FROM EXCHANGELOG WHERE CREATEDATE >= ?",
    },
    "message_registry": {
        "probe": "SELECT CREATEDATE FROM EGISZ_MESSAGES WHERE EGMID > ? ORDER BY EGMID ROWS 1",
        "floor": "SELECT MIN(EGMID) FROM EGISZ_MESSAGES WHERE CREATEDATE >= ?",
    },
}


def _fetch_scalar(con: Any, statement: str, param: Any) -> Any:
    cur = con.cursor()
    try:
        cur.execute(statement, (param,))
        row = cur.fetchone()
    finally:
        cur.close()
    return row[0] if row else None


def fetch_depth_floor(con: Any, *, source: str, depth_days: int, after_id: int) -> int:
    """Наименьший идентификатор источника, попадающий в окно глубины.

    Возвращает отметку, ДО которой строки не нужны: курсор поднимается до неё и дальше
    работает обычная keyset-пагинация. ``0`` означает «поднимать не надо» — отметка уже
    в окне, в источнике не осталось строк или ограничение снято (``depth_days <= 0``).

    Время источника наивное и трактуется как МСК (весь стек в Europe/Moscow), поэтому
    граница считается наивным ``datetime.now()`` — сравнение идёт в одной шкале.
    """
    if depth_days <= 0:
        return 0

    boundary = datetime.now() - timedelta(days=depth_days)
    next_row_date = _fetch_scalar(con, DEPTH_FLOOR_SQL[source]["probe"], int(after_id or 0))
    if next_row_date is None or next_row_date >= boundary:
        return 0

    floor_id = _fetch_scalar(con, DEPTH_FLOOR_SQL[source]["floor"], boundary)
    # Пустое окно (в источнике нет свежих строк) не должно обнулять отметку.
    return int(floor_id) - 1 if floor_id is not None else 0


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
    единственный ключ, по которому асинхронный ответ ЕГИСЗ находит свой документ.
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
                JADDR
                -- FIR_OID is not selected here: NSI matching owns dim_organizations.fir_oid.
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
    depth_days: int,
) -> ExtractResult:
    """EXCHANGELOG → exchangelog_raw.

    Отметка выгрузки считает по журналу шлюза: докуда прокси вычитана в raw. Двигается по
    концу непрерывного участка страницы — разрыв означает строку, которую источник ещё не
    закоммитил, и её подберёт следующий запуск. Повтор безопасен: загрузка идемпотентна
    (UPSERT по ``(logid, createdate)``), а строка журнала в источнике не меняется.
    """
    started_cursor = int(get_cursors(pg_conn, PIPELINE)["extract_logid_cursor"])
    cursor_logid = started_cursor
    total_loaded = 0

    depth_floor = fetch_depth_floor(
        fb_conn, source="exchangelog", depth_days=depth_days, after_id=cursor_logid
    )
    if depth_floor > cursor_logid:
        log.info(
            "Depth window %s day(s): starting EXCHANGELOG fetch at LOGID=%s instead of %s.",
            depth_days,
            depth_floor,
            cursor_logid,
        )
        cursor_logid = depth_floor

    for round_index in range(raw_rounds):
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
            round_index + 1,
        )

        if not log_rows:
            break

        load_raw_logs(pg_conn, log_rows)
        total_loaded += len(log_rows)

        logids = [int(row["logid"]) for row in log_rows]
        advanced = contiguous_prefix_end(logids, after=cursor_logid)
        if advanced < logids[-1]:
            log.warning(
                "EXCHANGELOG gap above LOGID=%s: %s row(s) loaded, extract cursor holds until "
                "the missing row arrives.",
                advanced,
                len(log_rows),
            )
            cursor_logid = advanced
            break

        cursor_logid = advanced
        if len(log_rows) < raw_rows:
            break

    if cursor_logid > started_cursor:
        update_cursors(pg_conn, PIPELINE, extract_logid=cursor_logid)
    if total_loaded > 0:
        _analyze_exchangelog_raw(pg_conn)

    log.info(
        "Extract complete: %s row(s), extract cursor LOGID=%s (was %s).",
        total_loaded,
        cursor_logid,
        started_cursor,
    )
    return {"count": total_loaded, "extract_logid_cursor": cursor_logid}


def extract_message_registry_batch(
    pg_conn: psycopg2.extensions.connection,
    fb_conn: Any,
    *,
    registry_rows: int,
    registry_rounds: int,
    depth_days: int,
) -> int:
    """EGISZ_MESSAGES → dim_message_document."""
    cursor_egmid = int(get_cursors(pg_conn, PIPELINE)["extract_egmid_cursor"])
    total_loaded = 0

    depth_floor = fetch_depth_floor(
        fb_conn, source="message_registry", depth_days=depth_days, after_id=cursor_egmid
    )
    if depth_floor > cursor_egmid:
        log.info(
            "Depth window %s day(s): starting EGISZ_MESSAGES fetch at EGMID=%s instead of %s.",
            depth_days,
            depth_floor,
            cursor_egmid,
        )
        cursor_egmid = depth_floor

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
        update_cursors(pg_conn, PIPELINE, extract_egmid=cursor_egmid)

        if len(rows) < registry_rows:
            break

    if total_loaded > 0:
        run_analyze(pg_conn, "ANALYZE public.dim_message_document")
    log.info("Message registry complete: %s row(s), EGMID=%s.", total_loaded, cursor_egmid)
    return total_loaded


def transform_exchangelog_batch(
    pg_conn: psycopg2.extensions.connection,
    *,
    transform_rows: int,
    transform_rounds: int,
    dictionary_changes: int = 0,
) -> TransformResult:
    """exchangelog_raw → documents/transactions; двигает отметку разбора.

    Отметка разбора считает по exchangelog_raw: докуда raw превращена в факты. Верхняя
    граница — отметка выгрузки: только до неё журнал заведомо вычитан без пропусков.

    Обе отметки читаются из etl_state, а не приходят от выгрузки: сорванная выгрузка
    (недоступный Firebird) не должна мешать разобрать то, что уже лежит в raw.
    """
    cursors = get_cursors(pg_conn, PIPELINE)
    ceiling = int(cursors["extract_logid_cursor"])
    watermark = int(cursors["transform_logid_cursor"])
    totals = {"transformed": 0, "unlinked": 0, "sends_without_clinic": 0}

    for iteration in range(transform_rounds):
        to_logid = bounded_transform_to_logid(
            pg_conn,
            from_logid=watermark,
            to_logid=ceiling,
            raw_rows=transform_rows,
        )
        if to_logid <= watermark:
            break

        started_at = time.monotonic()
        batch = transform_raw_to_facts(pg_conn, from_logid=watermark, to_logid=to_logid)
        elapsed = time.monotonic() - started_at
        for key in totals:
            totals[key] += int(batch.get(key, 0))
        log.info(
            "Transformed %s row(s) for LOGID (%s, %s] in %.1fs (iteration %s); "
            "unlinked responses: %s, sends without clinic: %s.",
            batch.get("transformed", 0),
            watermark,
            to_logid,
            elapsed,
            iteration + 1,
            batch.get("unlinked", 0),
            batch.get("sends_without_clinic", 0),
        )

        update_cursors(pg_conn, PIPELINE, transform_logid=to_logid)
        watermark = to_logid

    if totals["transformed"] > 0:
        _analyze_exchangelog_documents(pg_conn)

    return {**totals, "transform_logid_cursor": watermark, "dictionary_changes": int(dictionary_changes)}


@dag(
    dag_id="egisz_etl_dag",
    schedule=_setting("etl_schedule"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "etl"],
)
def egisz_etl_pipeline() -> None:
    # Ретраи гасят обрывы связи с источником: шлюз и его DNS пропадают на минуты, и
    # одиночный сбой не должен ронять весь запуск. Отметка двигается только после успеха,
    # поэтому повтор безопасен.
    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extract_exchangelog() -> ExtractResult:
        pg_conn = _dwh_connection()
        fb_conn = _proxy_connection()
        try:
            return extract_exchangelog_batch(
                pg_conn,
                fb_conn,
                raw_rows=get_int("extract_raw_rows"),
                raw_rounds=get_int("extract_raw_rounds"),
                depth_days=get_int("extract_depth_days"),
            )
        finally:
            fb_conn.close()
            pg_conn.close()

    # Реестр подач наполняется ДО transform: без него асинхронный ответ ЕГИСЗ
    # не с чем связать, и исход отправки был бы потерян. Недоступный источник не снимает
    # остальную цепочку — то, что уже лежит в raw, разбирается и без него.
    @task(
        pool=DWH_POOL,
        retries=2,
        retry_delay=timedelta(minutes=1),
        trigger_rule="all_done",
    )
    def extract_registry() -> int:
        pg_conn = _dwh_connection()
        fb_conn = _proxy_connection()
        try:
            return extract_message_registry_batch(
                pg_conn,
                fb_conn,
                registry_rows=get_int("registry_rows"),
                registry_rounds=get_int("registry_rounds"),
                depth_days=get_int("extract_depth_days"),
            )
        finally:
            fb_conn.close()
            pg_conn.close()

    # Справочники читаются до transform: клиника и вид СЭМД резолвятся по ним при разборе.
    # Пересчёт архива задача не ведёт: хранимые реквизиты документа справочников не читают —
    # клиника подставляется живым соединением витрины, реестр OID тоже читается на чтении.
    @task(
        pool=DWH_POOL,
        retries=2,
        retry_delay=timedelta(minutes=1),
        trigger_rule="all_done",
    )
    def sync_dictionaries() -> int:
        pg_conn = _dwh_connection()
        fb_conn = _proxy_connection()
        try:
            organization_rows = fetch_organizations(fb_conn)
            license_rows = fetch_licenses(fb_conn)
            log.info(
                "Fetched %s organization and %s license row(s) from proxy.",
                len(organization_rows),
                len(license_rows),
            )
            changed = sync_directories(pg_conn, organization_rows, license_rows)
            relinked = recompute_document_jids(pg_conn) if changed else 0
            if relinked:
                _analyze_exchangelog_documents(pg_conn)
            log.info(
                "%s dictionary row(s) changed; %s document row(s) re-resolved.",
                changed,
                relinked,
            )
            return changed + relinked
        finally:
            fb_conn.close()
            pg_conn.close()

    # Ретраи гасят транзиентный DeadlockDetected: суточное обслуживание пересекается
    # с пятиминутным батчем по блокировкам documents/document_attributes; откат
    # и повтор безопасны — transform идемпотентен, отметка двигается только после успеха.
    @task(
        pool=DWH_POOL,
        retries=2,
        retry_delay=timedelta(minutes=1),
        trigger_rule="all_done",
    )
    def transform(dictionary_changes: int | None = None) -> TransformResult:
        pg_conn = _dwh_connection()
        try:
            return transform_exchangelog_batch(
                pg_conn,
                transform_rows=get_int("transform_rows"),
                transform_rounds=get_int("transform_rounds"),
                dictionary_changes=int(dictionary_changes or 0),
            )
        finally:
            pg_conn.close()

    # Витрины обновляются там же, где меняется их основание, — иначе слой со «свежестью
    # фактов» отстаёт от фактов.
    @task(
        pool=DWH_POOL,
        retries=2,
        retry_delay=timedelta(minutes=1),
        trigger_rule="all_done",
    )
    def refresh_marts(transformed: TransformResult | None) -> None:
        if not int((transformed or {}).get("transformed", 0)) and not int(
            (transformed or {}).get("dictionary_changes", 0)
        ):
            raise AirflowSkipException("Факты не менялись — обновлять нечего.")
        pg_conn = _dwh_connection()
        try:
            for matview in REPORT_MARTS:
                _refresh_matview(pg_conn, matview)
            run_analyze(pg_conn, *(f"ANALYZE {matview}" for matview in REPORT_MARTS))
        finally:
            pg_conn.close()

    extracted = extract_exchangelog()
    registry = extract_registry()
    dictionaries = sync_dictionaries()
    transformed = transform(dictionaries)
    refreshed = refresh_marts(transformed)

    extracted >> registry >> dictionaries >> transformed >> refreshed


egisz_etl_pipeline()
