from __future__ import annotations

import logging
import time
from typing import Any

import psycopg2

from egisz_elt.common import (
    PIPELINE,
    BatchMetadata,
    PipelineBatchInfo,
    bounded_transform_to_logid,
    get_cursors,
    load_raw_logs,
    pending_transform_tail,
    refresh_error_breakdown,
    run_analyze,
    serialize_exchangelog_row,
    transform_raw_to_facts,
    update_cursors,
)

log = logging.getLogger(__name__)


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


def extract_exchangelog(
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


def transform_exchangelog(
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
