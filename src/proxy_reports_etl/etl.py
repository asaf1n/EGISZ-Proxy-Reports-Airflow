from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from proxy_reports_etl.config import AppConfig
from proxy_reports_etl.fb_client import fetch_rows_after_cursor
from proxy_reports_etl.locks import release_advisory_lock, try_acquire_advisory_lock
from proxy_reports_etl.pg_client import (
    ensure_tables,
    get_last_cursor,
    set_last_cursor,
    upsert_raw_rows,
)


@dataclass(frozen=True)
class SyncStats:
    fetched: int
    upserted: int
    last_cursor_before: str
    last_cursor_after: str


def run_sync(*, cfg: AppConfig, fb_con, pg_con, log=print) -> SyncStats:
    """
    Minimal incremental ETL:
      - read last_cursor from Postgres
      - fetch batch from Firebird where cursor_column > last_cursor
      - upsert rows into Postgres raw table, keyed by cursor_value
      - update last_cursor

    The cursor is stored as TEXT for portability; numeric cursors are still compared numerically when possible.
    """
    ensure_tables(pg_con, target_table=cfg.etl.target_table)
    try_acquire_advisory_lock(pg_con, pipeline=cfg.etl.pipeline)
    try:
        last_before = get_last_cursor(pg_con, pipeline=cfg.etl.pipeline)
        after_cursor: Any = last_before
        # Try to pass numeric cursor to Firebird when possible
        if last_before != "":
            try:
                after_cursor = int(last_before)
            except Exception:
                after_cursor = last_before
        log(
            f"sync_start pipeline={cfg.etl.pipeline} cursor_column={cfg.etl.cursor_column} "
            f"last_cursor={last_before!r} batch_size={cfg.etl.batch_size}"
        )

        rows = fetch_rows_after_cursor(
            fb_con,
            source_sql=cfg.etl.source_sql,
            cursor_column=cfg.etl.cursor_column,
            after_cursor=after_cursor,
            limit=cfg.etl.batch_size,
        )
        max_cursor = upsert_raw_rows(
            pg_con, target_table=cfg.etl.target_table, rows=rows, cursor_column=cfg.etl.cursor_column
        )
        if max_cursor is not None:
            set_last_cursor(pg_con, pipeline=cfg.etl.pipeline, last_cursor=max_cursor)
            last_after = max_cursor
        else:
            last_after = last_before
        log(f"sync_done fetched={len(rows)} last_cursor_after={last_after!r}")
        return SyncStats(
            fetched=len(rows),
            upserted=len(rows),
            last_cursor_before=last_before,
            last_cursor_after=last_after,
        )
    finally:
        release_advisory_lock(pg_con, pipeline=cfg.etl.pipeline)
