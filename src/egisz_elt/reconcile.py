from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg2

from egisz_elt.common import serialize_exchangelog_row, transform_raw_to_facts

log = logging.getLogger(__name__)


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
