from __future__ import annotations

import logging
from typing import Any

import psycopg2

from egisz_elt.common import serialize_exchangelog_row, transform_raw_to_facts

log = logging.getLogger(__name__)

# Reconcile recovers scattered chain messages. Mirrors the transform's own -500
# getDocumentFile lookback so coalesced windows scan no more raw than a single window.
RECONCILE_WINDOW_MAX_GAP = 500


def count_exchangelog_rows(con: Any) -> int:
    """Count all EXCHANGELOG rows in the proxy journal.

    Cheap COUNT used as a memory guard before the full LOGID set is pulled into the worker —
    see README.md §«Полная сверка константности источник↔raw».
    """
    cur = con.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM EXCHANGELOG")
        row = cur.fetchone()
        return int(row[0] or 0)
    finally:
        cur.close()


def fetch_exchangelog_logids(con: Any) -> set[int]:
    """Return the full set of EXCHANGELOG LOGIDs from the proxy journal.

    The whole journal is materialized as one set so reconcile can set-diff it against the whole
    of ``exchangelog_raw`` — rows that landed out of LOGID order below the watermark are
    invisible to the forward cursor (`LOGID > last_logid`) and only a full-range diff finds
    them. See README.md §«Полная сверка константности источник↔raw».
    """
    cur = con.cursor()
    try:
        cur.execute("SELECT LOGID FROM EXCHANGELOG")
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


def get_all_raw_logids(con: psycopg2.extensions.connection) -> set[int]:
    """Return every LOGID present in exchangelog_raw.

    The full raw set is the Postgres side of the constancy set-diff against the proxy journal —
    see README.md §«Полная сверка константности источник↔raw».
    """
    with con.cursor() as cur:
        cur.execute("SELECT logid FROM exchangelog_raw")
        return {int(row[0]) for row in cur.fetchall()}


def coalesce_logid_windows(
    logids: list[int] | set[int],
    *,
    max_gap: int = RECONCILE_WINDOW_MAX_GAP,
) -> list[tuple[int, int]]:
    """Group LOGIDs into ``(lo, hi)`` windows, merging runs separated by ``<= max_gap``.

    Transforming the single ``min..max`` span would re-parse everything between two distant
    LOGIDs; per-id windows would issue one transform call per row. Coalescing into dense
    windows bounds the re-transform to the actual gaps.
    """
    ordered = sorted({int(value) for value in logids})
    windows: list[tuple[int, int]] = []
    for logid in ordered:
        if windows and logid - windows[-1][1] <= max_gap:
            windows[-1] = (windows[-1][0], logid)
        else:
            windows.append((logid, logid))
    return windows


def transform_missing_windows(
    con: psycopg2.extensions.connection,
    missing: list[int] | set[int],
    *,
    max_gap: int = RECONCILE_WINDOW_MAX_GAP,
) -> int:
    """Run ``egisz_transform_raw_to_facts`` over each dense LOGID window of ``missing``."""
    total = 0
    for lo, hi in coalesce_logid_windows(missing, max_gap=max_gap):
        total += transform_raw_to_facts(con, from_logid=lo - 1, to_logid=hi)
    return total
