from __future__ import annotations

import logging
from typing import Any

from egisz_elt.common import serialize_exchangelog_row

log = logging.getLogger(__name__)


def fetch_exchangelog_after_cursor(
    con: Any,
    *,
    after_logid: int,
    limit: int,
) -> list[dict[str, Any]]:
    """Fetch an EXCHANGELOG batch via keyset pagination by LOGID for in-process E+L load.

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
