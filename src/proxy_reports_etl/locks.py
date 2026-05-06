from __future__ import annotations

import hashlib


class PipelineLockBusyError(RuntimeError):
    pass


def _lock_key(name: str) -> int:
    digest = hashlib.md5(name.encode("utf-8")).digest()[:8]
    n = int.from_bytes(digest, "big", signed=False)
    if n >= 1 << 63:
        n -= 1 << 64
    return n


def try_acquire_advisory_lock(con, *, pipeline: str) -> None:
    key = _lock_key(pipeline)
    with con.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (key,))
        row = cur.fetchone()
    con.commit()
    ok = bool(row and row[0])
    if not ok:
        raise PipelineLockBusyError(f"pipeline lock is busy: {pipeline}")


def release_advisory_lock(con, *, pipeline: str) -> None:
    key = _lock_key(pipeline)
    try:
        with con.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (key,))
            cur.fetchone()
        con.commit()
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
