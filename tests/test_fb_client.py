from __future__ import annotations

from datetime import datetime
from typing import Any

from egisz_elt.fb_client import (
    fetch_exchangelog_after_cursor,
    fetch_exchangelog_by_logids,
    fetch_exchangelog_logids,
    fetch_organizations,
)


class FakeCursor:
    description: list[tuple[str, ...]] = []

    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection
        self.result: list[tuple[Any, ...]] = []
        self.executed_sql = ""
        self.params: tuple[Any, ...] | None = None
        self.closed = False

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.executed_sql = sql
        self.params = params
        self.connection.executed_sql.append(sql)
        self.result = self.connection.rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.result

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.executed_sql: list[str] = []
        self.cursor_instance = FakeCursor(self)

    def cursor(self) -> FakeCursor:
        return self.cursor_instance


def test_fetch_organizations_selects_jpersons_legal_entity_columns() -> None:
    con = FakeConnection(
        [(1, "Clinic", "1234567890", "Main street")],
    )

    assert fetch_organizations(con) == [(1, "Clinic", "1234567890", "Main street")]
    assert "JINN" in con.executed_sql[-1]
    assert "JADDR" in con.executed_sql[-1]


def test_fetch_organizations_preserves_empty_legal_entity_values() -> None:
    con = FakeConnection(
        [(1, "Clinic", None, None)],
    )

    assert fetch_organizations(con) == [(1, "Clinic", None, None)]


def test_fetch_exchangelog_after_cursor_includes_createdate_for_message_analytics() -> None:
    con = FakeConnection(
        [(101, None, None, "msg-1", 1, "log", "<xml/>")],
    )

    rows = fetch_exchangelog_after_cursor(con, after_logid=100, limit=500)

    assert rows == [
        {
            "logid": 101,
            "logdate": None,
            "createdate": None,
            "msgid": "msg-1",
            "logstate": 1,
            "logtext": "log",
            "msgtext": "<xml/>",
        }
    ]
    assert con.cursor_instance.params == (100, 500)


def test_fetch_exchangelog_after_cursor_applies_created_cutoff() -> None:
    cutoff = datetime(2026, 5, 18)
    con = FakeConnection([])

    fetch_exchangelog_after_cursor(con, after_logid=100, limit=500, created_from=cutoff)

    assert "COALESCE(LOGDATE, CREATEDATE) >= ?" in con.cursor_instance.executed_sql
    assert con.cursor_instance.params == (100, cutoff, 500)


def test_fetch_exchangelog_logids_applies_created_cutoff_and_returns_ints() -> None:
    cutoff = datetime(2026, 5, 18)
    con = FakeConnection([(101,), (102,)])

    logids = fetch_exchangelog_logids(con, created_from=cutoff)

    assert logids == [101, 102]
    assert "COALESCE(LOGDATE, CREATEDATE) >= ?" in con.cursor_instance.executed_sql
    assert con.cursor_instance.params == (cutoff,)


def test_fetch_exchangelog_logids_without_cutoff_scans_all() -> None:
    con = FakeConnection([(7,)])

    assert fetch_exchangelog_logids(con) == [7]
    assert "WHERE" not in con.cursor_instance.executed_sql
    assert con.cursor_instance.params == ()


def test_fetch_exchangelog_by_logids_serializes_rows() -> None:
    con = FakeConnection([(101, None, None, "msg-1", 1, "log", "<xml/>")])

    rows = fetch_exchangelog_by_logids(con, [101])

    assert rows == [
        {
            "logid": 101,
            "logdate": None,
            "createdate": None,
            "msgid": "msg-1",
            "logstate": 1,
            "logtext": "log",
            "msgtext": "<xml/>",
        }
    ]
    assert "WHERE LOGID IN (?)" in con.cursor_instance.executed_sql


def test_fetch_exchangelog_by_logids_chunks_in_lists() -> None:
    con = FakeConnection([(1, None, None, "m", 1, "l", "x")])

    fetch_exchangelog_by_logids(con, [1, 2, 3], chunk_size=2)

    # Two chunks => two IN-list queries with 2 and 1 placeholders respectively.
    assert len(con.executed_sql) == 2
    assert "WHERE LOGID IN (?, ?)" in con.executed_sql[0]
    assert "WHERE LOGID IN (?)" in con.executed_sql[1]


def test_fetch_exchangelog_by_logids_empty_returns_empty_without_query() -> None:
    con = FakeConnection([])

    assert fetch_exchangelog_by_logids(con, []) == []
    assert con.executed_sql == []
