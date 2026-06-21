from __future__ import annotations

from typing import Any

from egisz_elt.dimensions import fetch_organizations
from egisz_elt.extract import fetch_exchangelog_after_cursor
from egisz_elt.reconcile import (
    count_exchangelog_rows,
    fetch_exchangelog_by_logids,
    fetch_exchangelog_logids,
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

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.result[0] if self.result else None

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


def test_fetch_exchangelog_after_cursor_does_not_filter_by_date() -> None:
    con = FakeConnection([])

    fetch_exchangelog_after_cursor(con, after_logid=100, limit=500)

    assert "COALESCE(LOGDATE, CREATEDATE)" not in con.cursor_instance.executed_sql
    assert con.cursor_instance.params == (100, 500)


def test_count_exchangelog_rows_returns_int() -> None:
    con = FakeConnection([(42,)])

    assert count_exchangelog_rows(con) == 42
    assert "SELECT COUNT(*) FROM EXCHANGELOG" in con.cursor_instance.executed_sql


def test_fetch_exchangelog_logids_scans_full_range_without_band() -> None:
    con = FakeConnection([(101,), (102,), (102,)])

    result = fetch_exchangelog_logids(con)

    assert result == {101, 102}
    # Full-range constancy check: no banded LOGID window in the query.
    assert "SELECT LOGID FROM EXCHANGELOG" in con.cursor_instance.executed_sql
    assert "WHERE" not in con.cursor_instance.executed_sql
    assert con.cursor_instance.params is None


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
