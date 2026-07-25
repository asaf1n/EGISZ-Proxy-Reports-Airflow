from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from conftest import load_dag_module

_etl_dag = load_dag_module("egisz_etl_dag")
fetch_organizations = _etl_dag.fetch_organizations
fetch_exchangelog_after_cursor = _etl_dag.fetch_exchangelog_after_cursor
fetch_message_registry_after_cursor = _etl_dag.fetch_message_registry_after_cursor

_maintenance_dag = load_dag_module("egisz_maintenance_dag")
fetch_exchangelog_by_logids = _maintenance_dag.fetch_exchangelog_by_logids
source_logid_bounds = _maintenance_dag.source_logid_bounds
fetch_source_logids_range = _maintenance_dag.fetch_source_logids_range


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
        [(101, None, None, "msg-1", 1, "log", "<xml/>", "/emdr/callback")],
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
            "uri": "/emdr/callback",
        }
    ]
    assert con.cursor_instance.params == (100, 500)


def test_fetch_exchangelog_after_cursor_reads_uri_for_subsystem() -> None:
    """URI вызова задаёт подсистему ЕГИСЗ без разбора payload."""
    con = FakeConnection([])

    fetch_exchangelog_after_cursor(con, after_logid=0, limit=10)

    assert "URI" in con.cursor_instance.executed_sql


def test_fetch_exchangelog_after_cursor_does_not_filter_by_date() -> None:
    con = FakeConnection([])

    fetch_exchangelog_after_cursor(con, after_logid=100, limit=500)

    assert "COALESCE(LOGDATE, CREATEDATE)" not in con.cursor_instance.executed_sql
    assert con.cursor_instance.params == (100, 500)


def test_fetch_message_registry_uses_keyset_pagination_by_egmid() -> None:
    """Реестр подач читается тем же keyset-курсором, что и журнал."""
    con = FakeConnection([(7, "MSG", "http://gost-1.lan:9945", "UID", None)])

    rows = fetch_message_registry_after_cursor(con, after_egmid=5, limit=100)

    assert rows == [(7, "MSG", "http://gost-1.lan:9945", "UID", None)]
    assert "FROM EGISZ_MESSAGES" in con.cursor_instance.executed_sql
    assert "WHERE EGMID > ?" in con.cursor_instance.executed_sql
    assert "ROWS ?" in con.cursor_instance.executed_sql
    assert con.cursor_instance.params == (5, 100)


def test_fetch_message_registry_empty_limit_skips_query() -> None:
    con = FakeConnection([])

    assert fetch_message_registry_after_cursor(con, after_egmid=0, limit=0) == []
    assert con.executed_sql == []


def test_source_logid_bounds_limits_scan_to_window() -> None:
    con = FakeConnection([(100, 200)])
    since = datetime(2026, 6, 1, tzinfo=timezone.utc)

    assert source_logid_bounds(con, since=since) == (100, 200)
    assert "COALESCE(LOGDATE, CREATEDATE) >= ?" in con.cursor_instance.executed_sql
    assert con.cursor_instance.params == (since,)


def test_source_logid_bounds_empty_window_returns_zeros() -> None:
    con = FakeConnection([(None, None)])

    assert source_logid_bounds(con, since=datetime(2026, 6, 1, tzinfo=timezone.utc)) == (0, 0)


def test_fetch_source_logids_range_reads_one_chunk() -> None:
    """Сверка идёт шагами по LOGID: окно целиком в память воркера не поднимается."""
    con = FakeConnection([(101,), (102,), (102,)])

    assert fetch_source_logids_range(con, low=100, high=200) == {101, 102}
    assert "LOGID >= ? AND LOGID <= ?" in con.cursor_instance.executed_sql
    assert con.cursor_instance.params == (100, 200)


def test_fetch_exchangelog_by_logids_serializes_rows() -> None:
    con = FakeConnection([(101, None, None, "msg-1", 1, "log", "<xml/>", "/ips/callback")])

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
            "uri": "/ips/callback",
        }
    ]
    assert "WHERE LOGID IN (?)" in con.cursor_instance.executed_sql


def test_fetch_exchangelog_by_logids_chunks_in_lists() -> None:
    con = FakeConnection([(1, None, None, "m", 1, "l", "x", "/emdr/callback")])

    fetch_exchangelog_by_logids(con, [1, 2, 3], chunk_size=2)

    # Two chunks => two IN-list queries with 2 and 1 placeholders respectively.
    assert len(con.executed_sql) == 2
    assert "WHERE LOGID IN (?, ?)" in con.executed_sql[0]
    assert "WHERE LOGID IN (?)" in con.executed_sql[1]


def test_fetch_exchangelog_by_logids_empty_returns_empty_without_query() -> None:
    con = FakeConnection([])

    assert fetch_exchangelog_by_logids(con, []) == []
    assert con.executed_sql == []
