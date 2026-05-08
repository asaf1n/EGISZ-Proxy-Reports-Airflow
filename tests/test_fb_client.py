from __future__ import annotations

from typing import Any

from egisz_elt.fb_client import fetch_egisz_messages_after_cursor, fetch_exchangelog_after_cursor, fetch_organizations


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection
        self.result: list[tuple[Any, ...]] = []
        self.executed_sql = ""
        self.closed = False

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.executed_sql = sql
        self.connection.executed_sql.append(sql)
        self.result = self.connection.organization_rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.result

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, organization_rows: list[tuple[Any, ...]]) -> None:
        self.organization_rows = organization_rows
        self.executed_sql: list[str] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)


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


class FakeMessagesCursor:
    description: list[tuple[str, ...]] = []

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.params: tuple[Any, ...] | None = None
        self.closed = False

    def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> None:
        self.params = params

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def close(self) -> None:
        self.closed = True


class FakeMessagesConnection:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.cursor_instance = FakeMessagesCursor(rows)

    def cursor(self) -> FakeMessagesCursor:
        return self.cursor_instance


def test_fetch_egisz_messages_after_cursor_serializes_rows_for_xcom() -> None:
    con = FakeMessagesConnection(
        [(42, 10, "1", None, "<urn:uuid:msg>", "urn:uuid:reply", "doc-1", "<xml/>")],
    )

    rows = fetch_egisz_messages_after_cursor(con, after_egmid=41, limit=500)

    assert rows == [
        {
            "egmid": 42,
            "jid": 10,
            "kind": "1",
            "created_at": None,
            "msgid": "<urn:uuid:msg>",
            "reply_to": "urn:uuid:reply",
            "document_id": "doc-1",
            "msgtext": "<xml/>",
        }
    ]
    assert con.cursor_instance.params == (41, 500)


def test_fetch_exchangelog_after_cursor_includes_createdate_for_message_analytics() -> None:
    con = FakeMessagesConnection(
        [(101, None, None, "msg-1", 1, "log", "<xml/>")],
    )

    rows = fetch_exchangelog_after_cursor(con, after_log_id=100, limit=500)

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
