from __future__ import annotations

from typing import Any

from egisz_elt.fb_client import fetch_organizations


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
