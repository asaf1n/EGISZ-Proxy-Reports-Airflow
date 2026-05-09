from __future__ import annotations

import pytest

from egisz_elt.pg_client import (
    BOOTSTRAP_LOCK_TIMEOUT,
    BOOTSTRAP_STATEMENT_TIMEOUT,
    DIRECTORY_SYNC_LOCK_TIMEOUT,
    DIRECTORY_SYNC_PAGE_SIZE,
    DIRECTORY_SYNC_STATEMENT_TIMEOUT,
    _read_bootstrap_sql,
    ensure_tables,
    load_raw_logs,
    normalize_message_id,
    sync_directory,
    transform_raw_to_facts,
)


class FakeConnection:
    def cursor(self):  # pragma: no cover - must not be reached in this test
        raise AssertionError("load_raw_logs should fail before opening a cursor")

    def commit(self) -> None:  # pragma: no cover - must not be reached in this test
        raise AssertionError("load_raw_logs should fail before commit")


def test_load_raw_logs_rejects_missing_required_exchangelog_keys() -> None:
    row = {
        "logid": 1,
        "logdate": "2026-05-07T15:00:00",
        "createdate": "2026-05-07T14:59:00",
        "msgid": "message-1",
        "logstate": 1,
        "logtext": "ok",
    }

    with pytest.raises(ValueError, match="msgtext"):
        load_raw_logs(FakeConnection(), [row])


def test_normalize_message_id_strips_urn_uuid_wrapper() -> None:
    assert normalize_message_id("<urn:uuid:dd73fc79-e2e6-479c-a285-2a470fc4f04e>") == "dd73fc79-e2e6-479c-a285-2a470fc4f04e"
    assert normalize_message_id("urn:uuid:dd73fc79-e2e6-479c-a285-2a470fc4f04e") == "dd73fc79-e2e6-479c-a285-2a470fc4f04e"
    assert normalize_message_id("dd73fc79-e2e6-479c-a285-2a470fc4f04e") == "dd73fc79-e2e6-479c-a285-2a470fc4f04e"


class FakeTransformCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.result: tuple[int] = (3,)

    def __enter__(self) -> "FakeTransformCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.calls.append((sql, params))

    def fetchone(self) -> tuple[int]:
        return self.result

    def fetchall(self) -> list[tuple[str, str]]:
        return []


class FakeTransformConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeTransformCursor()
        self.committed = False

    def cursor(self) -> FakeTransformCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.committed = True


def test_ensure_tables_sets_bounded_bootstrap_timeouts(monkeypatch: pytest.MonkeyPatch) -> None:
    con = FakeTransformConnection()
    monkeypatch.setattr("egisz_elt.pg_client._read_bootstrap_sql", lambda: "CREATE TABLE example(id int);")

    ensure_tables(con)

    assert con.cursor_instance.calls == [
        ("SET LOCAL lock_timeout = %s", (BOOTSTRAP_LOCK_TIMEOUT,)),
        ("SET LOCAL statement_timeout = %s", (BOOTSTRAP_STATEMENT_TIMEOUT,)),
        ("CREATE TABLE example(id int);", None),
    ]
    assert con.committed is True


def test_transform_raw_to_facts_passes_log_and_message_cursor_bounds() -> None:
    con = FakeTransformConnection()

    transformed = transform_raw_to_facts(con, min_log_id=10, max_log_id=20, min_egmid=30, max_egmid=40)

    assert transformed == 3
    assert con.cursor_instance.calls[0] == (
        "SELECT public.egisz_transform_raw_to_facts(%s, %s, %s, %s)",
        (10, 20, 30, 40),
    )
    assert con.committed is True


def test_bootstrap_sql_uses_semd_identifiers_before_transport_host_fallback() -> None:
    sql = _read_bootstrap_sql()

    document_priority = "COALESCE(t.local_uid_semd, t.emdr_id, t.relates_to_id"
    jid_priority = "COALESCE(p.message_jid, p.jid_from_payload) AS resolved_jid"

    assert document_priority in sql
    assert jid_priority in sql


def test_bootstrap_sql_interprets_patient_address_schematron_and_network_errors() -> None:
    sql = _read_bootstrap_sql()

    assert "Не указан адрес пациента" in sql
    assert "Сетевая ошибка: " in sql
    assert "'Сетевая ошибка'" in sql
    assert "ошибка связи (транспорт)" not in sql
    assert "Наименование СЭМД отсутствует в справочнике СЭМД" in sql
    assert "Наименование СЭМД отсутствует в НСИ 1520" not in sql
    assert "egisz_error_interpretation_rules" in sql
    assert "v_rpt_error_interpretations_ui" in sql
    assert "FROM public.v_stg_channel_network_errors_by_document s" in sql


class FakeSyncCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []

    def __enter__(self) -> "FakeSyncCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.calls.append((sql, params))


class FakeSyncConnection:
    def __init__(self) -> None:
        self.cursor_instance = FakeSyncCursor()
        self.committed = False

    def cursor(self) -> FakeSyncCursor:
        return self.cursor_instance

    def commit(self) -> None:
        self.committed = True


def test_sync_directory_sets_timeouts_and_uses_paged_execute_values(monkeypatch: pytest.MonkeyPatch) -> None:
    con = FakeSyncConnection()
    captured: dict[str, object] = {}

    def fake_execute_values(cursor: object, sql: str, values: list[tuple[object, ...]], page_size: int) -> None:
        captured["cursor"] = cursor
        captured["sql"] = sql
        captured["values"] = values
        captured["page_size"] = page_size

    monkeypatch.setattr("egisz_elt.pg_client.execute_values", fake_execute_values)

    sync_directory(con, "dim_organizations", [(1, "Clinic", "1234567890", "Address")])

    assert con.cursor_instance.calls == [
        ("SET LOCAL lock_timeout = %s", (DIRECTORY_SYNC_LOCK_TIMEOUT,)),
        ("SET LOCAL statement_timeout = %s", (DIRECTORY_SYNC_STATEMENT_TIMEOUT,)),
    ]
    assert captured["cursor"] is con.cursor_instance
    assert "INSERT INTO dim_organizations" in str(captured["sql"])
    assert captured["values"] == [(1, "Clinic", "1234567890", "Address")]
    assert captured["page_size"] == DIRECTORY_SYNC_PAGE_SIZE
    assert con.committed is True
