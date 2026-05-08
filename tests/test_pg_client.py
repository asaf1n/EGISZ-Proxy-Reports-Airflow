from __future__ import annotations

import pytest

from egisz_elt.pg_client import _read_bootstrap_sql, load_raw_logs, normalize_message_id, transform_raw_to_facts


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
    assert "egisz_error_interpretation_rules" in sql
    assert "v_rpt_error_interpretations_ui" in sql
