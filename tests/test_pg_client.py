from __future__ import annotations

import pytest

from pathlib import Path

from egisz_elt.pg_client import (
    DIRECTORY_SYNC_LOCK_TIMEOUT,
    DIRECTORY_SYNC_PAGE_SIZE,
    DIRECTORY_SYNC_STATEMENT_TIMEOUT,
    get_related_message_identifiers,
    load_raw_logs,
    normalize_message_id,
    sync_directory,
    transform_raw_to_facts,
)

DWH_INIT_SQL_PATH = Path(__file__).resolve().parents[1] / "db" / "dwh_init.sql"


def _read_dwh_init_sql() -> str:
    # Находим папку db/parts
    parts_dir = DWH_INIT_SQL_PATH.parent / "parts"
    sql_contents = []
    
    # Читаем все SQL-файлы и склеиваем их
    for sql_file in sorted(parts_dir.glob("*.sql")):
        sql_contents.append(sql_file.read_text(encoding="utf-8"))
        
    return "\n".join(sql_contents)


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

    transformed = transform_raw_to_facts(con, from_logid=10, to_logid=20, from_egmid=30, to_egmid=40)

    assert transformed == 3
    assert con.cursor_instance.calls[0] == (
        "SELECT public.egisz_transform_raw_to_facts(%s, %s, %s, %s)",
        (10, 20, 30, 40),
    )
    assert con.committed is True


def test_dwh_init_sql_uses_semd_identifiers_before_transport_host_fallback() -> None:
    sql = _read_dwh_init_sql()

    document_key_view = 'd.document_key AS "Документ (ключ учёта)"'

    assert document_key_view in sql
    assert "CREATE OR REPLACE FUNCTION public.egisz_document_key" in sql
    assert "public.egisz_document_key(m.document_id, m.document_id)" in sql
    assert "public.egisz_clean_text_value(t.message_id),\n        t.exchangelog_log_id::text" not in sql
    assert "CREATE OR REPLACE FUNCTION public.egisz_normalize_semd_code" in sql
    assert "public.v_egisz_documents_enriched_ui" in sql
    assert 'd.jid::text AS "JID клиники"' in sql


def test_dwh_init_sql_maps_semd_kind_to_reference_oid() -> None:
    sql = _read_dwh_init_sql()

    assert "INSERT INTO dim_semd_types (code, type_code, name, level, format_code, start_date, end_date, implementation_guide, git_link)" in sql
    assert "oid = EXCLUDED.code" in sql
    assert "SET oid = code" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_dim_semd_types_oid" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_xml_local_uid_norm" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_xml_document_id_norm" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_xml_message_id_norm" in sql
    assert "current_messages AS" in sql
    assert "current_message_ids AS" in sql
    assert "current_document_ids AS" in sql
    assert "public.egisz_xml_text(r.msgtext, 'KIND') AS kind_xml" in sql
    assert "public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'localUid')) AS local_uid_xml" in sql
    assert "public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'DOCUMENTID')) AS document_id_xml" in sql
    assert "COALESCE(r.local_uid_xml, r.document_id_xml, public.egisz_clean_text_value(m.document_id)) AS local_uid_semd" in sql
    assert "public.egisz_clean_text_value(d.local_uid)" in sql
    assert "status = EXCLUDED.status" in sql
    assert "status_category = EXCLUDED.status_category" in sql
    assert "SELECT DISTINCT ON (document_key)" in sql
    assert "public.egisz_normalize_semd_code(r.kind_xml) AS semd_code" in sql
    assert "src_doc.semd_code AS source_document_semd_code" in sql
    assert "p.source_document_semd_code" in sql
    assert "WHERE d.oid = n.code" in sql
    assert "WHERE dst.oid = public.egisz_normalize_semd_code(d.semd_code)" in sql
    assert "FROM public.fact_egisz_documents" in sql
    assert "FROM public.stg_egisz_messages m" in sql
    assert "CREATE OR REPLACE VIEW public.fact_egisz_messages AS" in sql
    assert "FROM public.v_egisz_documents_enriched_ui d" in sql
    assert "document_group_key" not in sql
    assert "CREATE MATERIALIZED VIEW public.v_egisz_documents_daily_ui" in sql
    assert "p.error_code = 'NO_DOCUMENT_KIND_ON_DATE'" not in sql
    assert "regexp_match(COALESCE(p.msgtext, ''), '\\[([0-9]+)\\]')" not in sql
    assert "regexp_match(COALESCE(r.msgtext, ''), '\\[([0-9]+)\\]')" not in sql
    assert "message_kind" not in sql
    assert "license_kind" not in sql
    assert "documentTypeName" not in sql
    assert "documentName" not in sql


def test_reporting_views_do_not_depend_on_raw_tables() -> None:
    reporting_sql = "\n".join(
        (DWH_INIT_SQL_PATH.parent / "parts" / file_name).read_text(encoding="utf-8")
        for file_name in ("70_views_core.sql", "75_views_stg.sql", "80_views_rpt.sql")
    )

    assert "exchangelog_raw" not in reporting_sql
    assert "egisz_messages_raw" not in reporting_sql
    assert "stg_egisz_messages" not in reporting_sql
    assert "fact_egisz_messages" not in reporting_sql
    assert "fact_egisz_transactions" not in reporting_sql


def test_dwh_init_sql_interprets_patient_address_schematron_and_network_errors() -> None:
    sql = _read_dwh_init_sql()

    assert "Не указан адрес пациента" in sql
    assert "Данные пациента не соответствуют ГИП" in sql
    assert "Документ уже зарегистрирован в РЭМД" in sql
    assert "Не удалось получить файл ЭМД из предоставляющей ИС" in sql
    assert "Ошибка регистрации" in sql
    assert "Отказ РЭМД" not in sql
    assert "Отказ РЭМД (ns2status: error)" not in sql
    assert "Сетевая ошибка: " in sql
    assert "'Сетевая ошибка'" in sql
    assert "ошибка связи (транспорт)" not in sql
    assert "Наименование СЭМД отсутствует в справочнике СЭМД" in sql
    assert "Наименование СЭМД отсутствует в НСИ 1520" not in sql
    assert "egisz_error_interpretation_rules" in sql
    assert "v_rpt_error_interpretations_ui" in sql
    assert 'AS "Ошибки JSON raw"' not in sql
    assert "egisz_error_messages_row" in sql
    assert "FROM public.v_stg_channel_network_errors_by_document s" in sql


def test_dwh_init_sql_keeps_only_three_reported_emd_statuses() -> None:
    sql = _read_dwh_init_sql()

    assert "Прокси не отдаёт отдельный статус для синхронного приёма" in sql
    assert "THEN 'success'" in sql
    assert "THEN 'sent'" not in sql
    assert "WHEN t.status = 'sent' THEN 'Отправлен'" not in sql
    assert "WHEN d.status = 'success' THEN 'Успешный ответ'" in sql
    assert "WHEN d.status = 'network_error' THEN 'Ошибка связи'" in sql
    assert "WHEN d.status = 'registration_error' THEN 'Ошибка регистрации'" in sql
    assert "WHERE e.final_status IN ('success', 'error')" in sql
    assert "WHERE \"Статус\" IN ('success', 'error')" in sql
    assert "AND NULLIF(TRIM(\"Документ (ключ учёта)\"), '') IS NOT NULL" in sql
    assert "public.egisz_clean_text_value(t.message_id),\n        t.exchangelog_log_id::text" not in sql
    assert "pending_source AS" not in sql
    assert "WHEN e.final_status = 'success' THEN 'Успешно'" not in sql


def test_dwh_init_sql_does_not_keep_egisz_messages_raw_staging() -> None:
    sql = _read_dwh_init_sql()

    assert "CREATE TABLE IF NOT EXISTS egisz_messages_raw" not in sql
    assert "INSERT INTO egisz_messages_raw" not in sql
    assert "DROP TABLE IF EXISTS public.egisz_messages_raw CASCADE" in sql


def test_get_related_message_identifiers_parses_refs_in_postgres() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[object, ...]) -> None:
            self.calls.append((sql, params))

        def fetchall(self) -> list[tuple[str, str]]:
            return [("msgid", "m-1"), ("document_id", "doc-1"), ("msgid", "m-1")]

    class Connection:
        def __init__(self) -> None:
            self.cursor_instance = Cursor()

        def cursor(self) -> Cursor:
            return self.cursor_instance

    con = Connection()

    related = get_related_message_identifiers(con, from_logid=10, to_logid=20)

    assert related == {"msgids": {"m-1"}, "document_ids": {"doc-1"}}
    assert con.cursor_instance.calls[0][1] == (10, 20)
    assert "public.exchangelog_raw" in con.cursor_instance.calls[0][0]


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
