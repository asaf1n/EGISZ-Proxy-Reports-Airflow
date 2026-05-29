from __future__ import annotations

import pytest

from pathlib import Path

from egisz_elt.pg_client import (
    DIRECTORY_SYNC_LOCK_TIMEOUT,
    DIRECTORY_SYNC_PAGE_SIZE,
    DIRECTORY_SYNC_STATEMENT_TIMEOUT,
    get_cursors,
    load_raw_logs,
    normalize_message_id,
    sync_directory,
    transform_raw_to_facts,
    update_cursors,
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


def test_transform_raw_to_facts_passes_logid_bounds() -> None:
    con = FakeTransformConnection()

    transformed = transform_raw_to_facts(con, from_logid=10, to_logid=20)

    assert transformed == 3
    assert con.cursor_instance.calls[0] == (
        "SELECT public.egisz_transform_raw_to_facts(%s, %s)",
        (10, 20),
    )
    assert con.committed is True


def test_dwh_init_sql_uses_semd_identifiers_before_transport_host_fallback() -> None:
    sql = _read_dwh_init_sql()

    document_key_view = 'd.document_key AS "Документ (ключ учёта)"'

    assert document_key_view in sql
    assert "CREATE OR REPLACE FUNCTION public.egisz_document_key" in sql
    assert "public.egisz_document_key" in sql
    assert "public.egisz_clean_text_value(t.message_id),\n        t.exchangelog_log_id::text" not in sql
    assert "CREATE OR REPLACE FUNCTION public.egisz_normalize_semd_code" in sql
    assert "public.v_egisz_documents_enriched_ui" in sql
    assert 'd.jid::text AS "JID клиники"' in sql


def test_enriched_mart_is_incrementally_maintained_table_not_full_refresh() -> None:
    sql = _read_dwh_init_sql()
    transform_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "50_transform.sql").read_text(encoding="utf-8")

    # Витрина — persistent-таблица поверх переиспользуемого источника, а не materialized view.
    assert "CREATE OR REPLACE VIEW public.v_egisz_documents_enriched_src" in sql
    assert "CREATE TABLE public.v_egisz_documents_enriched_ui" in sql
    assert "CREATE MATERIALIZED VIEW public.v_egisz_documents_enriched_ui" not in sql

    # Полный REFRESH обогащённой витрины (O(архив) на каждом цикле) удалён из init/DAG-контракта.
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY public.v_egisz_documents_enriched_ui" not in sql
    assert "REFRESH MATERIALIZED VIEW public.v_egisz_documents_enriched_ui" not in sql

    # transform сопровождает витрину инкрементально по затронутым document_key (updated_at = now()).
    assert "INSERT INTO public.v_egisz_documents_enriched_ui" in transform_sql
    assert "FROM public.v_egisz_documents_enriched_src" in transform_sql
    assert "WHERE d.updated_at = now()" in transform_sql
    # Дневной rollup остаётся materialized view.
    assert "CREATE MATERIALIZED VIEW public.v_egisz_documents_daily_ui" in sql


def test_dwh_init_sql_maps_semd_kind_to_reference_oid() -> None:
    sql = _read_dwh_init_sql()
    transform_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "50_transform.sql").read_text(encoding="utf-8")

    assert "INSERT INTO dim_semd_types (code, type_code, name, level, format_code, start_date, end_date, implementation_guide, git_link)" in sql
    assert "oid = EXCLUDED.code" in sql
    assert "SET oid = code" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_dim_semd_types_oid" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_xml_local_uid_norm" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_xml_document_id_norm" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_xml_message_id_norm" in sql
    assert "candidate_log_ids AS" in sql
    assert "public.egisz_xml_text(r.msgtext, 'KIND') AS kind_xml" in sql
    assert "public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'localUid')) AS local_uid_xml" in sql
    assert "public.egisz_clean_text_value(public.egisz_xml_text(r.msgtext, 'DOCUMENTID')) AS document_id_xml" in sql
    assert "COALESCE(r.local_uid_xml, exch_ref.local_uid, gdf_ref.local_uid) AS local_uid_semd" in transform_sql
    assert "public.egisz_clean_text_value(d.local_uid)" in sql
    assert "status_category = CASE" in sql
    # Запись об ЭМД (waiting) появляется при минимальном наборе localUid + JID + KIND,
    # которые собираются по document_key из разных сообщений getDocumentFile.
    assert "document_attributes AS" in transform_sql
    assert "OR (a.jid IS NOT NULL AND a.semd_code IS NOT NULL)" in transform_sql
    # Документный факт по колбэкам по-прежнему строится в document-grain (DISTINCT ON).
    assert "SELECT DISTINCT ON (f.document_key)" in sql
    assert "public.egisz_normalize_semd_code(r.kind_xml) AS semd_code" in sql
    assert "src_doc.semd_code AS source_document_semd_code" in sql
    assert "p.source_document_semd_code" in sql
    assert "WHERE d.oid = n.code" in sql
    assert "WHERE dst.oid = public.egisz_normalize_semd_code(d.semd_code)" in sql
    assert "FROM public.fact_egisz_documents" in sql
    assert "CREATE OR REPLACE VIEW public.fact_egisz_messages AS" not in sql
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
    transform_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "50_transform.sql").read_text(encoding="utf-8")

    assert "Не указан адрес пациента" in sql
    assert "Данные пациента не соответствуют ГИП" in sql
    assert "Документ уже зарегистрирован в РЭМД" in sql
    assert "Не удалось получить файл ЭМД из предоставляющей ИС" in sql
    assert "Ошибка асинхронного ответа" in sql
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
    assert "FROM public.fact_egisz_documents d" in sql
    assert "WHERE d.status = 'network_error'" in sql
    assert "fact_egisz_channel_errors" not in transform_sql


def test_dwh_init_sql_keeps_only_three_reported_emd_statuses() -> None:
    sql = _read_dwh_init_sql()
    transform_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "50_transform.sql").read_text(encoding="utf-8")

    assert "Прокси не отдаёт отдельный статус для синхронного приёма" in sql
    assert "THEN 'success'" in sql
    assert "THEN 'sent'" not in sql
    assert "WHEN t.status = 'sent' THEN 'Отправлен'" not in sql
    assert "WHEN d.status = 'success' THEN 'Успешный ответ'" in sql
    assert "WHEN d.status = 'network_error' THEN 'Ошибка связи'" in sql
    assert "WHEN d.status = 'async_error' THEN 'Ошибка асинхронного ответа'" in sql
    assert "WHERE e.final_status IN ('success', 'error')" in sql
    assert "NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'localUid')), '') IS NOT NULL" in sql
    assert "outbound_ref.document_key" not in sql
    assert "exch_ref.document_key" in sql
    assert "gdf_events AS" in transform_sql
    assert "gdf_ref.document_key" in transform_sql
    assert "exchangelog_raw er" not in transform_sql
    assert "dim_egisz_exchangelog_refs" in sql
    assert "CREATE TABLE IF NOT EXISTS dim_egisz_message_refs" not in sql
    assert "DROP TABLE IF EXISTS public.dim_egisz_message_refs" in sql
    assert "EGISZ_MESSAGES" not in sql
    assert "status = 'waiting'" in sql
    assert "f.error_json_text" in sql
    assert ", message, callback_url" in sql
    assert "error_message," not in transform_sql
    assert "error_message =" not in transform_sql
    rpt_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "80_views_rpt.sql").read_text(encoding="utf-8")
    assert 'NULLIF(TRIM("Документ (ключ учёта)"), \'\') IS NOT NULL' in rpt_sql
    assert 'NULLIF(TRIM("localUid СЭМД"), \'\') IS NOT NULL' not in rpt_sql
    assert "public.egisz_clean_text_value(t.message_id),\n        t.exchangelog_log_id::text" not in sql
    assert "pending_source AS" not in sql
    assert "WHEN e.final_status = 'success' THEN 'Успешно'" not in sql


def test_dwh_init_sql_does_not_keep_legacy_egisz_messages_staging() -> None:
    sql = _read_dwh_init_sql()

    assert "CREATE TABLE IF NOT EXISTS stg_egisz_messages" not in sql
    assert "CREATE TABLE IF NOT EXISTS egisz_messages_raw" not in sql
    assert "INSERT INTO egisz_messages_raw" not in sql
    assert "DROP TABLE IF EXISTS public.egisz_messages_raw CASCADE" in sql
    assert "DROP TABLE IF EXISTS public.stg_egisz_messages CASCADE" in sql


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


def test_get_cursors_reads_last_logid() -> None:
    class Cursor:
        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, _sql: str, _params: tuple[object, ...]) -> None:
            return None

        def fetchone(self) -> tuple[int]:
            return (123,)

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

    assert get_cursors(Connection(), "egisz") == {"last_logid": 123}


def test_update_cursors_upserts_last_logid() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[object, ...]) -> None:
            self.calls.append((sql, params))

    class Connection:
        def __init__(self) -> None:
            self.cursor_instance = Cursor()
            self.committed = False

        def cursor(self) -> Cursor:
            return self.cursor_instance

        def commit(self) -> None:
            self.committed = True

    con = Connection()
    update_cursors(con, "egisz", logid=11)

    assert con.committed is True
    sql, params = con.cursor_instance.calls[0]
    assert "INSERT INTO elt_state (pipeline, last_logid)" in sql
    assert "last_logid = GREATEST(elt_state.last_logid, EXCLUDED.last_logid)" in sql
    assert params == ("egisz", 11)
