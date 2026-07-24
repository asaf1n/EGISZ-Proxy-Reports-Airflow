from __future__ import annotations

import pytest

from pathlib import Path

from egisz_elt.common import (
    connect_pg,
    get_cursors,
    load_raw_logs,
    transform_raw_to_facts,
    update_cursors,
)
from egisz_elt.dimensions import (
    DIRECTORY_SYNC_LOCK_TIMEOUT,
    DIRECTORY_SYNC_PAGE_SIZE,
    DIRECTORY_SYNC_STATEMENT_TIMEOUT,
    sync_directory,
)
from egisz_elt.reconcile import (
    coalesce_logid_windows,
    get_all_raw_logids,
    transform_missing_windows,
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


def test_connect_pg_recovers_cp1251_server_error_text(monkeypatch: pytest.MonkeyPatch) -> None:
    """Русифицированный PostgreSQL отвечает на отказ подключения текстом в cp1251;
    без восстановления реальная причина (пароль/база/pg_hba) прячется за
    UnicodeDecodeError из psycopg2."""
    import psycopg2

    server_message = "ВАЖНО:  пользователь \"egisz\" не прошёл проверку подлинности"
    raw = server_message.encode("cp1251")

    def failing_connect(*_args: object, **_kwargs: object) -> None:
        raw.decode("utf-8")

    monkeypatch.setattr("egisz_elt.common.psycopg2.connect", failing_connect)

    with pytest.raises(psycopg2.OperationalError, match="проверку подлинности") as excinfo:
        connect_pg("postgresql://egisz:wrong@localhost:5432/dwh_egisz")

    assert isinstance(excinfo.value.__cause__, UnicodeDecodeError)


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
        "SELECT public.transform_raw_to_facts(%s, %s, %s)",
        (10, 20, 0),
    )
    assert con.committed is True


def test_dwh_init_sql_uses_semd_identifiers_before_transport_host_fallback() -> None:
    sql = _read_dwh_init_sql()

    assert "d.dwh_id" in sql
    assert "CREATE OR REPLACE FUNCTION public.dwh_id" in sql
    assert "public.dwh_id" in sql
    assert "public.clean_text_value(t.message_id),\n        t.logid::text" not in sql
    assert "CREATE OR REPLACE FUNCTION public.normalize_semd_code" in sql
    assert "public.rpt_documents" in sql
    assert 'f.clinic_jid AS "JID Клиники"' in sql


def test_error_matching_matches_all_rules_independently() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "parts" / "40_functions_errors.sql").read_text(encoding="utf-8")
    assert "error_matching_rule_labels" in sql
    assert "ORDER BY r.rule_code" in sql
    matching_fn = sql.split("error_matching_rule_labels")[1].split("error_item_atoms")[0]
    assert "LIMIT 1" not in matching_fn


def test_error_matching_is_tiered() -> None:
    """Ярусный матчинг: победа первого яруса (min match_tier), внутри яруса — все
    совпадения с дедупом интерпретаций."""
    parts = DWH_INIT_SQL_PATH.parent / "parts"
    rules = (parts / "30_error_rules.sql").read_text(encoding="utf-8")
    assert "match_tier" in rules
    assert "chk_dim_error_rules_match_tier" in rules
    # таксономия: зона ответственности и повторяемость с CHECK-доменом
    assert "responsibility" in rules
    assert "is_retryable" in rules
    assert "chk_dim_error_type_group_responsibility" in rules
    fns = (parts / "40_functions_errors.sql").read_text(encoding="utf-8")
    matching_fn = fns.split("error_matching_rule_labels")[1].split("error_item_atoms")[0]
    assert "min(match_tier)" in matching_fn
    # ИЭМК: RegistryError (атрибуты) парсится отдельной веткой build_errors_json
    assert "CREATE OR REPLACE FUNCTION public.xml_registry_errors" in fns
    build_fn = fns.split("CREATE OR REPLACE FUNCTION public.build_errors_json")[1].split("$$;")[0]
    assert "xml_registry_errors" in build_fn
    # faultcode: локальная часть в UPPERCASE, последним в COALESCE error_code
    parsing = (parts / "20_functions_parsing.sql").read_text(encoding="utf-8")
    assert "faultcode" in parsing
    assert "COALESCE(v_error_code_xml, v_code_xml, v_faultcode)" in parsing


def test_rpt_error_breakdown_exposes_responsibility() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "parts" / "80_views_rpt.sql").read_text(encoding="utf-8")
    breakdown = sql.split("CREATE MATERIALIZED VIEW public.rpt_error_breakdown")[1].split(
        "COMMENT ON MATERIALIZED VIEW public.rpt_error_breakdown")[0]
    assert "responsibility" in breakdown
    assert "is_retryable" in breakdown


def test_error_classify_uses_atomic_item_atoms() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "parts" / "40_functions_errors.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE FUNCTION public.error_item_atoms" in sql
    classify = sql.split("CREATE OR REPLACE FUNCTION public.error_classify")[1].split("$$;")[0]
    assert "error_item_atoms" in classify
    assert "error_interpretation_type" not in classify


def test_rpt_error_breakdown_is_materialized_and_splits_error_types() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "parts" / "80_views_rpt.sql").read_text(encoding="utf-8")
    # Матвью: горячая витрина «Анализ ошибок» предрассчитана и индексирована.
    assert "CREATE MATERIALIZED VIEW public.rpt_error_breakdown" in sql
    breakdown = sql.split("CREATE MATERIALIZED VIEW public.rpt_error_breakdown")[1].split("COMMENT ON MATERIALIZED VIEW public.rpt_error_breakdown")[0]
    assert "string_to_array" in breakdown
    assert "' · '" in breakdown
    # Канонизация set-based: LEFT JOIN к словарю (без построчных подзапросов).
    assert "dim_error_type_group" in breakdown
    assert "public.documents doc" in breakdown
    assert "btrim(doc.error_types)" in breakdown
    # Уникальный индекс нужен для REFRESH ... CONCURRENTLY.
    assert "uq_rpt_error_breakdown" in sql
    # Дроп обоих видов объекта + REFRESH после transform.
    drops = (DWH_INIT_SQL_PATH.parent / "parts" / "60_drop_dependents.sql").read_text(encoding="utf-8")
    assert "DROP MATERIALIZED VIEW public.rpt_error_breakdown CASCADE" in drops


def test_rpt_documents_exposes_error_types_list_only() -> None:
    """rpt_document_versions (база rpt_documents) отдаёт полный список error_types
    как есть из documents; отбор по типу идёт через rpt_error_breakdown.
    rpt_documents = тот же проекшн, отфильтрованный по is_current_version."""
    sql = (DWH_INIT_SQL_PATH.parent / "parts" / "80_views_rpt.sql").read_text(encoding="utf-8")
    rpt = sql.split("CREATE OR REPLACE VIEW public.rpt_document_versions")[1].split("COMMENT ON VIEW public.rpt_document_versions")[0]
    assert "d.error_types" in rpt
    assert "canonical_error_list" not in rpt
    assert "AS error_type," not in rpt
    assert "split_part(" not in rpt


def test_document_version_layer_groups_by_doc_number() -> None:
    """Логический документ = (jid + semd_code + doc_number=PROTOCOLID); localUid — версия.
    CDA setId источником не отдаётся — группируем по журналу."""
    parts = DWH_INIT_SQL_PATH.parent / "parts"
    tables = (parts / "10_tables.sql").read_text(encoding="utf-8")
    transform = (parts / "50_transform.sql").read_text(encoding="utf-8")
    rpt = (parts / "80_views_rpt.sql").read_text(encoding="utf-8")
    health = (parts / "90_views_health_and_finalize.sql").read_text(encoding="utf-8")

    for col in (
        "doc_number",
        "document_group_id",
        "document_group_confidence",
        "semd_version_number",
        "superseded_by_dwh_id",
        "supersedes_dwh_id",
        "is_current_version",
    ):
        assert f"ADD COLUMN IF NOT EXISTS {col}" in tables

    assert "CREATE OR REPLACE FUNCTION public.recompute_document_versions" in transform
    assert "lower(btrim(d.doc_number))" in transform
    assert "'doc_number'" in transform
    assert "c_cap" in transform
    assert "PERFORM public.recompute_document_versions" in transform
    assert "public.recompute_document_versions(NULL::text[])" in health

    assert "CREATE OR REPLACE VIEW public.rpt_document_versions" in rpt
    assert "CREATE OR REPLACE VIEW public.rpt_documents AS" in rpt
    assert "WHERE is_current_version" in rpt
    assert "rpt_health_versions" in health


def test_gdf_chain_lookup_uses_persisted_jid() -> None:
    """jid события getDocumentFile фиксируется при парсинге; gdf_ref ищет цепочку по
    transactions.jid индексом. resolve_document_jid по payload внутри gdf_events при
    полножурнальном lookback reconcile вырождался в O(батч × журнал × regex)."""
    parts = DWH_INIT_SQL_PATH.parent / "parts"
    tables = (parts / "10_tables.sql").read_text(encoding="utf-8")
    transform = (parts / "50_transform.sql").read_text(encoding="utf-8")

    assert "idx_transactions_gdf_jid_logid" in tables

    gdf_events = transform.split("gdf_events AS (")[1].split("raw_parsed AS (")[0]
    assert "tx.jid" in gdf_events
    assert "LATERAL public.resolve_document_jid" not in gdf_events
    assert "JOIN public.exchangelog_raw" not in gdf_events

    # Бэкфилл архива: без него поздний callback не свяжется со старым запросом.
    assert transform.count("AND t.jid IS NULL") == 1
    # Агрегация document_attributes ограничена документами батча, не всем архивом.
    assert "batch_document_ids" in transform


def test_parse_attempts_marker_prevents_reparse_of_uninsertable_rows() -> None:
    """Попытка парсинга фиксируется в exchangelog_parse_attempts. Строки без реквизитов
    (нет msgid/localUid/emdrId/getDocumentFile) в transactions не вставляются, поэтому
    анти-джойн по transactions.xml_parsed_at перепарсивал их каждым полножурнальным
    lookback'ом reconcile (~65 тыс. строк ≈ 6,4 мин на окно)."""
    parts = DWH_INIT_SQL_PATH.parent / "parts"
    tables = (parts / "10_tables.sql").read_text(encoding="utf-8")
    transform = (parts / "50_transform.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS exchangelog_parse_attempts" in tables
    # Бэкфилл маркера из уже распарсенных строк transactions: без него первый
    # полножурнальный lookback перепарсил бы весь архив, а не только «мусор».
    assert "INSERT INTO exchangelog_parse_attempts (logid)" in tables
    assert "SELECT logid FROM transactions WHERE xml_parsed_at IS NOT NULL" in tables
    assert "ANALYZE exchangelog_parse_attempts" in tables

    # Обе ветки parse_targets отбирают кандидатов по маркеру, не по transactions.
    parse_targets = transform.split("parse_targets AS (")[1].split("INSERT INTO public.transactions")[0]
    assert parse_targets.count("public.exchangelog_parse_attempts") == 2
    assert "xml_parsed_at" not in parse_targets

    # Маркер пишется на весь просканированный диапазон после вставки (анти-джойн
    # вставки должен видеть состояние маркера до батча).
    marker = transform.split("INSERT INTO public.exchangelog_parse_attempts (logid)")
    assert len(marker) == 2
    assert "ON CONFLICT (logid) DO NOTHING" in marker[1]
    parse_insert = transform.split("WITH candidate_log_ids AS (")[1]
    assert parse_insert.index("INSERT INTO public.transactions") < parse_insert.index(
        "INSERT INTO public.exchangelog_parse_attempts"
    )


def test_document_attributes_maintained_without_enriched_mart() -> None:
    sql = _read_dwh_init_sql()
    transform_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "50_transform.sql").read_text(encoding="utf-8")
    core_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "70_views_core.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS public.document_attributes" in core_sql
    assert "CREATE OR REPLACE FUNCTION public.reconcile_document_attributes" in core_sql
    assert "CREATE TABLE public.REMOVED_ENRICHED_UI" not in sql
    assert "CREATE MATERIALIZED VIEW public.REMOVED_ENRICHED_UI" not in sql
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY public.REMOVED_ENRICHED_UI" not in sql
    assert "REFRESH MATERIALIZED VIEW public.REMOVED_ENRICHED_UI" not in sql
    assert "reconcile_document_attributes" in transform_sql
    assert "reconcile_document_attributes_ui" in core_sql
    assert "INSERT INTO public.REMOVED_ENRICHED_UI" not in transform_sql
    assert "CREATE MATERIALIZED VIEW public.v_documents_daily_ui" not in sql
    assert "CREATE MATERIALIZED VIEW public.v_egisz_documents_daily_ui" not in sql


def test_rpt_documents_view_has_expected_columns() -> None:
    rpt_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "80_views_rpt.sql").read_text(encoding="utf-8")
    for legacy_name in (
        "Идентификатор документа (localUid)",
        "JID из журнала (gost, число)",
        "JID из gost в REPLYTO",
        "JID (EGISZ_LICENSES)",
        "Токен gost (REPLYTO)",
        "Токен gost (нецифр., для отображения)",
        "Медицинская организация",
        "Регистрационный номер РЭМД",
        "Рег. номер РЭМД (emdrid)",
        "DWH_ID",
        "OID Клиники",
        "OID организации",
        "День (тренд)",
    ):
        assert legacy_name not in rpt_sql
    for column in (
        "dwh_id",
        "status",
        "status_label",
        "status_sort",
        "semd_code",
        "semd_name",
        "semd_label",
        "clinic_jid",
        "clinic_name",
        "clinic_oid",
        "clinic_host",
        "clinic_inn",
        "clinic_jid_mismatch",
        "semd_emdr_id",
        "error_types",
        "error_text",
    ):
        assert column in rpt_sql
    core_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "70_views_core.sql").read_text(encoding="utf-8")
    assert "clinic_oid_xml" in core_sql
    assert "clinic_oid_jpersons" in core_sql
    assert "public.document_source_mismatch" in core_sql
    assert "LEFT JOIN public.dim_document_status ds ON ds.code = d.status" in rpt_sql
    assert "'нет'::text AS \"Расхождение источников JID\"" not in core_sql


def test_connectivity_view_has_no_stale_jid_coalesce() -> None:
    rpt_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "80_views_rpt.sql").read_text(encoding="utf-8")
    assert "JID из журнала" not in rpt_sql
    assert "JID клиники (ключ)" not in rpt_sql
    assert "Ответы РЭМД: успех (документов)" not in rpt_sql
    assert '"Рег. номер РЭМД" AS "Рег. номер РЭМД (emdrid)"' not in rpt_sql
    assert '"Рег. номер РЭМД (emdrid)" AS "Рег. номер РЭМД"' not in rpt_sql


def test_dwh_init_sql_maps_semd_kind_to_reference_oid() -> None:
    sql = _read_dwh_init_sql()
    transform_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "50_transform.sql").read_text(encoding="utf-8")

    assert "INSERT INTO dim_semd_types (code, type_code, name, level, format_code, start_date, end_date, implementation_guide, git_link)" in sql
    assert "oid = EXCLUDED.code" in sql
    assert "SET oid = code" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_dim_semd_types_oid" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_transactions_dwh_id_semd" in sql
    # Функциональные XML-индексы по msgtext не используются transform (parse-once в transactions).
    assert "DROP INDEX IF EXISTS idx_exchangelog_raw_xml_local_uid_norm" in sql
    # DOCUMENTID-парсинг снят вместе с EGISZ_MESSAGES: индекс и реквизит должны быть удалены.
    assert "DROP INDEX IF EXISTS idx_exchangelog_raw_xml_document_id_norm" in sql
    assert "DOCUMENTID" not in sql
    assert "DROP INDEX IF EXISTS idx_exchangelog_raw_xml_message_id_norm" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_xml" not in sql
    assert "candidate_log_ids AS" in sql
    assert "CREATE OR REPLACE FUNCTION public.parse_exchangelog_row" in sql
    assert "CROSS JOIN LATERAL public.parse_exchangelog_row" in transform_sql
    assert "tx.xml_semd_code AS kind_xml" in transform_sql
    assert "tx.xml_local_uid AS local_uid_xml" in transform_sql
    assert "tx.xml_dwh_id AS dwh_id_xml" in transform_sql
    assert "COALESCE(r.local_uid_xml, exch_ref.local_uid, gdf_ref.local_uid) AS local_uid_semd" in transform_sql
    assert "public.clean_text_value(d.local_uid)" in sql
    # status_category удалён как выводимый из status; transform им больше не управляет,
    # а развёрнутые БД чистятся идемпотентным DROP COLUMN.
    assert "status_category = CASE" not in sql
    assert "status_category," not in sql
    assert "DROP COLUMN IF EXISTS status_category" in sql
    assert "document_attributes AS" in transform_sql
    assert "document_resolved AS" in transform_sql
    assert "resolve_document_jid" in transform_sql
    assert "OR (a.resolved_jid IS NOT NULL AND a.semd_code IS NOT NULL)" in transform_sql
    assert "SELECT DISTINCT ON (f.dwh_id)" in sql
    assert "public.normalize_semd_code(r.kind_xml) AS semd_code" in sql
    assert "src_doc.semd_code AS source_document_semd_code" in sql
    assert "p.source_document_semd_code" in sql
    assert "WHERE dst.oid = public.normalize_semd_code(d.semd_code)" in sql
    assert "FROM public.documents" in sql
    assert "CREATE OR REPLACE VIEW public.fact_egisz_messages AS" not in sql
    assert "FROM public.rpt_documents" in sql
    assert "document_group_key" not in sql
    assert "CREATE MATERIALIZED VIEW public.v_documents_daily_ui" not in sql
    assert "p.error_code = 'NO_DOCUMENT_KIND_ON_DATE'" not in sql
    assert "regexp_match(COALESCE(p.msgtext, ''), '\\[([0-9]+)\\]')" not in sql
    assert "regexp_match(COALESCE(r.msgtext, ''), '\\[([0-9]+)\\]')" not in sql
    assert "message_kind" not in sql
    assert "license_kind" not in sql
    assert "documentTypeName" not in sql
    assert "documentName" not in sql


def test_reporting_views_do_not_depend_on_raw_tables() -> None:
    reporting_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "80_views_rpt.sql").read_text(encoding="utf-8")

    assert "exchangelog_raw" not in reporting_sql
    assert "egisz_messages_raw" not in reporting_sql
    assert "stg_egisz_messages" not in reporting_sql
    assert "fact_egisz_messages" not in reporting_sql
    assert "transactions" not in reporting_sql
    assert "dim_exchangelog_refs" not in reporting_sql


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
    assert "CREATE OR REPLACE FUNCTION public.network_error_type" in sql
    assert "dim_error_rules" in sql
    assert "CREATE MATERIALIZED VIEW public.rpt_error_breakdown" in sql
    assert 'AS "Ошибки JSON raw"' not in sql
    assert "error_messages_row" in sql
    assert "FROM public.documents d" in sql
    assert "WHERE r.status = 'network_error'" in sql
    assert "fact_egisz_channel_errors" not in transform_sql


def test_dwh_init_sql_keeps_only_three_reported_emd_statuses() -> None:
    sql = _read_dwh_init_sql()
    transform_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "50_transform.sql").read_text(encoding="utf-8")

    # Синхронный RegisterDocumentResponse = только приём запроса (pending);
    # регистрация подтверждается асинхронным callback'ом.
    assert "приём запроса РЭМД, а не регистрацию документа" in sql
    assert "COALESCE(p_document_status, '') ~* 'зарегистр'" in sql
    assert "'RegisterDocumentResponse'" in sql
    assert "THEN 'success'" in sql
    assert "THEN 'sent'" not in sql
    assert "WHEN t.status = 'sent' THEN 'Отправлен'" not in sql
    assert "CREATE TABLE IF NOT EXISTS dim_document_status" in sql
    assert "('success', 'Успешно зарегистрирован'" in sql
    assert "('network_error', 'Ошибка связи'" in sql
    assert "('async_error', 'Ошибка асинхронного ответа РЭМД'" in sql
    assert "ds.label AS status_label" in sql
    assert "WHEN d.status = 'success' THEN 'Успешно зарегистрирован'" not in sql
    assert "WHERE e.final_status IN ('success', 'error')" in sql
    assert "NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL" in transform_sql
    parsing_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "20_functions_parsing.sql").read_text(encoding="utf-8")
    drop_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "60_drop_dependents.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE FUNCTION public.resolve_document_jid" in parsing_sql
    assert "CREATE OR REPLACE FUNCTION public.jid_from_mo_uid" in parsing_sql
    assert "CREATE OR REPLACE FUNCTION public.jid_from_host" in parsing_sql
    assert "CREATE OR REPLACE FUNCTION public.document_source_mismatch" in parsing_sql
    assert "egisz_xml_text" not in transform_sql
    assert "outbound_ref.dwh_id" not in sql
    assert "exch_ref.dwh_id" in sql
    assert "gdf_events AS" in transform_sql
    assert "gdf_ref.dwh_id" in transform_sql
    assert "exchangelog_raw er" not in transform_sql
    assert "CREATE TABLE IF NOT EXISTS dim_exchangelog_refs" not in sql
    assert "INSERT INTO public.dim_exchangelog_refs" not in transform_sql
    assert "xml_parsed_at" in sql
    assert "CREATE TABLE IF NOT EXISTS dim_egisz_message_refs" not in sql
    assert "DROP TABLE IF EXISTS public.dim_egisz_message_refs" not in drop_sql
    assert "EGISZ_MESSAGES" not in sql
    assert "status = 'waiting'" in sql
    assert "f.error_json_text" in sql
    assert "error_messages_row" in transform_sql
    assert "COALESCE(NULLIF(btrim(f.error_json_text), ''), f.message)" not in transform_sql
    assert ", message, callback_url" in sql
    assert "error_message," not in transform_sql
    assert "error_message =" not in transform_sql
    rpt_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "80_views_rpt.sql").read_text(encoding="utf-8")
    assert "NULLIF(btrim(d.dwh_id), '') IS NOT NULL" in rpt_sql
    assert "DWH_ID" not in rpt_sql
    assert "public.clean_text_value(t.message_id),\n        t.logid::text" not in sql
    assert "pending_source AS" not in sql
    assert "WHEN e.final_status = 'success' THEN 'Успешно'" not in sql


def test_dwh_init_sql_does_not_keep_legacy_egisz_messages_staging() -> None:
    sql = _read_dwh_init_sql()
    drop_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "60_drop_dependents.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS stg_egisz_messages" not in sql
    assert "CREATE TABLE IF NOT EXISTS egisz_messages_raw" not in sql
    assert "INSERT INTO egisz_messages_raw" not in sql
    assert "DROP TABLE IF EXISTS public.egisz_messages_raw CASCADE" not in drop_sql
    assert "DROP TABLE IF EXISTS public.stg_egisz_messages CASCADE" not in drop_sql


class FakeSyncCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.rowcount = 0

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

    def fake_execute_values(
        cursor: object,
        sql: str,
        values: list[tuple[object, ...]],
        page_size: int,
        *,
        fetch: bool = False,
    ) -> None:
        captured["cursor"] = cursor
        captured["sql"] = sql
        captured["values"] = values
        captured["page_size"] = page_size
        captured["fetch"] = fetch
        con.cursor_instance.rowcount = len(values)

    monkeypatch.setattr("egisz_elt.dimensions.execute_values", fake_execute_values)

    changed = sync_directory(con, "dim_organizations", [(1, "Clinic", "1234567890", "Address", "1.2.3")])

    assert changed == 1
    assert con.cursor_instance.calls == [
        ("SET LOCAL lock_timeout = %s", (DIRECTORY_SYNC_LOCK_TIMEOUT,)),
        ("SET LOCAL statement_timeout = %s", (DIRECTORY_SYNC_STATEMENT_TIMEOUT,)),
    ]
    assert captured["cursor"] is con.cursor_instance
    assert "INSERT INTO dim_organizations" in str(captured["sql"])
    assert "IS DISTINCT FROM EXCLUDED." in str(captured["sql"])
    assert captured["values"] == [(1, "Clinic", "1234567890", "Address", "1.2.3")]
    assert captured["page_size"] == DIRECTORY_SYNC_PAGE_SIZE
    assert captured.get("fetch", False) is False
    assert con.committed is True


def test_get_cursors_reads_last_logid_only() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.sql = ""

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: str, _params: tuple[object, ...]) -> None:
            self.sql = sql

        def fetchone(self) -> tuple[int]:
            return (123,)

    class Connection:
        def __init__(self) -> None:
            self.cursor_instance = Cursor()

        def cursor(self) -> Cursor:
            return self.cursor_instance

    con = Connection()
    assert get_cursors(con, "egisz") == {"last_logid": 123}
    assert "source_min_created_at" not in con.cursor_instance.sql


def test_get_cursors_returns_defaults_when_pipeline_missing() -> None:
    class Cursor:
        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, _sql: str, _params: tuple[object, ...]) -> None:
            return None

        def fetchone(self) -> None:
            return None

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

    assert get_cursors(Connection(), "egisz") == {"last_logid": 0}


def test_get_all_raw_logids_returns_int_set_over_full_table() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.sql = ""

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            self.sql = sql

        def fetchall(self) -> list[tuple[int]]:
            return [(101,), (102,), (102,)]

    class Connection:
        def __init__(self) -> None:
            self.cursor_instance = Cursor()

        def cursor(self) -> Cursor:
            return self.cursor_instance

    con = Connection()
    assert get_all_raw_logids(con) == {101, 102}
    assert con.cursor_instance.sql == "SELECT logid FROM exchangelog_raw"


def test_get_all_raw_logids_filters_by_since() -> None:
    from datetime import datetime, timezone

    class Cursor:
        def __init__(self) -> None:
            self.sql = ""
            self.params: tuple[object, ...] | None = None

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
            self.sql = sql
            self.params = params

        def fetchall(self) -> list[tuple[int]]:
            return [(101,)]

    class Connection:
        def __init__(self) -> None:
            self.cursor_instance = Cursor()

        def cursor(self) -> Cursor:
            return self.cursor_instance

    since = datetime(2026, 6, 1, tzinfo=timezone.utc)
    con = Connection()
    assert get_all_raw_logids(con, since=since) == {101}
    assert "COALESCE(createdate, logdate) >= %s" in con.cursor_instance.sql
    assert con.cursor_instance.params == (since,)


def test_coalesce_logid_windows_merges_runs_within_gap() -> None:
    # 100..102 dense; 5000 far apart; default max_gap=0 merges only consecutive LOGIDs.
    assert coalesce_logid_windows([102, 100, 101, 5000]) == [(100, 102), (5000, 5000)]


def test_coalesce_logid_windows_keeps_non_adjacent_separate() -> None:
    # Gaps wider than max_gap+1 stay separate unless max_gap is raised explicitly.
    assert coalesce_logid_windows([100, 300, 1000]) == [(100, 100), (300, 300), (1000, 1000)]
    assert coalesce_logid_windows([100, 300, 1000], max_gap=199) == [(100, 300), (1000, 1000)]
    assert coalesce_logid_windows([100, 300, 1000], max_gap=500) == [(100, 300), (1000, 1000)]


def test_coalesce_logid_windows_empty() -> None:
    assert coalesce_logid_windows([]) == []


def test_transform_missing_windows_calls_transform_per_window() -> None:
    calls: list[tuple[int, int, int]] = []

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, _sql: str, params: tuple[int, int, int]) -> None:
            calls.append(params)

        def fetchone(self) -> tuple[int]:
            return (2,)

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            return None

    total = transform_missing_windows(FakeConnection(), [100, 101, 5000])

    assert total == 4
    assert calls == [(99, 101, 100), (4999, 5000, 5000)]


def test_dwh_init_sql_drops_source_min_created_at_from_elt_state() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "parts" / "10_tables.sql").read_text(encoding="utf-8")

    # Дата-отсечка источника снята целиком: ни колонки, ни date-seed.
    assert "ALTER TABLE elt_state DROP COLUMN IF EXISTS source_min_created_at" in sql
    assert "source_min_created_at timestamptz" not in sql
    assert "INSERT INTO elt_state (pipeline, last_logid)\nVALUES ('egisz', 0)" in sql
    assert "2026-05-18" not in sql
    assert "SOURCE_MIN_CREATED_AT" not in sql


def test_dwh_init_sql_partitions_time_series_tables() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "parts" / "10_tables.sql").read_text(encoding="utf-8")
    transform_sql = (DWH_INIT_SQL_PATH.parent / "parts" / "50_transform.sql").read_text(encoding="utf-8")

    assert "PARTITION BY RANGE (createdate)" in sql
    assert "PARTITION BY RANGE (log_date)" in sql
    assert "PRIMARY KEY (logid, createdate)" in sql
    assert "PRIMARY KEY (logid, log_date)" in sql
    assert "exchangelog_raw_default PARTITION OF public.exchangelog_raw DEFAULT" in sql
    assert "transactions_default PARTITION OF public.transactions DEFAULT" in sql
    assert "relkind <> 'p'" in sql
    assert "ON CONFLICT (logid, log_date) DO UPDATE SET" in transform_sql
    assert "ON CONFLICT (logid, log_date)" in transform_sql


def test_load_raw_logs_uses_partitioned_upsert_target() -> None:
    import inspect

    from egisz_elt import common

    source = inspect.getsource(common.load_raw_logs)
    assert "ON CONFLICT (logid, createdate)" in source


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
