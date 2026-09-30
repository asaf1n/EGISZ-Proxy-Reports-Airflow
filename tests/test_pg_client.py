from __future__ import annotations

import pytest

from pathlib import Path

from conftest import load_dag_module, sql_section

# Общие функции берём из ETL-DAG: он канонический носитель общего блока,
# идентичность копий в соседних DAG-файлах проверяет test_dag_selfcontainment.py.
extract_dag = load_dag_module("egisz_etl_dag")
connect_pg = extract_dag.connect_pg
get_cursors = extract_dag.get_cursors
load_raw_logs = extract_dag.load_raw_logs
RAW_LOG_COLUMNS = extract_dag.RAW_LOG_COLUMNS
transform_raw_to_facts = extract_dag.transform_raw_to_facts
update_cursors = extract_dag.update_cursors

refresh_dag = extract_dag  # общий блок живёт в DAG фактов
DIRECTORY_MERGE_EXPRESSIONS = refresh_dag.DIRECTORY_MERGE_EXPRESSIONS
DIRECTORY_SYNC_LOCK_TIMEOUT = refresh_dag.DIRECTORY_SYNC_LOCK_TIMEOUT
DIRECTORY_SYNC_PAGE_SIZE = refresh_dag.DIRECTORY_SYNC_PAGE_SIZE
DIRECTORY_SYNC_STATEMENT_TIMEOUT = refresh_dag.DIRECTORY_SYNC_STATEMENT_TIMEOUT
sync_directory = refresh_dag.sync_directory

maintenance_dag = load_dag_module("egisz_maintenance_dag")
coalesce_logid_windows = maintenance_dag.coalesce_logid_windows
fetch_raw_logids_range = maintenance_dag.fetch_raw_logids_range
transform_missing_windows = maintenance_dag.transform_missing_windows

DWH_INIT_SQL_PATH = Path(__file__).resolve().parents[1] / "db" / "dwh_init.sql"


def _read_dwh_init_sql() -> str:
    # Находим папку db
    parts_dir = DWH_INIT_SQL_PATH.parent
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

    monkeypatch.setattr("egisz_etl_dag.psycopg2.connect", failing_connect)

    with pytest.raises(psycopg2.OperationalError, match="проверку подлинности") as excinfo:
        connect_pg("postgresql://egisz:wrong@localhost:5432/dwh_bi")

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


def test_load_raw_logs_strips_embedded_nul_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    """EXCHANGELOG изредка приносит битые SOAP-тела с 0x00 внутри LOGTEXT/MSGTEXT;
    psycopg2 отказывается строить текстовый литерал с NUL ещё до обращения к серверу."""
    row = {
        "logid": 1,
        "logdate": "2026-05-07T15:00:00",
        "createdate": "2026-05-07T14:59:00",
        "msgid": "message-1",
        "logstate": 1,
        "logtext": "before\x00after",
        "msgtext": "clean",
        "uri": "http://example\x00.invalid",
    }
    captured: dict[str, list[tuple[object, ...]]] = {}

    def fake_execute_values(_cur, _sql, values, *_args, **_kwargs) -> None:
        captured["values"] = list(values)

    monkeypatch.setattr("egisz_etl_dag.execute_values", fake_execute_values)

    class Cursor:
        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

        def commit(self) -> None:
            return None

    load_raw_logs(Connection(), [row])

    (loaded,) = captured["values"]
    assert "\x00" not in loaded[RAW_LOG_COLUMNS.index("logtext")]
    assert "\x00" not in loaded[RAW_LOG_COLUMNS.index("uri")]
    assert loaded[RAW_LOG_COLUMNS.index("logtext")] == "beforeafter"


class FakeTransformCursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.result: tuple[object] = ({"transformed": 3},)

    def __enter__(self) -> "FakeTransformCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.calls.append((sql, params))

    def fetchone(self) -> tuple[object]:
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

    assert transformed == {"transformed": 3}
    assert con.cursor_instance.calls[0] == (
        "SELECT mart_egisz.transform_raw_to_facts(%s, %s)",
        (10, 20),
    )
    assert con.committed is True


def test_dwh_init_sql_uses_semd_identifiers_before_transport_host_fallback() -> None:
    sql = _read_dwh_init_sql()

    assert "d.dwh_id" in sql
    assert "CREATE OR REPLACE FUNCTION stg_egisz.dwh_id" in sql
    assert "stg_egisz.dwh_id" in sql
    assert "stg_egisz.clean_text_value(t.message_id),\n        t.logid::text" not in sql
    assert "stg_egisz.clean_text_value(t.msgid),\n        t.logid::text" not in sql
    assert "CREATE OR REPLACE FUNCTION stg_egisz.normalize_semd_code" in sql
    assert "serving_egisz.document_versions" in sql
    assert 'f.clinic_jid AS "JID Клиники"' in sql


def test_error_classification_takes_one_rule_per_element() -> None:
    """Элемент получает ровно один тип: первый ярус с совпадением, внутри яруса — правило
    с меньшим rule_code."""
    sql = (DWH_INIT_SQL_PATH.parent / "02_functions.sql").read_text(encoding="utf-8")
    classify = sql.split("CREATE OR REPLACE FUNCTION stg_egisz.classify_error")[1].split("$$;")[0]
    assert "FOR v_tier IN 1..4 LOOP" in classify
    assert "ORDER BY r.rule_code\n            LIMIT 1;" in classify
    assert "error_matching_rule_labels" not in sql
    assert "error_item_atoms" not in sql


def test_error_rules_dictionary_contract() -> None:
    """Справочник правил несёт классификацию по ярусам и шаги маскирования; зона
    ответственности и признак повтора наследуются от категории."""
    rules = (DWH_INIT_SQL_PATH.parent / "02_functions.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS mart_egisz.dim_error_rules" in rules
    assert "chk_dim_error_rules_kind" in rules
    assert "(match_tier <= 2) = (match_code IS NOT NULL)" in rules
    assert "CREATE TABLE IF NOT EXISTS mart_egisz.dim_error_category" in rules
    assert "CREATE TABLE IF NOT EXISTS mart_egisz.dim_error_type" in rules
    assert "responsibility IN ('клиника', 'МИС', 'интегратор', 'РЭМД', 'смешанная')" in rules
    assert "is_active" not in rules
    remd = rules.split("CREATE OR REPLACE FUNCTION stg_egisz.remd_error_items")[1].split("$$;")[0]
    assert "registrationWarnings" in remd
    ihe = rules.split("CREATE OR REPLACE FUNCTION stg_egisz.ihe_error_items")[1].split("$$;")[0]
    assert "RegistryError" in ihe
    for attribute in ("errorCode", "codeContext", "severity", "location"):
        assert f"'{attribute}'" in ihe
    # faultcode: локальная часть в UPPERCASE, последним в COALESCE error_code
    assert "faultcode" in rules
    assert "COALESCE(v_error_code_xml, v_code_xml, v_faultcode)" in rules


def test_document_error_exposes_responsibility() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    view = sql.split("CREATE MATERIALIZED VIEW serving_egisz.document_errors AS")[1].split(
        "COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors")[0]
    assert "t.responsibility" in view
    assert "t.is_retryable" in view


def test_errors_are_parsed_per_source_and_classified_once_per_batch() -> None:
    """Каждый источник разбирается своей функцией в свои столбцы stg; одинаковые элементы
    классифицируются один раз; приём разбирает ошибки пакета той же функцией, что и
    повторный разбор журнала."""
    schema = (DWH_INIT_SQL_PATH.parent / "01_schema.sql").read_text(encoding="utf-8")
    for column in ("network_error_code text", "network_error_text text", "network_error_type text",
                   "remd_errors jsonb", "ihe_errors jsonb"):
        assert column in schema
    transform = (DWH_INIT_SQL_PATH.parent / "03_transform.sql").read_text(encoding="utf-8")
    parse = transform.split("CREATE OR REPLACE FUNCTION stg_egisz.parse_message_errors")[1].split("$$;")[0]
    assert "stg_egisz.remd_error_items(r.msgtext)" in parse
    assert "stg_egisz.ihe_error_items(r.msgtext)" in parse
    assert "stg_egisz.network_error_code(r.logtext)" in parse
    assert "SELECT DISTINCT error_kind, error_code, error_text FROM pg_temp.message_error_items" in parse
    assert "CROSS JOIN LATERAL stg_egisz.classify_error(k.error_kind, k.error_code, k.error_text) c" in parse
    assert "PERFORM stg_egisz.parse_message_errors(from_logid, to_logid);" in transform


def test_current_document_errors_are_built_above_stage_in_common_form() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    common = sql.split("CREATE VIEW mart_egisz.message_errors AS")[1].split("COMMENT ON VIEW mart_egisz.message_errors")[0]
    for source in ("FROM stg_egisz.network_errors n", "FROM stg_egisz.remd_errors r", "FROM stg_egisz.ihe_errors h"):
        assert source in common
    assert "r.section IS NOT DISTINCT FROM 'registrationWarnings'" in common
    assert "h.severity ~* 'Warning$'" in common
    current = sql.split("CREATE MATERIALIZED VIEW mart_egisz.document_errors AS")[1].split(
        "COMMENT ON MATERIALIZED VIEW mart_egisz.document_errors")[0]
    assert "t.status IN ('success', 'error')" in current
    assert "m.message_at >= COALESCE(lr.responded_at, '-infinity'::timestamptz)" in current
    assert "FROM mart_egisz.message_errors m" in current
    view = sql.split("CREATE MATERIALIZED VIEW serving_egisz.document_errors AS")[1].split(
        "COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors")[0]
    assert "FROM mart_egisz.document_errors c" in view
    assert "LEFT JOIN mart_egisz.dim_error_type t" in view
    assert "c.is_warning" in view
    # Уникальный индекс нужен для REFRESH ... CONCURRENTLY.
    assert "ON serving_egisz.document_errors (dwh_id, error_no)" in sql


def test_source_error_text_stays_in_parsing_layer() -> None:
    """Исходный текст ошибки хранится в слое разбора: общая форма и опубликованные ошибки его
    не несут, служебная витрина собирает его по ключу источника."""
    sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")

    def body(start: str, end: str) -> str:
        return sql.split(start)[1].split(end)[0]

    network = body("CREATE VIEW stg_egisz.network_errors AS", "COMMENT ON VIEW stg_egisz.network_errors")
    remd = body("CREATE VIEW stg_egisz.remd_errors AS", "COMMENT ON VIEW stg_egisz.remd_errors")
    ihe = body("CREATE VIEW stg_egisz.ihe_errors AS", "COMMENT ON VIEW stg_egisz.ihe_errors")
    common = body("CREATE VIEW mart_egisz.message_errors AS", "COMMENT ON VIEW mart_egisz.message_errors")
    document = body("CREATE MATERIALIZED VIEW serving_egisz.document_errors AS",
                    "COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors")
    texts = body("CREATE MATERIALIZED VIEW mart_egisz_admin.document_error_texts AS",
                 "COMMENT ON MATERIALIZED VIEW mart_egisz_admin.document_error_texts")

    assert 'tx.network_error_text COLLATE "und-x-icu" AS error_text' in network
    assert 'e.message COLLATE "und-x-icu" AS message' in remd
    assert 'e.code_context COLLATE "und-x-icu" AS code_context' in ihe
    assert "error_text" not in common
    assert "error_text" not in document
    assert "COALESCE(n.error_text, r.message, h.code_context)" in texts


def test_document_versions_carry_no_error_columns() -> None:
    """Ошибки документа — отдельная витрина: у документной витрины нет колонок текста и
    типа ошибки."""
    sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    versions = sql.split("CREATE OR REPLACE VIEW serving_egisz.document_versions")[1].split("COMMENT ON VIEW serving_egisz.document_versions")[0]
    assert "error_types" not in versions
    assert "error_text" not in versions


def test_document_version_layer_groups_by_doc_number() -> None:
    """Логический документ = (jid + semd_code + doc_number=PROTOCOLID); localUid — версия.
    CDA setId источником не отдаётся — группируем по журналу."""
    parts = DWH_INIT_SQL_PATH.parent
    tables = (parts / "01_schema.sql").read_text(encoding="utf-8")
    transform = (parts / "03_transform.sql").read_text(encoding="utf-8")
    views = (parts / "04_views.sql").read_text(encoding="utf-8")
    documents_contract = tables.split("CREATE TABLE IF NOT EXISTS mart_egisz.documents (", 1)[1].split(");", 1)[0]

    for col in (
        "doc_number",
        "document_group_id",
        "document_group_confidence",
        "semd_version_number",
        "superseded_by_dwh_id",
        "supersedes_dwh_id",
        "is_current_version",
    ):
        assert f"    {col} " in documents_contract

    assert "CREATE OR REPLACE FUNCTION mart_egisz.recompute_document_versions" in transform
    assert "lower(btrim(d.doc_number))" in transform
    assert "'doc_number'" in transform
    assert "c_cap" in transform
    assert "PERFORM mart_egisz.recompute_document_versions" in transform
    assert "mart_egisz.recompute_document_versions(NULL::text[])" in views

    assert "CREATE OR REPLACE VIEW serving_egisz.document_versions" in views
    assert "r.is_current_version" in views
    assert "health_versions" in views


def test_response_links_to_document_through_message_registry() -> None:
    """Ответ ЕГИСЗ не несёт localUid: документ находится по relatesToMessage через
    реестр подач stg_egisz.message_registry. Ключ приводится к каноническому виду одной
    функцией на обеих сторонах — к MSGID подачи в представлении и при поиске."""
    parts = DWH_INIT_SQL_PATH.parent
    tables = (parts / "01_schema.sql").read_text(encoding="utf-8")
    parsing = (parts / "02_functions.sql").read_text(encoding="utf-8")
    transform = (parts / "03_transform.sql").read_text(encoding="utf-8")

    # Сырой слой хранит реестр как в источнике; правила — в представлении слоя разбора.
    assert "CREATE TABLE IF NOT EXISTS raw_egisz.egisz_messages" in tables
    assert "msgid text PRIMARY KEY" not in tables
    assert "idx_egisz_messages_egmid" in tables
    assert "CREATE TRIGGER" not in tables
    registry = parsing.split("CREATE OR REPLACE VIEW stg_egisz.message_registry AS", 1)[1].split(";", 1)[0]
    assert "stg_egisz.message_registry_key(m.msgid) AS msgid" in registry
    assert "WHEN stg_egisz.egisz_subsystem(NULL, NULL, m.replyto) = 'ИЭМК' THEN NULL" in registry
    assert "ELSE stg_egisz.dwh_id(m.documentid)" in registry
    assert "CREATE OR REPLACE FUNCTION stg_egisz.message_registry_key" in parsing

    msg_ref = transform.split("        ) msg_ref ON TRUE")[0].rsplit("LEFT JOIN LATERAL (", 1)[1]
    assert "FROM stg_egisz.message_registry m" in msg_ref
    assert "m.msgid = stg_egisz.message_registry_key(r.relates_to_msgid)" in msg_ref
    assert "EGISZ_MESSAGES" not in msg_ref
    assert "ORDER BY (m.document_uid IS NOT NULL) DESC, m.egmid DESC NULLS LAST" in msg_ref
    assert "true AS has_registry" in msg_ref

    # Правило привязки фиксируется на строке.
    assert "AS link_method" in transform
    assert "'message_registry'" in transform
    assert "'message_registry_no_document'" in transform
    assert "'unlinked'" in transform
    assert "AND reg.document_uid IS NULL" in transform
    assert "tx.egisz_subsystem IS DISTINCT FROM 'ИЭМК'" in transform
    assert "NOT EXISTS (\n          SELECT 1\n          FROM stg_egisz.message_registry m" in transform
    # Индексы вне текущего правила связки отсутствуют.
    assert "idx_transactions_gdf_jid_logid" not in tables
    assert "AND t.jid IS NULL" not in transform
    # Агрегация реквизитов ограничена документами батча, не всем архивом.
    assert "batch_document_ids" in transform


def test_msgid_contract_uses_two_canonical_names() -> None:
    parts = DWH_INIT_SQL_PATH.parent
    tables = (parts / "01_schema.sql").read_text(encoding="utf-8")
    parsing = (parts / "02_functions.sql").read_text(encoding="utf-8")
    transform = (parts / "03_transform.sql").read_text(encoding="utf-8")

    parse_contract = parsing.split("CREATE OR REPLACE FUNCTION stg_egisz.parse_exchangelog_row", 1)[1].split(
        "RETURNS TABLE (", 1
    )[1].split(")", 1)[0]
    assert "msgid text" in parse_contract
    assert "relates_to_msgid text" in parse_contract
    assert "exchange_msgid_norm" not in parsing
    assert "p.exchange_msgid_norm" not in transform
    assert "stg_egisz.normalize_message_id(COALESCE(NULLIF(btrim(p_msgid), ''), v_message_id_xml))" in parsing
    assert "stg_egisz.normalize_message_id(COALESCE(v_relates_to_message, v_relates_to))" in parsing

    transaction_contract = tables.split("CREATE TABLE IF NOT EXISTS stg_egisz.exchange_messages (", 1)[1].split(");", 1)[0]
    assert "msgid text" in transaction_contract
    assert "relates_to_msgid text" in transaction_contract
    assert "message_id text" not in transaction_contract
    assert "relates_to_id text" not in transaction_contract

    documents_contract = tables.split("CREATE TABLE IF NOT EXISTS mart_egisz.documents (", 1)[1].split(");", 1)[0]
    assert "msgid text" in documents_contract
    assert "relates_to_msgid text" in documents_contract
    assert "result_msgid text" not in documents_contract


def test_get_document_file_sent_requires_registry_and_excludes_linked_emd() -> None:
    transform = (DWH_INIT_SQL_PATH.parent / "03_transform.sql").read_text(encoding="utf-8")
    sent_branch = transform.split("-- Ветка запроса: getDocumentFile", 1)[1].split(
        "-- Отправки, по которым клиника", 1
    )[0]

    assert "tx.source_action = 'getDocumentFile'" in sent_branch
    assert "AND NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL" in sent_branch
    assert "AND NULLIF(btrim(tx.xml_emdr_id), '') IS NULL" in sent_branch
    assert "FROM stg_egisz.message_registry m" in sent_branch
    assert "WHERE m.document_uid = tx.xml_dwh_id" in sent_branch
    assert "WHERE m.document_uid = a.dwh_id" in sent_branch
    assert "status, first_sent_at, request_logid, msgid" in sent_branch
    assert "a.sent_msgid" in sent_branch
    assert "relates_to_msgid" not in sent_branch.split("INSERT INTO mart_egisz.documents", 1)[1].split(
        "ON CONFLICT", 1
    )[0]


def test_parse_attempts_marker_prevents_reparse_of_uninsertable_rows() -> None:
    """Попытка парсинга фиксируется в egisz_exchangelog_parse_attempts. Строки без реквизитов
    (нет msgid/localUid/emdrId/getDocumentFile) в exchange_messages не вставляются, поэтому
    анти-джойн по exchange_messages.xml_parsed_at перепарсивал их каждым полножурнальным
    lookback'ом reconcile (~65 тыс. строк ≈ 6,4 мин на окно)."""
    parts = DWH_INIT_SQL_PATH.parent
    tables = (parts / "01_schema.sql").read_text(encoding="utf-8")
    transform = (parts / "03_transform.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS etl_meta.egisz_exchangelog_parse_attempts" in tables
    # Схема описывает конечное состояние: разовое наполнение маркера в ней не живёт.
    assert "INSERT INTO etl_meta.egisz_exchangelog_parse_attempts" not in tables

    # Обе ветки parse_targets отбирают кандидатов по маркеру, не по exchange_messages.
    parse_targets = transform.split("parse_targets AS (")[1].split("INSERT INTO stg_egisz.exchange_messages")[0]
    assert parse_targets.count("etl_meta.egisz_exchangelog_parse_attempts") == 1
    assert "xml_parsed_at" not in parse_targets

    # Маркер пишется на весь просканированный диапазон после вставки (анти-джойн
    # вставки должен видеть состояние маркера до батча).
    marker = transform.split("INSERT INTO etl_meta.egisz_exchangelog_parse_attempts (logid)")
    assert len(marker) == 2
    assert "ON CONFLICT (logid) DO NOTHING" in marker[1]
    parse_insert = transform.split("WITH parse_targets AS (")[1]
    assert parse_insert.index("INSERT INTO stg_egisz.exchange_messages") < parse_insert.index(
        "INSERT INTO etl_meta.egisz_exchangelog_parse_attempts"
    )


def test_document_attributes_maintained_without_enriched_mart() -> None:
    sql = _read_dwh_init_sql()
    transform_sql = (DWH_INIT_SQL_PATH.parent / "03_transform.sql").read_text(encoding="utf-8")
    core_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS mart_egisz.document_attributes" in core_sql
    assert "CREATE OR REPLACE FUNCTION mart_egisz.recompute_document_attributes" in core_sql
    assert "recompute_document_attributes" in transform_sql
    # Параметр по умолчанию покрывает полный проход.
    assert "CREATE OR REPLACE FUNCTION public.reconcile_document_attributes" not in core_sql
    assert "reconcile_document_attributes" not in core_sql
    assert "CREATE MATERIALIZED VIEW public.v_documents_daily_ui" not in sql
    assert "CREATE MATERIALIZED VIEW public.v_egisz_documents_daily_ui" not in sql


def test_document_views_have_expected_columns() -> None:
    views_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
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
        assert legacy_name not in views_sql
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
        "clinic_oid_unknown",
        "semd_emdr_id",
    ):
        assert column in views_sql
    assert "clinic_oid_xml" in views_sql
    # Реквизиты, снятые вместе с отказом от лицензий в резолве: OID берётся из обмена,
    # признак «OID вне реестра» считается на чтении поверх dim_clinic_oid.
    assert "a.clinic_oid_jpersons" not in views_sql
    assert "a.clinic_oid_license" not in views_sql
    assert "a.clinic_jid_mismatch" not in views_sql
    assert "public.document_source_mismatch(" not in views_sql
    assert "mart_egisz.dim_clinic_oid r" in views_sql
    assert "LEFT JOIN mart_egisz.dim_document_status ds ON ds.code = d.status" in views_sql
    assert "'нет'::text AS \"Расхождение источников JID\"" not in views_sql


def test_connectivity_view_has_no_stale_jid_coalesce() -> None:
    rpt_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    assert "JID из журнала" not in rpt_sql
    assert "JID клиники (ключ)" not in rpt_sql
    assert "Ответы РЭМД: успех (документов)" not in rpt_sql
    assert '"Рег. номер РЭМД" AS "Рег. номер РЭМД (emdrid)"' not in rpt_sql
    assert '"Рег. номер РЭМД (emdrid)" AS "Рег. номер РЭМД"' not in rpt_sql


def test_dwh_init_sql_maps_semd_kind_to_reference_oid() -> None:
    sql = _read_dwh_init_sql()
    transform_sql = (DWH_INIT_SQL_PATH.parent / "03_transform.sql").read_text(encoding="utf-8")

    assert "INSERT INTO mart_egisz.dim_semd_types (code, type_code, name, level, format_code, start_date, end_date, implementation_guide, ig_oid)" in sql
    assert "oid = EXCLUDED.code" in sql
    assert "SET oid = code" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_dim_semd_types_oid" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_exchange_messages_dwh_id_semd" in sql
    # Функциональные XML-индексы по msgtext не используются transform (parse-once в exchange_messages).
    assert "idx_exchangelog_raw_xml" not in sql
    assert "candidate_log_ids AS" in sql
    assert "CREATE OR REPLACE FUNCTION stg_egisz.parse_exchangelog_row" in sql
    assert "CROSS JOIN LATERAL stg_egisz.parse_exchangelog_row" in transform_sql
    assert "tx.xml_semd_code AS kind_xml" in transform_sql
    assert "tx.xml_local_uid AS local_uid_xml" in transform_sql
    assert "tx.xml_dwh_id AS dwh_id_xml" in transform_sql
    assert "COALESCE(r.local_uid_xml, msg_ref.local_uid) AS local_uid_semd" in transform_sql
    assert "stg_egisz.clean_text_value(d.local_uid)" in sql
    # status_category выводится из status и в схеме не объявлен.
    assert "status_category" not in sql
    assert "document_attributes AS" in transform_sql
    assert "document_resolved AS" in transform_sql
    assert "resolve_document_jid" in transform_sql
    assert "AND a.resolved_jid IS NOT NULL" in transform_sql
    assert "has_network_error" not in transform_sql
    assert "SELECT DISTINCT ON (f.dwh_id)" in sql
    assert "stg_egisz.normalize_semd_code(r.kind_xml) AS semd_code" in sql
    assert "src_doc.semd_code AS source_document_semd_code" in sql
    assert "p.source_document_semd_code" in sql
    assert "WHERE dst.oid = stg_egisz.normalize_semd_code(d.semd_code)" in sql
    assert "FROM mart_egisz.documents" in sql
    assert "CREATE OR REPLACE VIEW public.fact_egisz_messages AS" not in sql
    assert "FROM serving_egisz.document_versions" in sql
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
    views_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    # Только слой выдачи документов: message-грейн он не читает. Секция document_attributes сюда не
    # входит — она как раз и переносит реквизиты с грейна exchange_messages на документ,
    # чтобы отчётному слою не приходилось этого делать.
    reporting_sql = "\n".join(
        line.split("--", 1)[0]
        for line in sql_section(views_sql, "document_versions").splitlines()
    )

    assert "raw_egisz." not in reporting_sql
    assert "egisz_messages_raw" not in reporting_sql
    assert "stg_egisz_messages" not in reporting_sql
    assert "fact_egisz_messages" not in reporting_sql
    assert "exchange_messages" not in reporting_sql
    assert "dim_exchangelog_refs" not in reporting_sql


def test_health_journal_continuity_allows_processed_raw_retention() -> None:
    views_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    health_sql = sql_section(views_sql, "health")

    assert "r.logid > COALESCE(s.transform_logid_cursor, 0)" in health_sql
    assert "разрывы LOGID в необработанном хвосте raw_egisz.exchangelog" in health_sql
    assert "Разобранный raw можно архивировать" in health_sql


def test_dwh_init_sql_interprets_patient_address_schematron_and_network_errors() -> None:
    sql = _read_dwh_init_sql()
    transform_sql = (DWH_INIT_SQL_PATH.parent / "03_transform.sql").read_text(encoding="utf-8")

    # Наименования типов — формулировки классификатора ФНСИ 1.2.643.5.1.13.13.99.2.305.
    assert "mart_egisz.dim_nsi_error_code" in sql
    assert "1.2.643.5.1.13.13.99.2.305" in sql
    assert "Адрес пациента: атрибуты элемента address:Type не соответствуют требованиям" in sql
    assert "Данные пациента с переданным локальным идентификатором отличаются от зарегистрированных в ГИП" in sql
    assert "Документ с указанным идентификатором (в РМИС/МИС) уже зарегистрирован" in sql
    assert "Ошибка при получении файла документа из предоставляющей системы" in sql
    # Трактовки, разошедшиеся со справочником, сняты вместе с выдуманными кодами.
    assert "Не указан адрес пациента" not in sql
    assert "Срок действия сертификата организации истек" not in sql
    assert "ORGANIZATION_NOT_REGISTERED" not in sql
    assert "CA_UNAVAILABLE" not in sql
    assert "Отказ РЭМД (ns2status: error)" not in sql
    # Ошибка связи — вид ошибки с исходным текстом шлюза; ни синтетического кода, ни
    # подставленных формулировок.
    assert "INTEGRATION_LOGSTATE_3" not in sql
    assert "Сетевая ошибка: " not in sql
    assert "'Сетевая ошибка'" not in sql
    assert "'нет деталей'" not in sql
    assert "'Неизвестная ошибка'" not in sql
    assert "'(без текста)'" not in sql
    assert "Наименование СЭМД отсутствует в справочнике СЭМД" in sql
    assert "CREATE OR REPLACE FUNCTION stg_egisz.remd_error_items" in sql
    assert "CREATE OR REPLACE FUNCTION stg_egisz.ihe_error_items" in sql
    assert "CREATE OR REPLACE FUNCTION stg_egisz.classify_error" in sql
    assert "CREATE MATERIALIZED VIEW serving_egisz.document_errors" in sql
    assert "CREATE VIEW serving_egisz.network_errors" in sql
    assert "CASE WHEN p.logstate = 3 THEN p.logtext ELSE p.xml_message END AS message_text" in transform_sql
    assert "fact_egisz_channel_errors" not in transform_sql


def test_dwh_init_sql_keeps_only_three_reported_emd_statuses() -> None:
    sql = _read_dwh_init_sql()
    transform_sql = (DWH_INIT_SQL_PATH.parent / "03_transform.sql").read_text(encoding="utf-8")

    # Статус документа определяет асинхронный ответ; сбой доставки статусом не является.
    classify = sql.split("CREATE OR REPLACE FUNCTION stg_egisz.classify_async_status")[1].split("$$;")[0]
    assert "p_source_action = 'sendRegisterDocumentResult'" in classify
    assert "COALESCE(p_document_status, '') ~* 'зарегистр'" in classify
    assert "ResponseStatusType" not in classify
    assert "p_registry_response_status = 'Success'" in classify
    assert "p_logstate" not in classify
    assert "'accepted'" not in classify
    assert "'unknown'" not in classify
    assert "CREATE TABLE IF NOT EXISTS mart_egisz.dim_document_status" in sql
    assert "('success', 'Успешно зарегистрирован'" in sql
    assert "('async_error', 'Ошибка асинхронного ответа РЭМД'" in sql
    assert "('sent', 'Отправлено'" in sql
    assert "'network_error'" not in sql
    # Код нефинального статуса не дублируется литералом в ветвях transform.
    assert "ELSE 'waiting'" not in sql
    assert "mart_egisz.document_status_nonfinal()" in transform_sql
    assert "ds.label AS status_label" in sql
    assert "WHEN d.status = 'success' THEN 'Успешно зарегистрирован'" not in sql
    assert "AND f.status IN ('success', 'error')" in transform_sql
    assert "CASE f.status WHEN 'success' THEN 'success' ELSE 'async_error' END" in transform_sql
    assert "NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL" in transform_sql
    parsing_sql = (DWH_INIT_SQL_PATH.parent / "02_functions.sql").read_text(encoding="utf-8")
    # Цепочка определения ЮЛ — одна функция: OID из содержания обмена, затем адрес обмена.
    resolve = parsing_sql.split("CREATE OR REPLACE FUNCTION mart_egisz.resolve_document_jid", 1)[1].split("$$;", 1)[0]
    assert "FROM mart_egisz.dim_clinic_oid r" in resolve
    assert "FROM mart_egisz.dim_clinic_endpoint r" in resolve
    assert resolve.index("WHEN mo.jid IS NOT NULL THEN 'mo_uid'") < resolve.index("WHEN ho.jid IS NOT NULL THEN 'host'")
    assert "jid_from_mo_uid" not in parsing_sql
    assert "jid_from_host" not in parsing_sql
    assert "egisz_xml_text" not in transform_sql
    assert "outbound_ref.dwh_id" not in sql
    # Ответ связывается с документом по реестру подач.
    assert "msg_ref.dwh_id" in transform_sql
    assert "exch_ref" not in transform_sql
    assert "gdf_events AS" not in transform_sql
    assert "gdf_ref" not in transform_sql
    assert "raw_egisz.exchangelog er" not in transform_sql
    assert "dim_exchangelog_refs" not in sql
    assert "xml_parsed_at" in sql
    assert "dim_egisz_message_refs" not in sql
    assert "status = 'sent'" in sql
    # Строковые сводки ошибок сняты: элементы хранит разобранное сообщение.
    assert "error_json_text" not in sql
    assert "error_messages_row" not in sql
    assert ", message, jid, jid_resolve_method, semd_code" in sql
    rpt_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    assert "NULLIF(btrim(d.dwh_id), '') IS NOT NULL" in rpt_sql
    assert "DWH_ID" not in rpt_sql
    assert "pending_source AS" not in sql


def test_dwh_init_sql_does_not_keep_legacy_egisz_messages_staging() -> None:
    sql = _read_dwh_init_sql()
    drop_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")

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

    monkeypatch.setattr("egisz_etl_dag.execute_values", fake_execute_values)

    changed = sync_directory(
        con, "mart_egisz.dim_organizations", [(1, "Clinic", "1234567890", "Address", "1.2.643.5.1.13.13.12.2.1.1")]
    )

    assert changed == 1
    assert con.cursor_instance.calls == [
        ("SET LOCAL lock_timeout = %s", (DIRECTORY_SYNC_LOCK_TIMEOUT,)),
        ("SET LOCAL statement_timeout = %s", (DIRECTORY_SYNC_STATEMENT_TIMEOUT,)),
    ]
    assert captured["cursor"] is con.cursor_instance
    assert "INSERT INTO mart_egisz.dim_organizations" in str(captured["sql"])
    assert "IS DISTINCT FROM EXCLUDED." in str(captured["sql"])
    assert captured["values"] == [(1, "Clinic", "1234567890", "Address", "1.2.643.5.1.13.13.12.2.1.1")]
    assert captured["page_size"] == DIRECTORY_SYNC_PAGE_SIZE
    assert con.committed is True


def test_sync_directory_never_clears_known_org_oid() -> None:
    """OID организации имеет два источника, поэтому UPSERT не затирает его пустым.

    Ведущий источник — справочник ФРМО; синхронизация справочников добирает OID из
    JPERSONS только там, где он ещё не известен. Предикат изменения сверяется с итоговым
    состоянием строки, иначе колонка считалась бы изменённой на каждом цикле и гоняла бы
    пересчёт JID документов впустую.
    """
    merge_sql = DIRECTORY_MERGE_EXPRESSIONS[("mart_egisz.dim_organizations", "fir_oid")]

    assert "dim_organizations.fir_oid" in merge_sql
    assert merge_sql.index("dim_organizations.fir_oid") < merge_sql.index("EXCLUDED.fir_oid")

    con = FakeSyncConnection()
    captured: dict[str, object] = {}

    def fake_execute_values(cursor: object, sql: str, values: list[tuple[object, ...]], page_size: int) -> None:
        captured["sql"] = sql
        con.cursor_instance.rowcount = len(values)

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("egisz_etl_dag.execute_values", fake_execute_values)
        sync_directory(con, "mart_egisz.dim_organizations", [(1, "Clinic", None, None, None)])

    sql = str(captured["sql"])
    assert f"fir_oid = {merge_sql}" in sql
    assert f"dim_organizations.fir_oid IS DISTINCT FROM {merge_sql}" in sql
    # Остальные колонки ведёт единственный источник — они перезаписываются как есть.
    assert "name = EXCLUDED.name" in sql


def test_clinic_registries_resolve_without_exchange_marker() -> None:
    """Резолв ЮЛ идёт по реестрам поверх лицензий, а не по отметке обмена MODIFYDATE.

    OID регистрационный: несколько ЮЛ на один OID означают отправку дочерних клиник
    с хоста головного ЮЛ, поэтому кандидат выбирается по собственному хосту. Адрес
    обмена несёт JID владельца хоста прямо в имени (gost-<N>, он же REPLY_TO подачи).
    """
    parsing_sql = (DWH_INIT_SQL_PATH.parent / "02_functions.sql").read_text(encoding="utf-8")

    assert "CREATE OR REPLACE VIEW mart_egisz.dim_clinic_oid" in parsing_sql
    assert "CREATE OR REPLACE VIEW mart_egisz.dim_clinic_endpoint" in parsing_sql
    assert "NULLIF(btrim(o.fir_oid), '') AS oid" in parsing_sql
    assert "FROM mart_egisz.dim_organizations o" in parsing_sql
    assert "ORDER BY oid, jid" in parsing_sql
    assert "FROM mart_egisz.dim_clinic_oid r" in parsing_sql
    assert "modifydate" not in parsing_sql
    assert "WHEN COALESCE(p_logtext, '') ~ ':9921" in parsing_sql
    assert "gost-[a-z0-9]+(?:-[a-z0-9]+)*(?:\\.[a-z0-9._-]+)?(?::[0-9]+)?" in parsing_sql
    # Именованные хосты (gost-sova) не должны обрезаться шаблоном по первому дефису.
    assert "gost-[a-z0-9]+(?:-[a-z0-9]+)*" in parsing_sql


def test_get_cursors_reads_a_cursor_per_phase() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.sql = ""

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: str, _params: tuple[object, ...]) -> None:
            self.sql = sql

        def fetchone(self) -> tuple[int, int, int]:
            return (123, 90, 45)

    class Connection:
        def __init__(self) -> None:
            self.cursor_instance = Cursor()

        def cursor(self) -> Cursor:
            return self.cursor_instance

    con = Connection()
    assert get_cursors(con, "egisz") == {
        "extract_logid_cursor": 123,
        "transform_logid_cursor": 90,
        "extract_egmid_cursor": 45,
    }
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

    assert get_cursors(Connection(), "egisz") == {
        "extract_logid_cursor": 0,
        "transform_logid_cursor": 0,
        "extract_egmid_cursor": 0,
    }


def test_fetch_raw_logids_range_reads_one_chunk() -> None:
    """Сверка сравнивает множества шагами по LOGID, а не всей таблицей."""

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
            return [(101,), (102,), (102,)]

    class Connection:
        def __init__(self) -> None:
            self.cursor_instance = Cursor()

        def cursor(self) -> Cursor:
            return self.cursor_instance

    con = Connection()
    assert fetch_raw_logids_range(con, low=100, high=200) == {101, 102}
    assert "logid >= %s AND logid <= %s" in con.cursor_instance.sql
    assert con.cursor_instance.params == (100, 200)


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
    calls: list[tuple[int, int]] = []

    class FakeCursor:
        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, _sql: str, params: tuple[int, int]) -> None:
            calls.append(params)

        def fetchone(self) -> tuple[dict[str, int]]:
            return ({"transformed": 2, "unlinked": 0, "sends_without_clinic": 0},)

    class FakeConnection:
        def cursor(self) -> FakeCursor:
            return FakeCursor()

        def commit(self) -> None:
            return None

    total = transform_missing_windows(FakeConnection(), [100, 101, 5000])

    assert total["transformed"] == 4
    assert calls == [(99, 101), (4999, 5000)]


def test_dwh_init_sql_declares_only_final_state_shape() -> None:
    """Схема объявляет конечное состояние, а не путь к нему.

    Разовые переименования и снятие отживших колонок — операции развёртывания, а не часть
    idempotent-схемы: в ней они превращаются в мусор, который прогоняется на каждом накате
    и переживает свой смысл.
    """
    sql = (DWH_INIT_SQL_PATH.parent / "01_schema.sql").read_text(encoding="utf-8")

    for legacy in ("source_min_created_at", "elt_state",
                   "last_logid", "last_egmid"):
        assert legacy not in sql, f"в схеме осталось упоминание {legacy!r}"
    # Курсор назван по фазе, которая его ведёт; строка конвейера заводится без значений.
    for cursor_column in (
        "extract_logid_cursor",
        "transform_logid_cursor",
        "extract_egmid_cursor",
    ):
        assert f"    {cursor_column} bigint DEFAULT 0," in sql
    assert "INSERT INTO etl_meta.egisz_etl_state (pipeline)\nVALUES ('egisz')" in sql
    assert "2026-05-18" not in sql
    assert "SOURCE_MIN_CREATED_AT" not in sql


def test_dwh_init_sql_partitions_time_series_tables() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "01_schema.sql").read_text(encoding="utf-8")
    transform_sql = (DWH_INIT_SQL_PATH.parent / "03_transform.sql").read_text(encoding="utf-8")

    assert "PARTITION BY RANGE (createdate)" in sql
    assert "PARTITION BY RANGE (log_date)" in sql
    assert "PRIMARY KEY (logid, createdate)" in sql
    assert "PRIMARY KEY (logid, log_date)" in sql
    assert "PARTITION OF raw_egisz.exchangelog DEFAULT" not in sql
    assert "PARTITION OF stg_egisz.exchange_messages DEFAULT" not in sql
    assert "CREATE OR REPLACE FUNCTION etl_meta.egisz_ensure_time_partitions" in sql
    # Схема объявляет партиционированные таблицы сразу, без конверсии из обычных.
    assert "relkind <> 'p'" not in sql
    assert "ON CONFLICT (logid, log_date) DO UPDATE SET" in transform_sql
    assert "ON CONFLICT (logid, log_date)" in transform_sql


def test_load_raw_logs_uses_partitioned_upsert_target() -> None:
    import inspect

    source = inspect.getsource(load_raw_logs)
    assert "ON CONFLICT (logid, createdate)" in source


def test_update_cursors_upserts_every_phase_cursor() -> None:
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
    update_cursors(con, "egisz", extract_logid=11)

    assert con.committed is True
    sql, params = con.cursor_instance.calls[0]
    assert "pipeline, extract_logid_cursor, transform_logid_cursor, extract_egmid_cursor" in sql
    for column in ("extract_logid_cursor", "transform_logid_cursor", "extract_egmid_cursor"):
        assert f"{column} = GREATEST(egisz_etl_state.{column}, EXCLUDED.{column})" in sql
    assert params == ("egisz", 11, 0, 0)
