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
transform_raw_to_facts = extract_dag.transform_raw_to_facts
update_cursors = extract_dag.update_cursors

etl_dag = extract_dag
DIRECTORY_SYNC_LOCK_TIMEOUT = etl_dag.DIRECTORY_SYNC_LOCK_TIMEOUT
DIRECTORY_SYNC_PAGE_SIZE = etl_dag.DIRECTORY_SYNC_PAGE_SIZE
DIRECTORY_SYNC_STATEMENT_TIMEOUT = etl_dag.DIRECTORY_SYNC_STATEMENT_TIMEOUT
sync_directory = etl_dag.sync_directory

maintenance_dag = load_dag_module("egisz_reconcile_maintenance_dag")
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
        "SELECT public.transform_raw_to_facts(%s, %s)",
        (10, 20),
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
    sql = (DWH_INIT_SQL_PATH.parent / "02_functions.sql").read_text(encoding="utf-8")
    assert "error_matching_rule_labels" in sql
    assert "ORDER BY r.rule_code" in sql
    matching_fn = sql.split("error_matching_rule_labels")[1].split("error_item_atoms")[0]
    assert "LIMIT 1" not in matching_fn


def test_error_matching_is_tiered() -> None:
    """Ярусный матчинг: победа первого яруса (min match_tier), внутри яруса — все
    совпадения с дедупом интерпретаций."""
    parts = DWH_INIT_SQL_PATH.parent
    rules = (parts / "02_functions.sql").read_text(encoding="utf-8")
    assert "match_tier" in rules
    assert "chk_dim_error_rules_match_tier" in rules
    # таксономия: зона ответственности и повторяемость с CHECK-доменом
    assert "responsibility" in rules
    assert "is_retryable" in rules
    assert "chk_dim_error_type_group_responsibility" in rules
    fns = (parts / "02_functions.sql").read_text(encoding="utf-8")
    matching_fn = fns.split("error_matching_rule_labels")[1].split("error_item_atoms")[0]
    assert "min(match_tier)" in matching_fn
    # ИЭМК: RegistryError (атрибуты) парсится отдельной веткой build_errors_json
    assert "CREATE OR REPLACE FUNCTION public.xml_registry_errors" in fns
    build_fn = fns.split("CREATE OR REPLACE FUNCTION public.build_errors_json")[1].split("$$;")[0]
    assert "xml_registry_errors" in build_fn
    # faultcode: локальная часть в UPPERCASE, последним в COALESCE error_code
    parsing = (parts / "02_functions.sql").read_text(encoding="utf-8")
    assert "faultcode" in parsing
    assert "COALESCE(v_error_code_xml, v_code_xml, v_faultcode)" in parsing


def test_rpt_error_breakdown_exposes_responsibility() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    breakdown = sql.split("CREATE MATERIALIZED VIEW public.rpt_error_breakdown")[1].split(
        "COMMENT ON MATERIALIZED VIEW public.rpt_error_breakdown")[0]
    assert "responsibility" in breakdown
    assert "is_retryable" in breakdown


def test_error_classify_uses_atomic_item_atoms() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "02_functions.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE FUNCTION public.error_item_atoms" in sql
    classify = sql.split("CREATE OR REPLACE FUNCTION public.error_classify")[1].split("$$;")[0]
    assert "error_item_atoms" in classify
    assert "error_interpretation_type" not in classify


def test_rpt_error_breakdown_is_materialized_and_splits_error_types() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
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
    drops = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    assert "DROP MATERIALIZED VIEW public.rpt_error_breakdown CASCADE" in drops


def test_rpt_documents_exposes_error_types_list_only() -> None:
    """rpt_document_versions (база rpt_documents) отдаёт полный список error_types
    как есть из documents; отбор по типу идёт через rpt_error_breakdown.
    rpt_documents = тот же проекшн, отфильтрованный по is_current_version."""
    sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    rpt = sql.split("CREATE OR REPLACE VIEW public.rpt_document_versions")[1].split("COMMENT ON VIEW public.rpt_document_versions")[0]
    assert "d.error_types" in rpt
    assert "canonical_error_list" not in rpt
    assert "AS error_type," not in rpt
    assert "split_part(" not in rpt


def test_document_version_layer_groups_by_doc_number() -> None:
    """Логический документ = (jid + semd_code + doc_number=PROTOCOLID); localUid — версия.
    CDA setId источником не отдаётся — группируем по журналу."""
    parts = DWH_INIT_SQL_PATH.parent
    tables = (parts / "01_schema.sql").read_text(encoding="utf-8")
    transform = (parts / "03_transform.sql").read_text(encoding="utf-8")
    rpt = (parts / "04_views.sql").read_text(encoding="utf-8")
    health = (parts / "04_views.sql").read_text(encoding="utf-8")

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


def test_response_links_to_document_through_message_registry() -> None:
    """Ответ ЕГИСЗ не несёт localUid: документ находится по relatesToMessage через
    реестр подач dim_message_document. Ключ приводится к каноническому виду одной
    функцией на обеих сторонах — при загрузке реестра и при поиске."""
    parts = DWH_INIT_SQL_PATH.parent
    tables = (parts / "01_schema.sql").read_text(encoding="utf-8")
    parsing = (parts / "02_functions.sql").read_text(encoding="utf-8")
    transform = (parts / "03_transform.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS dim_message_document" in tables
    assert "CREATE OR REPLACE FUNCTION public.message_registry_key" in parsing

    msg_ref = transform.split("        ) msg_ref ON TRUE")[0].rsplit("LEFT JOIN LATERAL (", 1)[1]
    assert "FROM public.dim_message_document m" in msg_ref
    assert "m.msgid = public.message_registry_key(r.relates_to_id)" in msg_ref

    # Правило привязки фиксируется на строке — иначе деградация остаётся незаметной.
    assert "AS link_method" in transform
    assert "'message_registry'" in transform
    assert "'unlinked'" in transform
    # Индексы и бэкфилл прежнего правила сняты вместе с ним.
    assert "DROP INDEX IF EXISTS idx_transactions_gdf_jid_logid" in tables
    assert "AND t.jid IS NULL" not in transform
    # Агрегация реквизитов ограничена документами батча, не всем архивом.
    assert "batch_document_ids" in transform


def test_parse_attempts_marker_prevents_reparse_of_uninsertable_rows() -> None:
    """Попытка парсинга фиксируется в exchangelog_parse_attempts. Строки без реквизитов
    (нет msgid/localUid/emdrId/getDocumentFile) в transactions не вставляются, поэтому
    анти-джойн по transactions.xml_parsed_at перепарсивал их каждым полножурнальным
    lookback'ом reconcile (~65 тыс. строк ≈ 6,4 мин на окно)."""
    parts = DWH_INIT_SQL_PATH.parent
    tables = (parts / "01_schema.sql").read_text(encoding="utf-8")
    transform = (parts / "03_transform.sql").read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS exchangelog_parse_attempts" in tables
    # Бэкфилл маркера из уже распарсенных строк transactions: без него первый
    # полножурнальный lookback перепарсил бы весь архив, а не только «мусор».
    assert "INSERT INTO exchangelog_parse_attempts (logid)" in tables
    assert "SELECT logid FROM transactions WHERE xml_parsed_at IS NOT NULL" in tables
    assert "ANALYZE exchangelog_parse_attempts" in tables

    # Обе ветки parse_targets отбирают кандидатов по маркеру, не по transactions.
    parse_targets = transform.split("parse_targets AS (")[1].split("INSERT INTO public.transactions")[0]
    assert parse_targets.count("public.exchangelog_parse_attempts") == 1
    assert "xml_parsed_at" not in parse_targets

    # Маркер пишется на весь просканированный диапазон после вставки (анти-джойн
    # вставки должен видеть состояние маркера до батча).
    marker = transform.split("INSERT INTO public.exchangelog_parse_attempts (logid)")
    assert len(marker) == 2
    assert "ON CONFLICT (logid) DO NOTHING" in marker[1]
    parse_insert = transform.split("WITH parse_targets AS (")[1]
    assert parse_insert.index("INSERT INTO public.transactions") < parse_insert.index(
        "INSERT INTO public.exchangelog_parse_attempts"
    )


def test_document_attributes_maintained_without_enriched_mart() -> None:
    sql = _read_dwh_init_sql()
    transform_sql = (DWH_INIT_SQL_PATH.parent / "03_transform.sql").read_text(encoding="utf-8")
    core_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")

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
    rpt_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
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
    core_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    assert "clinic_oid_xml" in core_sql
    assert "clinic_oid_jpersons" in core_sql
    assert "public.document_source_mismatch" in core_sql
    assert "LEFT JOIN public.dim_document_status ds ON ds.code = d.status" in rpt_sql
    assert "'нет'::text AS \"Расхождение источников JID\"" not in core_sql


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

    assert "INSERT INTO dim_semd_types (code, type_code, name, level, format_code, start_date, end_date, implementation_guide, git_link)" in sql
    assert "oid = EXCLUDED.code" in sql
    assert "SET oid = code" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_dim_semd_types_oid" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_transactions_dwh_id_semd" in sql
    # Функциональные XML-индексы по msgtext не используются transform (parse-once в transactions).
    assert "DROP INDEX IF EXISTS idx_exchangelog_raw_xml_local_uid_norm" in sql
    assert "DROP INDEX IF EXISTS idx_exchangelog_raw_xml_document_id_norm" in sql
    assert "DROP INDEX IF EXISTS idx_exchangelog_raw_xml_message_id_norm" in sql
    assert "CREATE INDEX IF NOT EXISTS idx_exchangelog_raw_xml" not in sql
    assert "candidate_log_ids AS" in sql
    assert "CREATE OR REPLACE FUNCTION public.parse_exchangelog_row" in sql
    assert "CROSS JOIN LATERAL public.parse_exchangelog_row" in transform_sql
    assert "tx.xml_semd_code AS kind_xml" in transform_sql
    assert "tx.xml_local_uid AS local_uid_xml" in transform_sql
    assert "tx.xml_dwh_id AS dwh_id_xml" in transform_sql
    assert "COALESCE(r.local_uid_xml, msg_ref.local_uid) AS local_uid_semd" in transform_sql
    assert "public.clean_text_value(d.local_uid)" in sql
    # status_category удалён как выводимый из status; transform им больше не управляет,
    # а развёрнутые БД чистятся идемпотентным DROP COLUMN.
    assert "status_category = CASE" not in sql
    assert "status_category," not in sql
    assert "DROP COLUMN IF EXISTS status_category" in sql
    assert "document_attributes AS" in transform_sql
    assert "document_resolved AS" in transform_sql
    assert "resolve_document_jid" in transform_sql
    assert "AND (a.has_network_error OR a.resolved_jid IS NOT NULL)" in transform_sql
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
    views_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    # Только слой rpt_*: message-грейн он не читает. Секция document_attributes сюда не
    # входит — она как раз и переносит реквизиты с грейна transactions на документ,
    # чтобы отчётному слою не приходилось этого делать.
    reporting_sql = "\n".join(
        line.split("--", 1)[0]
        for line in sql_section(views_sql, "rpt_documents").splitlines()
    )

    assert "exchangelog_raw" not in reporting_sql
    assert "egisz_messages_raw" not in reporting_sql
    assert "stg_egisz_messages" not in reporting_sql
    assert "fact_egisz_messages" not in reporting_sql
    assert "transactions" not in reporting_sql
    assert "dim_exchangelog_refs" not in reporting_sql


def test_dwh_init_sql_interprets_patient_address_schematron_and_network_errors() -> None:
    sql = _read_dwh_init_sql()
    transform_sql = (DWH_INIT_SQL_PATH.parent / "03_transform.sql").read_text(encoding="utf-8")

    # Наименования типов — формулировки классификатора ФНСИ 1.2.643.5.1.13.13.99.2.305.
    assert "dim_nsi_error_code" in sql
    assert "1.2.643.5.1.13.13.99.2.305" in sql
    assert "Адрес пациента: атрибуты элемента address:Type не соответствуют требованиям" in sql
    assert "Данные пациента с переданным локальным идентификатором отличаются от зарегистрированных в ГИП" in sql
    assert "Документ с указанным идентификатором (в РМИС/МИС) уже зарегистрирован" in sql
    assert "Ошибка при получении файла документа из предоставляющей системы" in sql
    assert "Ошибка асинхронного ответа" in sql
    # Трактовки, разошедшиеся со справочником, сняты вместе с выдуманными кодами.
    assert "Не указан адрес пациента" not in sql
    assert "Срок действия сертификата организации истек" not in sql
    assert "ORGANIZATION_NOT_REGISTERED" not in sql
    assert "CA_UNAVAILABLE" not in sql
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
    transform_sql = (DWH_INIT_SQL_PATH.parent / "03_transform.sql").read_text(encoding="utf-8")

    # Синхронный ответ (шаг 4 схемы регистрации) = только приём запроса ('accepted');
    # регистрация подтверждается асинхронным callback'ом.
    assert "приём запроса на регистрацию (шаг 4 схемы)" in sql
    assert "COALESCE(p_document_status, '') ~* 'зарегистр'" in sql
    assert "'RegisterDocumentResponse'" in sql
    assert "THEN 'success'" in sql
    assert "THEN 'accepted'" in sql
    assert "CREATE TABLE IF NOT EXISTS dim_document_status" in sql
    assert "('success', 'Успешно зарегистрирован'" in sql
    assert "('network_error', 'Ошибка связи'" in sql
    assert "('async_error', 'Ошибка асинхронного ответа РЭМД'" in sql
    assert "('sent', 'Отправлено'" in sql
    # Код нефинального статуса не дублируется литералом в ветвях transform.
    assert "ELSE 'waiting'" not in sql
    assert "public.document_status_nonfinal()" in transform_sql
    assert "ds.label AS status_label" in sql
    assert "WHEN d.status = 'success' THEN 'Успешно зарегистрирован'" not in sql
    assert "WHERE e.final_status IN ('success', 'error')" in sql
    assert "NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL" in transform_sql
    parsing_sql = (DWH_INIT_SQL_PATH.parent / "02_functions.sql").read_text(encoding="utf-8")
    drop_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE FUNCTION public.resolve_document_jid" in parsing_sql
    assert "CREATE OR REPLACE FUNCTION public.jid_from_mo_uid" in parsing_sql
    assert "CREATE OR REPLACE FUNCTION public.jid_from_host" in parsing_sql
    assert "CREATE OR REPLACE FUNCTION public.document_source_mismatch" in parsing_sql
    assert "egisz_xml_text" not in transform_sql
    assert "outbound_ref.dwh_id" not in sql
    # Ответ связывается с документом по реестру подач; самосоединение по journal-MSGID
    # и позиционная догадка «последний getDocumentFile клиники» сняты.
    assert "msg_ref.dwh_id" in transform_sql
    assert "exch_ref" not in transform_sql
    assert "gdf_events AS" not in transform_sql
    assert "gdf_ref" not in transform_sql
    assert "exchangelog_raw er" not in transform_sql
    assert "CREATE TABLE IF NOT EXISTS dim_exchangelog_refs" not in sql
    assert "INSERT INTO public.dim_exchangelog_refs" not in transform_sql
    assert "xml_parsed_at" in sql
    assert "CREATE TABLE IF NOT EXISTS dim_egisz_message_refs" not in sql
    assert "DROP TABLE IF EXISTS public.dim_egisz_message_refs" not in drop_sql
    assert "status = 'sent'" in sql
    assert "f.error_json_text" in sql
    assert "error_messages_row" in transform_sql
    assert "COALESCE(NULLIF(btrim(f.error_json_text), ''), f.message)" not in transform_sql
    assert ", message, jid, jid_resolve_method, semd_code" in sql
    assert "error_message," not in transform_sql
    assert "error_message =" not in transform_sql
    rpt_sql = (DWH_INIT_SQL_PATH.parent / "04_views.sql").read_text(encoding="utf-8")
    assert "NULLIF(btrim(d.dwh_id), '') IS NOT NULL" in rpt_sql
    assert "DWH_ID" not in rpt_sql
    assert "public.clean_text_value(t.message_id),\n        t.logid::text" not in sql
    assert "pending_source AS" not in sql
    assert "WHEN e.final_status = 'success' THEN 'Успешно'" not in sql


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


def test_get_cursors_reads_logid_cursor_only() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.sql = ""

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, sql: str, _params: tuple[object, ...]) -> None:
            self.sql = sql

        def fetchone(self) -> tuple[int, int]:
            return (123, 45)

    class Connection:
        def __init__(self) -> None:
            self.cursor_instance = Cursor()

        def cursor(self) -> Cursor:
            return self.cursor_instance

    con = Connection()
    assert get_cursors(con, "egisz") == {"logid_cursor": 123, "egmid_cursor": 45}
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

    assert get_cursors(Connection(), "egisz") == {"logid_cursor": 0, "egmid_cursor": 0}


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

    Разовые переименования и снятие отживших колонок — операции развёртывания
    (deploy/README.md §1.6–1.7), а не часть idempotent-схемы: в ней они превращаются
    в мусор, который прогоняется на каждом накате и переживает свой смысл.
    """
    sql = (DWH_INIT_SQL_PATH.parent / "01_schema.sql").read_text(encoding="utf-8")

    for legacy in ("source_min_created_at", "elt_state", "elt_job_runs",
                   "last_logid", "last_egmid"):
        assert legacy not in sql, f"в схеме осталось упоминание {legacy!r}"
    assert "INSERT INTO etl_state (pipeline, logid_cursor)\nVALUES ('egisz', 0)" in sql
    assert "2026-05-18" not in sql
    assert "SOURCE_MIN_CREATED_AT" not in sql


def test_dwh_init_sql_partitions_time_series_tables() -> None:
    sql = (DWH_INIT_SQL_PATH.parent / "01_schema.sql").read_text(encoding="utf-8")
    transform_sql = (DWH_INIT_SQL_PATH.parent / "03_transform.sql").read_text(encoding="utf-8")

    assert "PARTITION BY RANGE (createdate)" in sql
    assert "PARTITION BY RANGE (log_date)" in sql
    assert "PRIMARY KEY (logid, createdate)" in sql
    assert "PRIMARY KEY (logid, log_date)" in sql
    assert "PARTITION OF public.exchangelog_raw DEFAULT" not in sql
    assert "PARTITION OF public.transactions DEFAULT" not in sql
    assert "CREATE OR REPLACE FUNCTION public.ensure_time_partitions" in sql
    assert "relkind <> 'p'" in sql
    assert "ON CONFLICT (logid, log_date) DO UPDATE SET" in transform_sql
    assert "ON CONFLICT (logid, log_date)" in transform_sql


def test_load_raw_logs_uses_partitioned_upsert_target() -> None:
    import inspect

    source = inspect.getsource(load_raw_logs)
    assert "ON CONFLICT (logid, createdate)" in source


def test_update_cursors_upserts_logid_cursor() -> None:
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
    assert "INSERT INTO etl_state (pipeline, logid_cursor, egmid_cursor)" in sql
    assert "logid_cursor = GREATEST(etl_state.logid_cursor, EXCLUDED.logid_cursor)" in sql
    assert params == ("egisz", 11, 0)
