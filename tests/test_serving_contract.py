"""Контракт выдачи ошибок и очереди в serving_egisz и стоимость эксплуатационных запросов.

Статические проверки: состав столбцов и порядок обновления витрин задаёт SQL слоя, живой
DWH для них не нужен.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VIEWS_SQL = (ROOT / "db" / "04_views.sql").read_text(encoding="utf-8")
FUNCTIONS_SQL = (ROOT / "db" / "02_functions.sql").read_text(encoding="utf-8")
SCHEMA_SQL = (ROOT / "db" / "01_schema.sql").read_text(encoding="utf-8")

ERROR_CORPUS_PREDICATE = "r.status = 'async_error' OR c.error_kind = 'Ошибка связи'"


def view_body(create: str, comment: str) -> str:
    start = VIEWS_SQL.index(create)
    return VIEWS_SQL[start : VIEWS_SQL.index(comment, start)]


def test_document_errors_carry_the_error_corpus_flag() -> None:
    body = view_body("CREATE MATERIALIZED VIEW serving_egisz.document_errors AS",
                     "COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors IS")
    assert f"({ERROR_CORPUS_PREDICATE}) AS is_error_corpus" in body

    # Тот же отбор — у недельных и месячных агрегатов: признак не расходится с ними.
    for unit in ("weekly", "monthly"):
        aggregate = view_body(f"CREATE MATERIALIZED VIEW serving_egisz.document_errors_{unit} AS",
                              f"COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors_{unit} IS")
        assert f"AND ({ERROR_CORPUS_PREDICATE})" in aggregate


def test_error_aggregates_expose_period_denominators() -> None:
    for unit, key in (("weekly", "week_start"), ("monthly", "month_start")):
        aggregate = view_body(f"CREATE MATERIALIZED VIEW serving_egisz.document_errors_{unit} AS",
                              f"COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors_{unit} IS")
        assert "p.docs_total," in aggregate
        assert "p.docs_all," in aggregate
        assert f"JOIN serving_egisz.documents_{unit} p" in aggregate
        assert f"p.{key} = " in aggregate
        assert "p.clinic_label = r.clinic_label" in aggregate

        documents = view_body(f"CREATE MATERIALIZED VIEW serving_egisz.documents_{unit} AS",
                              f"COMMENT ON MATERIALIZED VIEW serving_egisz.documents_{unit} IS")
        assert "COUNT(DISTINCT d.dwh_id)::bigint AS docs_all" in documents
        # Знаменатель ошибок связи — все документы периода, знаменатель исходов — с ответом.
        assert "FILTER (WHERE d.status <> 'sent')::bigint AS docs_total" in documents


def test_document_error_types_is_one_row_per_document() -> None:
    body = view_body("CREATE MATERIALIZED VIEW serving_egisz.document_error_types AS",
                     "COMMENT ON MATERIALIZED VIEW serving_egisz.document_error_types IS")
    assert "FROM serving_egisz.document_errors e" in body
    assert "GROUP BY e.dwh_id" in body
    for column in ("errors_count", "error_types", "error_categories", "error_kinds",
                   "has_network_error", "has_remd_error", "is_error_corpus"):
        assert f"AS {column}" in body
    # Ключ нужен для REFRESH ... CONCURRENTLY.
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_document_error_types" in VIEWS_SQL
    # Исходный текст ошибки в опубликованный слой не выносится.
    assert "error_text" not in body


def test_pending_queue_history_reuses_the_queue_definition() -> None:
    body = view_body("CREATE MATERIALIZED VIEW serving_egisz.pending_queue_daily AS",
                     "COMMENT ON MATERIALIZED VIEW serving_egisz.pending_queue_daily IS")
    assert "serving_egisz.is_pending_at(q.first_sent_at, q.first_callback_at, anchor.ts)" in body
    assert "serving_egisz.pending_segment_at(q.first_sent_at, anchor.ts)" in body
    assert "NOT seg.is_no_response" in body
    # Пороги — из справочника, не литералы; пояс читается каталогом, поэтому один раз.
    assert "FROM mart_egisz.dim_pending_segments" in body
    assert len(re.findall(r"serving_egisz\.report_timezone\(\)", body)) == 2
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_pending_queue_daily" in VIEWS_SQL


def test_new_marts_are_refreshed_in_dependency_order() -> None:
    refresh = VIEWS_SQL.split("CREATE OR REPLACE FUNCTION serving_egisz.refresh_report_marts(", 1)[1].split("$$;", 1)[0]
    marts = re.findall(r"'([a-z_]+\.\w+)'", refresh.split("ARRAY[", 1)[1].split("]::regclass[]", 1)[0])

    assert marts.index("serving_egisz.document_errors") < marts.index("serving_egisz.document_error_types")
    # Агрегаты ошибок читают знаменатели документных витрин своего периода.
    assert marts.index("serving_egisz.documents_weekly") < marts.index("serving_egisz.document_errors_weekly")
    assert marts.index("serving_egisz.documents_monthly") < marts.index("serving_egisz.document_errors_monthly")
    assert "serving_egisz.pending_queue_daily" in marts
    for mart in ("document_error_types", "pending_queue_daily"):
        assert f"DROP MATERIALIZED VIEW IF EXISTS serving_egisz.{mart} CASCADE;" in VIEWS_SQL
        assert f"ANALYZE serving_egisz.{mart};" in VIEWS_SQL


def test_registry_without_document_is_driven_from_registry_rows() -> None:
    """Детализация идёт от подач без документа к ответам, а не от каждого ответа к реестру.

    Обратный порядок искал подачу для сотен тысяч сообщений и занимал десятки секунд.
    """
    body = view_body("CREATE OR REPLACE VIEW mart_egisz_admin.health_message_registry_no_document AS",
                     "COMMENT ON VIEW mart_egisz_admin.health_message_registry_no_document IS")
    assert "SELECT DISTINCT ON (m.msgid)" in body
    assert "WHERE m.document_uid IS NULL" in body
    assert "ORDER BY m.msgid, m.egmid DESC NULLS LAST" in body
    assert "stg_egisz.message_registry_key(t.relates_to_msgid) = reg.msgid" in body
    assert "OFFSET 0" in body

    # Поиск по ключу опирается на индекс с тем же выражением.
    assert "ON stg_egisz.exchange_messages (stg_egisz.message_registry_key(relates_to_msgid))" in FUNCTIONS_SQL


def test_health_signals_do_not_rebuild_the_registry_detail_or_sort_the_journal() -> None:
    body = view_body("CREATE OR REPLACE VIEW mart_egisz_admin.health_signals AS",
                     "-- Наблюдаемость слоя версий.")
    # Счёт «не более 500» не требует сортировки детализации по LOGID.
    assert 'ORDER BY "LOGID"' not in body
    # Размер очереди без ответа считается один раз на все три порога сигнала.
    assert body.count("no_response_after c") == 1
    assert "COUNT(DISTINCT dwh_id)::numeric FROM mart_egisz.documents WHERE status = 'sent'" not in body

    # Последние 500 размеченных ответов читаются по индексу, упорядоченному по LOGID.
    assert "idx_exchange_messages_logid_linked" in SCHEMA_SQL
    assert "(logid DESC)" in SCHEMA_SQL.split("idx_exchange_messages_logid_linked", 1)[1].split(";", 1)[0]


def test_queue_is_read_from_documents_current_at_the_current_moment() -> None:
    body = view_body("CREATE OR REPLACE VIEW serving_egisz.documents_sent AS",
                     "COMMENT ON VIEW serving_egisz.documents_sent IS")
    assert "FROM serving_egisz.documents_current r" in body
    assert "serving_egisz.pending_segment_at(r.first_sent_at, now()) seg" in body
    assert "WHERE r.status = 'sent'\n  AND NOT seg.is_no_response" in body

    versions = view_body("CREATE OR REPLACE VIEW serving_egisz.document_versions AS",
                         "COMMENT ON VIEW serving_egisz.document_versions IS")
    # Ступень подбирается только нефинальным статусам и внутри LATERAL.
    assert "serving_egisz.pending_segment_at(d.first_sent_at, now())" in versions
    assert "WHERE ds.is_final IS NOT TRUE" in versions


def function_body(name: str) -> str:
    start = VIEWS_SQL.index(f"CREATE OR REPLACE FUNCTION {name}(")
    return VIEWS_SQL[start : VIEWS_SQL.index("$$;", start)]


def test_fee_rate_and_activity_rules_are_separate_parameter_tables() -> None:
    fee = SCHEMA_SQL.split("CREATE TABLE IF NOT EXISTS mart_egisz.jid_fee_rates (", 1)[1].split(");", 1)[0]
    assert "jid_monthly_fee numeric(12, 2)" in fee
    assert "active_days" not in fee
    rules = SCHEMA_SQL.split("CREATE TABLE IF NOT EXISTS mart_egisz.dim_jid_activity_rules (", 1)[1].split(");", 1)[0]
    for column in ("active_days", "quiet_days", "no_success_min_docs"):
        assert f"{column} integer NOT NULL" in rules
    assert "jid_monthly_fee" not in rules

    revenue = view_body("CREATE MATERIALIZED VIEW serving_egisz.clinic_activity_daily AS",
                        "COMMENT ON MATERIALIZED VIEW serving_egisz.clinic_activity_daily IS")
    assert "FROM mart_egisz.jid_fee_rates f" in revenue
    assert "FROM mart_egisz.dim_jid_activity_rules r" in revenue


def test_period_dependent_metrics_are_functions_of_the_period() -> None:
    contribution = function_body("serving_egisz.clinic_error_rate_contribution")
    for parameter in ("p_from timestamptz", "p_to timestamptz", "p_clinic_labels text[]", "p_semd_labels text[]",
                      "p_error_types text[]"):
        assert parameter in contribution
    assert "FROM mart_egisz.dim_control_chart_phases p" in contribution
    assert "FROM serving_egisz.documents_current d" in contribution

    activity = function_body("serving_egisz.clinic_activity")
    assert "p_from timestamptz" in activity and "p_to timestamptz" in activity
    # Окна и пороги — из правил активности, ставка — из таблицы ставок, как у денежной витрины.
    assert "FROM mart_egisz.dim_jid_activity_rules r" in activity
    assert "FROM mart_egisz.jid_fee_rates f" in activity
    for column in ("is_active boolean", "is_new boolean", "is_churned boolean", "is_silent boolean",
                   "is_no_success boolean", "monthly_fee numeric"):
        assert column in activity

    # Тело — один запрос на STABLE-функциях: планировщик подставляет его в запрос потребителя.
    for body in (contribution, activity):
        assert "LANGUAGE sql\nSTABLE" in body


def test_documents_current_is_the_indexed_state_of_the_document() -> None:
    """Состояние документа к выдаче — текущие версии без документов за сроком ожидания; ключи поиска
    индексированы; список без ответа — отдельный объект, оба обновляются одним вызовом."""
    body = view_body("CREATE MATERIALIZED VIEW serving_egisz.documents_current AS",
                     "COMMENT ON MATERIALIZED VIEW serving_egisz.documents_current IS")
    assert "FROM serving_egisz.document_versions v\nWHERE v.is_current_version\n  AND v.sent_state IS DISTINCT FROM 'no_response'" in body
    for column in ("ips_date", "clinic_label", "semd_label", "semd_local_uid", "relates_to_msgid", "semd_emdr_id", "logid"):
        assert f"ON serving_egisz.documents_current ({column});" in body
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_current" in body

    no_response = view_body("CREATE MATERIALIZED VIEW serving_egisz.documents_no_response AS",
                            "COMMENT ON MATERIALIZED VIEW serving_egisz.documents_no_response IS")
    assert "WHERE v.is_current_version\n  AND v.sent_state = 'no_response'" in no_response
    for column in ("semd_local_uid", "relates_to_msgid", "request_logid"):
        assert f"ON serving_egisz.documents_no_response ({column});" in no_response

    refresh = VIEWS_SQL.split("CREATE OR REPLACE FUNCTION serving_egisz.refresh_report_marts(", 1)[1].split("$$;", 1)[0]
    marts = re.findall(r"'([a-z_]+\.\w+)'", refresh.split("ARRAY[", 1)[1].split("]::regclass[]", 1)[0])
    assert marts.index("serving_egisz.documents_current") < marts.index("serving_egisz.document_errors")
    assert marts.index("serving_egisz.documents_no_response") == marts.index("serving_egisz.documents_current") + 1
    for mart in ("documents_current", "documents_no_response"):
        assert f"ANALYZE serving_egisz.{mart};" in VIEWS_SQL


def test_current_state_consumers_read_documents_current() -> None:
    """Опубликованные ошибки, контроль качества, витрины текущего состояния и вклад клиник читают
    состояние документа к выдаче."""
    for create, comment in (
        ("CREATE MATERIALIZED VIEW serving_egisz.document_errors AS", "COMMENT ON MATERIALIZED VIEW serving_egisz.document_errors IS"),
        ("CREATE MATERIALIZED VIEW mart_egisz_admin.document_quality AS", "COMMENT ON MATERIALIZED VIEW mart_egisz_admin.document_quality IS"),
        ("CREATE MATERIALIZED VIEW serving_egisz.semd_error_categories_daily AS",
         "COMMENT ON MATERIALIZED VIEW serving_egisz.semd_error_categories_daily IS"),
        ("CREATE MATERIALIZED VIEW serving_egisz.registration_speed_daily AS",
         "COMMENT ON MATERIALIZED VIEW serving_egisz.registration_speed_daily IS"),
        ("CREATE OR REPLACE VIEW serving_egisz.clinic_semd_activity AS", "COMMENT ON VIEW serving_egisz.clinic_semd_activity IS"),
    ):
        assert "serving_egisz.documents_current" in view_body(create, comment), create


def test_status_filter_values_follow_documents_current() -> None:
    statuses = view_body("CREATE VIEW serving_egisz.document_status_details AS",
                         "COMMENT ON VIEW serving_egisz.document_status_details IS")
    assert "AND ss.code <> 'no_response'" in statuses
    segments = view_body("CREATE VIEW serving_egisz.pending_segments AS", "COMMENT ON VIEW serving_egisz.pending_segments IS")
    assert "WHERE NOT g.is_no_response" in segments


def test_error_text_masking_hides_only_personal_data() -> None:
    """Маскирование текста для выдачи — функция слоя витрин: применяет только шаги нормализации,
    скрывающие персональные данные. Классификация на stage нормализует текст в тип сама и
    функцию маскирования не вызывает."""
    mask = FUNCTIONS_SQL.split("CREATE OR REPLACE FUNCTION mart_egisz.mask_error_text(", 1)[1].split("$$;", 1)[0]
    assert "WHERE r.rule_kind = 'нормализация'" in mask
    assert "AND r.masks_personal_data" in mask
    assert "ORDER BY r.apply_order" in mask
    classify = FUNCTIONS_SQL.split("CREATE OR REPLACE FUNCTION stg_egisz.classify_error(", 1)[1].split("$$;", 1)[0]
    assert "mask_error_text" not in classify
    assert "WHERE r.rule_kind = 'нормализация'" in classify
    assert "CREATE OR REPLACE FUNCTION stg_egisz.mask_error_text" not in FUNCTIONS_SQL
