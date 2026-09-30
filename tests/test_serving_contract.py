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


def test_documents_sent_reuses_the_segment_of_document_versions() -> None:
    body = view_body("CREATE OR REPLACE VIEW serving_egisz.documents_sent AS",
                     "COMMENT ON VIEW serving_egisz.documents_sent IS")
    assert "pending_segment_code_at" not in body
    assert "pending_segment_at" not in body
    assert "r.pending_segment," in body
    assert "r.sent_state," in body
    assert "WHERE r.is_current_version\n  AND r.sent_state IS NOT NULL" in body

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
    for column in ("active_days", "quiet_days", "no_success_min_docs", "volume_medium_min_docs", "volume_heavy_min_docs"):
        assert f"{column} integer NOT NULL" in rules
    assert "jid_monthly_fee" not in rules

    revenue = view_body("CREATE MATERIALIZED VIEW serving_egisz.clinic_revenue_daily AS",
                        "COMMENT ON MATERIALIZED VIEW serving_egisz.clinic_revenue_daily IS")
    assert "FROM mart_egisz.jid_fee_rates f" in revenue
    assert "FROM mart_egisz.dim_jid_activity_rules r" in revenue


def test_period_dependent_metrics_are_functions_of_the_period() -> None:
    contribution = function_body("serving_egisz.clinic_error_rate_contribution")
    for parameter in ("p_from timestamptz", "p_to timestamptz", "p_clinic_labels text[]", "p_semd_labels text[]",
                      "p_error_types text[]"):
        assert parameter in contribution
    assert "FROM mart_egisz.dim_control_chart_phases p" in contribution
    assert "WHERE d.is_current_version" in contribution

    activity = function_body("serving_egisz.clinic_activity_period")
    assert "p_from timestamptz" in activity and "p_to timestamptz" in activity
    # Окна и пороги — из правил активности, ставка — из таблицы ставок, как у денежной витрины.
    assert "FROM mart_egisz.dim_jid_activity_rules r" in activity
    assert "FROM mart_egisz.jid_fee_rates f" in activity
    for column in ("is_active boolean", "is_new boolean", "is_churned boolean", "is_silent boolean",
                   "is_no_success boolean", "volume_segment text", "monthly_fee numeric"):
        assert column in activity

    # Тело — один запрос на STABLE-функциях: планировщик подставляет его в запрос потребителя.
    for body in (contribution, activity):
        assert "LANGUAGE sql\nSTABLE" in body


def test_search_keys_are_indexed_current_document_keys() -> None:
    body = view_body("CREATE MATERIALIZED VIEW serving_egisz.document_search_keys AS",
                     "COMMENT ON MATERIALIZED VIEW serving_egisz.document_search_keys IS")
    assert "FROM serving_egisz.document_versions d\nWHERE d.is_current_version" in body
    for column in ("semd_local_uid", "relates_to_msgid", "semd_emdr_id", "logid"):
        assert f"ON serving_egisz.document_search_keys ({column});" in body
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_document_search_keys" in body

    refresh = VIEWS_SQL.split("CREATE OR REPLACE FUNCTION serving_egisz.refresh_report_marts(", 1)[1].split("$$;", 1)[0]
    assert "'serving_egisz.document_search_keys'" in refresh
    assert "ANALYZE serving_egisz.document_search_keys;" in VIEWS_SQL
