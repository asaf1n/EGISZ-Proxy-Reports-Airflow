"""Контракт выдачи ошибок и очереди и стоимость эксплуатационных запросов.

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
    body = view_body("CREATE MATERIALIZED VIEW mart_egisz_selfservice.document_error AS",
                     "COMMENT ON MATERIALIZED VIEW mart_egisz_selfservice.document_error IS")
    assert f"({ERROR_CORPUS_PREDICATE}) AS is_error_corpus" in body

    # Тот же отбор — у недельных и месячных агрегатов: признак не расходится с ними.
    for unit in ("weekly", "monthly"):
        aggregate = view_body(f"CREATE MATERIALIZED VIEW mart_egisz.agg_document_error_{unit} AS",
                              f"COMMENT ON MATERIALIZED VIEW mart_egisz.agg_document_error_{unit} IS")
        assert f"AND ({ERROR_CORPUS_PREDICATE})" in aggregate


def test_error_aggregates_expose_period_denominators() -> None:
    for unit, key in (("weekly", "week_start"), ("monthly", "month_start")):
        aggregate = view_body(f"CREATE MATERIALIZED VIEW mart_egisz.agg_document_error_{unit} AS",
                              f"COMMENT ON MATERIALIZED VIEW mart_egisz.agg_document_error_{unit} IS")
        assert "p.docs_total," in aggregate
        assert "p.docs_all," in aggregate
        assert f"JOIN public.rpt_documents_{unit} p" in aggregate
        assert f"p.{key} = " in aggregate
        assert "p.clinic_label = r.clinic_label" in aggregate

        documents = view_body(f"CREATE MATERIALIZED VIEW public.rpt_documents_{unit} AS",
                              f"COMMENT ON MATERIALIZED VIEW public.rpt_documents_{unit} IS")
        assert "COUNT(DISTINCT d.dwh_id)::bigint AS docs_all" in documents
        # Знаменатель ошибок связи — все документы периода, знаменатель исходов — с ответом.
        assert "FILTER (WHERE d.status <> 'sent')::bigint AS docs_total" in documents


def test_document_error_types_is_one_row_per_document() -> None:
    body = view_body("CREATE MATERIALIZED VIEW mart_egisz_selfservice.document_error_type AS",
                     "COMMENT ON MATERIALIZED VIEW mart_egisz_selfservice.document_error_type IS")
    assert "FROM mart_egisz_selfservice.document_error e" in body
    assert "GROUP BY e.dwh_id" in body
    for column in ("errors_count", "error_types", "error_categories", "error_kinds",
                   "has_network_error", "has_remd_error", "is_error_corpus"):
        assert f"AS {column}" in body
    # Ключ нужен для REFRESH ... CONCURRENTLY.
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_document_error_type" in VIEWS_SQL
    # Исходный текст ошибки в опубликованный слой не выносится.
    assert "error_text" not in body


def test_pending_queue_history_reuses_the_queue_definition() -> None:
    body = view_body("CREATE MATERIALIZED VIEW public.rpt_pending_queue_daily AS",
                     "COMMENT ON MATERIALIZED VIEW public.rpt_pending_queue_daily IS")
    assert "public.is_pending_at(q.first_sent_at, q.first_callback_at, anchor.ts)" in body
    assert "public.pending_segment_at(q.first_sent_at, anchor.ts)" in body
    assert "NOT seg.is_no_response" in body
    # Пороги — из справочника, не литералы; пояс читается каталогом, поэтому один раз.
    assert "FROM public.dim_pending_segments" in body
    assert len(re.findall(r"public\.report_timezone\(\)", body)) == 2
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_rpt_pending_queue_daily" in VIEWS_SQL


def test_new_marts_are_refreshed_in_dependency_order() -> None:
    refresh = VIEWS_SQL.split("CREATE OR REPLACE FUNCTION public.refresh_report_marts()", 1)[1].split("$$;", 1)[0]
    marts = re.findall(r"REFRESH MATERIALIZED VIEW ([a-z_]+\.\w+);", refresh)

    assert marts.index("mart_egisz_selfservice.document_error") < marts.index("mart_egisz_selfservice.document_error_type")
    # Агрегаты ошибок читают знаменатели документных витрин своего периода.
    assert marts.index("public.rpt_documents_weekly") < marts.index("mart_egisz.agg_document_error_weekly")
    assert marts.index("public.rpt_documents_monthly") < marts.index("mart_egisz.agg_document_error_monthly")
    assert "public.rpt_pending_queue_daily" in marts
    for mart in ("mart_egisz_selfservice.document_error_type", "public.rpt_pending_queue_daily"):
        assert f"DROP MATERIALIZED VIEW IF EXISTS {mart} CASCADE;" in VIEWS_SQL
        assert f"ANALYZE {mart};" in VIEWS_SQL


def test_registry_without_document_is_driven_from_registry_rows() -> None:
    """Детализация идёт от подач без документа к ответам, а не от каждого ответа к реестру.

    Обратный порядок искал подачу для сотен тысяч сообщений и занимал десятки секунд.
    """
    body = view_body("CREATE OR REPLACE VIEW public.rpt_health_message_registry_no_document AS",
                     "COMMENT ON VIEW public.rpt_health_message_registry_no_document IS")
    assert "SELECT DISTINCT ON (m.msgid)" in body
    assert "WHERE m.document_uid IS NULL" in body
    assert "ORDER BY m.msgid, m.source_egmid DESC NULLS LAST" in body
    assert "public.message_registry_key(t.relates_to_msgid) = reg.msgid" in body
    assert "OFFSET 0" in body

    # Поиск по ключу опирается на индекс с тем же выражением.
    assert "ON public.transactions (public.message_registry_key(relates_to_msgid))" in FUNCTIONS_SQL


def test_health_signals_do_not_rebuild_the_registry_detail_or_sort_the_journal() -> None:
    body = view_body("CREATE OR REPLACE VIEW public.rpt_health_signals AS",
                     "-- Наблюдаемость слоя версий.")
    # Счёт «не более 500» не требует сортировки детализации по LOGID.
    assert 'ORDER BY "LOGID"' not in body
    # Размер очереди без ответа считается один раз на все три порога сигнала.
    assert body.count("no_response_after c") == 1
    assert "COUNT(DISTINCT dwh_id)::numeric FROM public.documents WHERE status = 'sent'" not in body

    # Последние 500 размеченных ответов читаются по индексу, упорядоченному по LOGID.
    assert "idx_transactions_logid_linked" in SCHEMA_SQL
    assert "(logid DESC)" in SCHEMA_SQL.split("idx_transactions_logid_linked", 1)[1].split(";", 1)[0]


def test_documents_sent_reuses_the_segment_of_documents_current() -> None:
    body = view_body("CREATE OR REPLACE VIEW public.rpt_documents_sent AS",
                     "COMMENT ON VIEW public.rpt_documents_sent IS")
    assert "pending_segment_code_at" not in body
    assert "pending_segment_at" not in body
    assert "r.pending_segment," in body
    assert "r.sent_state," in body
    assert "WHERE r.sent_state IS NOT NULL" in body

    versions = view_body("CREATE OR REPLACE VIEW public.rpt_document_versions AS",
                         "COMMENT ON VIEW public.rpt_document_versions IS")
    # Ступень подбирается только нефинальным статусам и внутри LATERAL.
    assert "public.pending_segment_at(d.first_sent_at, now())" in versions
    assert "WHERE ds.is_final IS NOT TRUE" in versions
