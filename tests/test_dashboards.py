from __future__ import annotations

import json
from pathlib import Path

INTEGRATION_DASHBOARD = Path("metabase_dashboards/01_integration_egisz.json")
TAB_BY_LEGACY = {
    "01_operational.json": "operational",
    "02_service.json": "service",
    "03_documents_no_response.json": "queue",
    "04_quality_and_errors.json": "errors",
    "06_semd_archive.json": "archive",
}


def _archive_click_target(card: dict) -> dict:
    return card.get("click_behavior") or {}


def _assert_archive_tab_click(card: dict) -> None:
    click = _archive_click_target(card)
    assert click.get("targetDashboard") == "Интеграция с ЕГИСЗ"
    assert click.get("tab") == "archive"


def _integration_dashboard() -> dict:
    return json.loads(INTEGRATION_DASHBOARD.read_text(encoding="utf-8"))


def _legacy_dashboard(legacy_file: str) -> dict:
    tab = TAB_BY_LEGACY[legacy_file]
    base = _integration_dashboard()
    return {
        **{key: value for key, value in base.items() if key != "cards"},
        "cards": [card for card in base["cards"] if card.get("tab") == tab],
    }


def _dashboard_paths() -> list[Path]:
    return sorted(Path("metabase_dashboards").glob("*.json"))


def _native_queries(dashboard: dict) -> list[str]:
    return [
        card["dataset_query"]["native"]["query"]
        for card in dashboard["cards"]
        if card.get("dataset_query", {}).get("type") == "native"
    ]


def _card_query_fingerprint(card: dict) -> str:
    dq = card.get("dataset_query", {})
    if dq.get("type") == "query":
        return json.dumps(dq.get("query", {}), ensure_ascii=False, sort_keys=True)
    return dq.get("native", {}).get("query", "")


def test_all_dashboards_default_to_full_width() -> None:
    dashboards = _dashboard_paths()
    assert dashboards, "Expected dashboard JSON files in metabase_dashboards/"

    for path in dashboards:
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload.get("width") == "full", f"{path.name} must default to full width"


def test_service_network_top_groups_by_typed_label() -> None:
    dashboard = _legacy_dashboard("02_service.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == "Типы сетевых ошибок (за период)")
    query = card["dataset_query"]["native"]["query"]
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")

    assert '"Тип сетевой ошибки"' in query
    assert 'public.egisz_network_error_type(d.error_text) AS "Тип сетевой ошибки"' in sql
    assert "per_kind AS" in query
    assert "[[AND {{dwh_date}}]]" in query
    assert "Остальные (" in query


def test_quality_dashboard_has_no_transport_detail_block() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    names = {c.get("name") for c in dashboard["cards"]}
    param_slugs = {p["slug"] for p in dashboard["parameters"]}
    queries = _native_queries(dashboard)
    for retired in (
        "Топ клиник по сбоям транспорта",
        "Типы сетевых ошибок (все дни)",
        "Тренд ошибок связи по дням",
        "Детализация ошибок связи",
        "Объёмы доступности по дням",
        "Доля доступности по дням",
        "Доступность транспорта: день × JID",
    ):
        assert retired not in names
    assert "connectivity_day_filter" not in param_slugs
    assert all("v_rpt_connectivity_" not in q for q in queries)
    assert all("Ошибок связи (транспорт)" not in q for q in queries)


def test_operational_error_types_include_network_slice() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(card for card in dashboard["cards"] if card.get("name") == "Виды ошибок по категориям")
    query = card["dataset_query"]["native"]["query"]

    assert card["display"] == "row"
    assert "v_rpt_error_category_breakdown_ui" in query
    assert '"Категория ошибки"' in query
    assert '"Вид ошибки"' in query
    assert "FROM public.v_rpt_documents_ui d" in sql
    assert '"Статус (код)" IN (\'async_error\', \'network_error\')' in sql
    assert "WHEN d.status = 'network_error' THEN 'Сетевая ошибка'" in Path("db/parts/70_views_core.sql").read_text(encoding="utf-8")


def _view_column_names(view_name: str) -> set[str]:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    marker = f"CREATE OR REPLACE VIEW public.{view_name} AS"
    start = sql.index(marker)
    select_start = sql.index("SELECT", start)
    from_start = sql.index("FROM", select_start)
    select_list = sql[select_start:from_start]
    columns: set[str] = set()
    for line in select_list.splitlines():
        stripped = line.strip().rstrip(",")
        if not stripped or stripped == "SELECT":
            continue
        if " AS " in stripped.upper():
            alias = stripped.rsplit(" AS ", 1)[-1].strip().strip('"')
            columns.add(alias)
        elif stripped.startswith('"'):
            columns.add(stripped.strip('"'))
    return columns


def test_operational_latest_operations_table_matches_documents_view() -> None:
    dashboard = _legacy_dashboard("01_operational.json")
    card = next(card for card in dashboard["cards"] if card["name"] == "Последние операции")
    view_columns = _view_column_names("v_rpt_documents_ui")
    configured_columns = {
        column["name"]
        for column in card["visualization_settings"]["table.columns"]
        if column.get("enabled", True)
    }

    assert configured_columns.issubset(view_columns), sorted(configured_columns - view_columns)
    assert "Дата обработки" in configured_columns
    assert "JID+Наименование" in configured_columns
    assert "ИНН клиники" in configured_columns
    assert "Исходный текст ошибки" in configured_columns
    assert "Обработано IPS" not in configured_columns


def test_documents_ui_reads_document_grain_without_view_side_filters() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    transform_sql = Path("db/parts/50_transform.sql").read_text(encoding="utf-8")

    assert 'NULLIF(TRIM("localUid СЭМД"), \'\') IS NOT NULL' not in sql
    assert "NULLIF(btrim(ref.local_uid), '') IS NOT NULL" in transform_sql
    assert "egisz_xml_text" not in transform_sql
    assert '"ИНН клиники"' in sql
    assert '"Исходный текст ошибки"' in sql


def test_service_dashboard_trends_are_hourly_with_period_filter() -> None:
    dashboard = _legacy_dashboard("02_service.json")
    card = next(
        c for c in dashboard["cards"]
        if c.get("name") == "Отказы по часам: связь и асинхронный ответ"
    )
    query = card["dataset_query"]["native"]["query"]
    assert "date_trunc('hour', \"Дата обработки\")" in query
    assert "[[AND {{dwh_date}}]]" in query
    assert card["visualization_settings"]["graph.dimensions"] == ["Час"]
    metrics = card["visualization_settings"]["graph.metrics"]
    assert "Ошибка связи" in metrics
    assert "Ошибка асинхронного ответа РЭМД" in metrics


def test_service_healthcheck_table_scope() -> None:
    dashboard = _legacy_dashboard("02_service.json")
    by_name = {c.get("name"): c for c in dashboard["cards"]}
    table = by_name["Детализация healthcheck"]
    scope = "\"Код сигнала\" NOT IN ('queue_24h', 'pending_backlog_24h')"
    assert scope in table["dataset_query"]["native"]["query"]
    names = set(by_name)
    assert "Топ клиник по доле отказов" not in names
    assert "Статус healthcheck" not in names
    assert "Отказы: асинхронный ответ vs связь" not in names
    assert "Сигналы ELT" not in names

    refusal_pie = by_name["РЭМД vs связь"]
    assert refusal_pie["display"] == "pie"
    assert refusal_pie["row"] == by_name["Отказы по часам: связь и асинхронный ответ"]["row"]
    assert refusal_pie["col"] > by_name["Отказы по часам: связь и асинхронный ответ"]["col"]
    assert 'CASE "Статус (код)"' in refusal_pie["dataset_query"]["native"]["query"]
    assert refusal_pie["visualization_settings"]["pie.metric"] == "Документов"
    assert by_name["Контроль качества данных"]["row"] == table["row"]


def test_service_transport_block_layout() -> None:
    dashboard = _legacy_dashboard("02_service.json")
    by_name = {c.get("name"): c for c in dashboard["cards"]}
    assert by_name["Транзакции по дням и статусам"]["row"] == 0
    assert by_name["Транзакции по дням и статусам"]["row"] < by_name["Тренд ошибок связи по дням"]["row"]
    hourly = by_name["Отказы по часам: связь и асинхронный ответ"]
    pie = by_name["РЭМД vs связь"]
    assert hourly["sizeX"] + pie["sizeX"] == 24
    assert hourly["row"] == pie["row"]
    assert by_name["Сбоев связи за период"]["display"] == "scalar"
    assert "v_rpt_network_errors_detail_ui" in by_name["Тренд ошибок связи по дням"]["dataset_query"]["native"]["query"]


def test_operational_status_breakdown_uses_four_canonical_statuses() -> None:
    dashboard = _legacy_dashboard("01_operational.json")
    latest_card = next(card for card in dashboard["cards"] if card["name"] == "Последние операции")
    card = next(card for card in dashboard["cards"] if card["name"] == "Статусы за период")
    service = _legacy_dashboard("02_service.json")
    trend_card = next(card for card in service["cards"] if card.get("name") == "Транзакции по дням и статусам")
    trend_query = trend_card["dataset_query"]["native"]["query"]
    rows = card["visualization_settings"]["pie.rows"]
    row_keys = {row["key"] for row in rows}

    assert latest_card.get("query_tier") == "query_builder"
    assert card.get("query_tier") == "query_builder"
    assert card["source_model"] == "Документы"
    assert "public.v_egisz_transactions_enriched_ui" not in trend_query
    assert latest_card["metabase-parameter-targets"]["dwh_date"] == {
        "model_ref": "Документы",
        "field_name": "Дата обработки",
    }
    assert card["dataset_query"]["query"]["breakout"] == [["field", "Документы:Статус", None]]
    assert card["visualization_settings"]["pie.metric"] == "Документов"
    assert "Успешно зарегистрирован" in row_keys
    assert "Ошибка асинхронного ответа РЭМД" in row_keys
    assert "Ошибка связи" in row_keys
    assert "В обработке" in row_keys
    assert "Успешный ответ" not in row_keys
    assert "Неизвестная ошибка" not in row_keys
    assert "Нераспознан" not in row_keys
    # Тренд по дням: статус берётся из канонической колонки, без отсечения waiting.
    assert "public.v_rpt_semd_archive_ui" in trend_query
    assert "SELECT DATE(\"Дата обработки\") AS \"Дата\", \"Статус\"" in trend_query
    assert "WHERE \"Статус\" IN ('success', 'error')" not in trend_query
    assert "CREATE OR REPLACE VIEW public.v_rpt_documents_ui" in Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert "FROM public.v_rpt_documents_ui" in Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert trend_card["metabase-field-filters"]["dwh_date"] == {
        "table_ref": "public.v_rpt_semd_archive_ui",
        "field_name": "Дата обработки",
    }


def test_documents_view_exposes_canonical_status_label_and_code() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    core = Path("db/parts/70_views_core.sql").read_text(encoding="utf-8")

    # Единая нотификация статуса (4 значения) задаётся один раз в core-витрине.
    assert "'Успешно зарегистрирован'" in core
    assert "'Ошибка асинхронного ответа РЭМД'" in core
    assert "'Ошибка связи'" in core
    assert "'В обработке'" in core
    assert 'AS "Статус (код)"' in core
    # Презентационная витрина отдаёт RU-нотификацию как «Статус» и машинный «Статус (код)».
    assert '"Статус (отчёт)" AS "Статус"' in sql
    assert '"Статус (код)"' in sql


def test_quality_error_slices_use_documents_ui_not_legacy_error_status() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    queries = _native_queries(dashboard)
    assert all("v_egisz_documents_enriched_ui" not in q for q in queries)
    assert all('"Статус" = \'error\'' not in q for q in queries)


def test_quality_error_totals_use_async_and_network_codes() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(card for card in dashboard["cards"] if card.get("name") == "Тепловая карта: клиника × день")
    query = card["dataset_query"]["native"]["query"]
    assert card["display"] == "table"
    assert '"Статус (код)" IN (\'async_error\', \'network_error\')' in query


def test_quality_success_slices_are_uncapped() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    for name in ("Успешность по клиникам", "Успешность по типам СЭМД"):
        card = next(c for c in dashboard["cards"] if c.get("name") == name)
        query = card["dataset_query"]["native"]["query"]
        assert card["dataset_query"]["type"] == "native"
        assert "LIMIT" not in query.upper()


def test_quality_error_rate_clinic_by_semd_card() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == "% ошибок: клиника × тип СЭМД")
    query = card["dataset_query"]["native"]["query"]

    assert card["display"] == "table"
    assert card["sizeX"] == 12
    assert card["col"] == 0
    assert "v_rpt_documents_ui" in query
    # Срез по парам клиника × тип СЭМД с долей ошибок по документному универсуму.
    assert "GROUP BY 1, 3" in query
    assert '"Статус (код)" IN (\'async_error\', \'network_error\')' in query
    assert '"Статус (код)" IN (\'success\', \'async_error\', \'network_error\')' in query
    assert "COUNT(DISTINCT \"Документ (ключ учёта)\")" in query
    # Показываем только пары с хотя бы одной ошибкой и без ограничения числа строк.
    assert "HAVING COUNT(DISTINCT \"Документ (ключ учёта)\") FILTER (WHERE \"Статус (код)\" IN ('async_error', 'network_error')) > 0" in query
    assert "ORDER BY 4 DESC" in query
    assert '"Код СЭМД"' in query
    assert card["metabase-field-filters"]["dwh_date"] == {
        "table_ref": "public.v_rpt_documents_ui",
        "field_name": "Дата обработки",
    }


def test_quality_error_rate_error_kind_by_semd_card() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == "% ошибок: тип ошибки × тип СЭМД")
    query = card["dataset_query"]["native"]["query"]

    assert card["display"] == "table"
    assert card["sizeX"] == 12
    assert card["col"] == 12
    assert card["row"] == 19
    assert "v_rpt_error_category_breakdown_ui" in query
    assert "WITH pairs AS" in query
    assert "SUM(docs) OVER (PARTITION BY semd)" in query
    assert '"Код СЭМД"' in query
    assert "COUNT(DISTINCT \"Документ (ключ учёта)\")" in query
    assert "ORDER BY 3 DESC" in query
    assert "LIMIT" not in query
    assert card["metabase-field-filters"]["dwh_date"] == {
        "table_ref": "public.v_rpt_error_category_breakdown_ui",
        "field_name": "Обработано IPS",
    }


def test_archive_top_semd_table_shows_share_of_total() -> None:
    dashboard = json.loads(Path("metabase_dashboards/06_semd_archive.json").read_text(encoding="utf-8"))
    card = next(c for c in dashboard["cards"] if c.get("name") == "Топ по типу СЭМД")
    query = card["dataset_query"]["native"]["query"]
    assert 'AS "%"' in query
    assert '["name","%"]' in (card.get("visualization_settings") or {}).get("column_settings", {})


def test_archive_top_semd_uses_same_document_universe_as_total() -> None:
    dashboard = json.loads(Path("metabase_dashboards/06_semd_archive.json").read_text(encoding="utf-8"))
    total = next(card for card in dashboard["cards"] if card["name"] == "Всего документов")
    top = next(card for card in dashboard["cards"] if card["name"] == "Топ по типу СЭМД")
    top_query = top["dataset_query"]["native"]["query"]

    assert '"Код СЭМД"' in top_query
    assert "v_rpt_semd_archive_ui" in total["dataset_query"]["native"]["query"]
    assert "v_rpt_semd_archive_ui" in top_query
    assert "COUNT(DISTINCT \"Документ (ключ учёта)\")" in top_query


def test_transform_backfills_semd_code_from_transactions() -> None:
    sql = Path("db/parts/50_transform.sql").read_text(encoding="utf-8")
    assert "UPDATE public.fact_egisz_documents d" in sql
    assert "FROM public.fact_egisz_transactions t" in sql
    assert "NULLIF(btrim(d.semd_code), '') IS NULL" in sql


def test_document_metric_cards_count_distinct_document_key() -> None:
    document_views = (
        "v_rpt_documents_ui",
        "v_rpt_semd_archive_ui",
        "v_rpt_network_errors_detail_ui",
        "v_rpt_error_category_breakdown_ui",
        "v_rpt_client_documents_ui",
    )
    allowed_count_star = {
        "01_integration_egisz.json": {"v_health_signals_ui"},
        "05_executive.json": {"active_jid"},
        "08_client_bianalytic.json": {"per_patient"},
    }
    violations: list[str] = []
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for card in dashboard["cards"]:
            dq = card.get("dataset_query", {})
            if dq.get("type") not in ("native", "query"):
                continue
            query = _card_query_fingerprint(card)
            if not any(view in query for view in document_views):
                continue
            if "COUNT(*)" not in query:
                continue
            allow = allowed_count_star.get(path.name, set())
            if any(token in query for token in allow):
                continue
            violations.append(f"{path.name} / {card.get('name', '?')}")
    assert not violations, "Document cards must not use COUNT(*) without allowlist: " + ", ".join(violations)


def test_error_interpretations_view_uses_canonical_labels() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE VIEW public.v_rpt_error_interpretations_ui" in sql
    assert "FROM public.v_rpt_documents_ui" in sql
    assert "'Успешно зарегистрирован'" in sql
    assert "Успешный ответ" not in sql


def test_archive_no_code_documents_are_qualified_by_status() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    dashboard = json.loads(Path("metabase_dashboards/06_semd_archive.json").read_text(encoding="utf-8"))
    card = next(card for card in dashboard["cards"] if card["name"] == "Топ по типу СЭМД")
    query = card["dataset_query"]["native"]["query"]

    assert '"СЭМД (архив)"' in sql
    assert "Документ с ошибкой и не определён код" in sql
    assert '"Тип ошибки"' in sql
    assert '"Код СЭМД"' in query
    assert "v_rpt_semd_archive_ui" in query
    assert 'NULLIF(TRIM("Код СЭМД"), \'\') IS NOT NULL' not in query


def test_document_views_choose_latest_journal_entry_before_status_priority() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")

    assert 'NULLIF("LOGID журнала EXCHANGELOG", \'\')::bigint DESC NULLS LAST' in sql
    assert "CASE WHEN document_row_id ~ '^[0-9]+$' THEN document_row_id::bigint END DESC NULLS LAST" in sql


def test_dashboards_do_not_expose_technical_document_key_fallbacks() -> None:
    payload = "\n".join(path.read_text(encoding="utf-8") for path in _dashboard_paths())
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")

    assert "document_group_key" not in payload
    assert "Ключ документа (группировка)" not in payload
    assert "egisz_document_identity_key" not in payload
    assert "MessageID; relatesToMessage" not in payload
    assert "PARTITION BY COALESCE(" not in sql
    assert "PARTITION BY NULLIF(" in sql
    assert "Документ (ключ учёта)" in sql


def test_only_recognized_documents_feed_non_queue_dashboards() -> None:
    transform_sql = Path("db/parts/50_transform.sql").read_text(encoding="utf-8")
    assert "NULLIF(btrim(ref.local_uid), '') IS NOT NULL" in transform_sql
    assert "egisz_xml_text" not in transform_sql
    assert "pending_source AS" not in transform_sql

    quality = _legacy_dashboard("04_quality_and_errors.json")
    quality_queries = _native_queries(quality)
    assert any(
        '"Статус (код)" IN (\'success\', \'async_error\', \'network_error\')' in q
        and '"Код СЭМД"' in q
        for q in quality_queries
    )

    client_service = json.loads(Path("metabase_dashboards/07_client_service.json").read_text(encoding="utf-8"))
    service_queries = _native_queries(client_service)
    assert all("status_code NOT IN ('pending', 'sent')" not in q for q in service_queries)
    assert all("status_code = 'pending'" not in q for q in service_queries)

    bi = json.loads(Path("metabase_dashboards/08_client_bianalytic.json").read_text(encoding="utf-8"))
    bi_queries = _native_queries(bi)
    assert all("status_code NOT IN ('pending', 'sent')" not in q for q in bi_queries)


def test_error_analytics_use_raw_json_column_for_grouping() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    queries = _native_queries(dashboard)

    assert any("v_rpt_error_category_breakdown_ui" in query for query in queries)
    assert all("fact_egisz_transactions" not in query for query in queries)
    assert all("\"Ошибки JSON raw\"" not in query for query in queries)


def test_quality_success_slices_sort_by_total_desc() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    clinic = next(c for c in dashboard["cards"] if c.get("name") == "Успешность по клиникам")
    semd = next(c for c in dashboard["cards"] if c.get("name") == "Успешность по типам СЭМД")
    assert "ORDER BY 2 DESC" in semd["dataset_query"]["native"]["query"]
    assert "ORDER BY 3 DESC" in clinic["dataset_query"]["native"]["query"]
    assert clinic["row"] == 12


def test_quality_dashboard_has_no_slice_section_headers() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    text_cards = [c.get("text", "") for c in dashboard["cards"] if c.get("display") == "text"]
    assert not any(t.startswith("## Успешность по срезам") for t in text_cards)
    assert not any(t.startswith("## Срезы ошибок по парам") for t in text_cards)


def test_quality_error_structure_section_is_category_colored_row_card() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    names = {c.get("name") for c in dashboard["cards"]}
    assert "Ошибки по категории" not in names
    assert "Топ видов ошибок" not in names

    card = next(c for c in dashboard["cards"] if c.get("name") == "Виды ошибок по категориям")
    viz = card["visualization_settings"]
    query = card["dataset_query"]["native"]["query"]

    assert card["display"] == "row"
    assert card["sizeX"] >= 11
    assert "v_rpt_error_category_breakdown_ui" in query
    assert "GROUP BY 1, 2" in query
    assert viz["graph.dimensions"] == ["Категория ошибки", "Вид ошибки"]
    assert viz["stackable.stack_type"] == "stacked"
    assert viz.get("graph.label_value_formatting") == "compact"
    assert viz.get("graph.y_axis.scale") == "linear"
    assert viz.get("graph.x_axis.title_text", "unset") == ""
    assert viz.get("graph.y_axis.title_text", "unset") == ""
    assert "series_settings" not in viz
    _assert_archive_tab_click(card)


def test_quality_semd_error_stacked_bar_hides_negligible_tail() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == "Виды ошибок по типам СЭМД")
    query = card["dataset_query"]["native"]["query"]

    assert card["display"] == "bar"
    assert "rn <= 15" in query
    assert "v_rpt_error_category_breakdown_ui" in query
    assert card["visualization_settings"]["graph.metrics"] == ["Документов"]
    assert card["visualization_settings"]["stackable.stack_type"] == "stacked"
    assert card["visualization_settings"].get("graph.show_stack_values") == "total"
    assert card["visualization_settings"].get("graph.label_value_frequency") == "all"
    assert card["visualization_settings"].get("graph.x_axis.scale") == "ordinal"
    assert card["visualization_settings"].get("graph.x_axis.title_text", "unset") == ""
    assert card["visualization_settings"].get("graph.y_axis.title_text", "unset") == ""
    _assert_archive_tab_click(card)


def test_quality_percent_columns_use_comma_decimal_separator() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    percent_cols: list[str] = []
    for card in dashboard["cards"]:
        cs = (card.get("visualization_settings") or {}).get("column_settings") or {}
        for col_key, settings in cs.items():
            if isinstance(settings, dict) and settings.get("suffix") == " %":
                percent_cols.append(f"{card.get('name')} / {col_key}")
                assert settings.get("decimals") == 1
                assert settings.get("number_separators") == ", "
    assert percent_cols, "expected percent column_settings in quality dashboard"


def test_executive_dashboard_mixes_ops_and_finance_metrics() -> None:
    dashboard = json.loads(Path("metabase_dashboards/05_executive.json").read_text(encoding="utf-8"))
    queries = _native_queries(dashboard)

    assert dashboard["name"] == "Управленческий дашборд"

    # 05 после перестройки опирается ТОЛЬКО на реальные данные DWH.
    # Управленческий дашборд не должен читать снятые service_audit-витрины и таблицы.
    assert all("v_rpt_service_audit_" not in q for q in queries), (
        "05_executive must not reference removed v_rpt_service_audit_* views"
    )
    for retired_table in ("clients", "subscriptions", "billing", "tickets",
                          "sla_metrics", "sed_transfers", "churn_events",
                          "client_costs_monthly"):
        assert all(f"FROM {retired_table}" not in q and f"from {retired_table}" not in q for q in queries), (
            f"05_executive must not reference removed table '{retired_table}'"
        )

    # Источники только реальные.
    assert any("v_rpt_documents_ui" in q for q in queries)
    assert all("v_rpt_documents_no_response_ui" not in q for q in queries)

    # Фикс-тариф 10 000 ₽/JID/мес зашит явно в SQL карточек (раньше прятался в view-константе).
    assert any("10000" in q for q in queries), "MRR formula must use the fixed 10 000 ₽/JID/month tariff"

    assert all("'pending'" not in q for q in queries)


def test_executive_dashboard_uses_section_headers() -> None:
    dashboard = json.loads(Path("metabase_dashboards/05_executive.json").read_text(encoding="utf-8"))
    text_cards = [card for card in dashboard["cards"] if card.get("display") == "text"]
    assert len(text_cards) >= 3, "Управленческий дашборд должен сегментироваться text-заголовками разделов"
    for card in text_cards:
        assert "text" in card and card["text"].strip(), "text-карточка должна содержать содержимое"


def test_client_service_dashboard_uses_jid_filter_and_client_view() -> None:
    dashboard = json.loads(Path("metabase_dashboards/07_client_service.json").read_text(encoding="utf-8"))
    queries = _native_queries(dashboard)

    assert dashboard["name"] == "Клиентский дашборд. Мониторинг сервиса интеграции с ЕГИСЗ"
    assert any(p["name"] == "JID клиники" for p in dashboard["parameters"])
    assert any(p["name"] == "Период" and p.get("default") == "past7days~" for p in dashboard["parameters"])
    assert any(p["name"] == "Тип документа" for p in dashboard["parameters"])
    assert all("public.v_rpt_client_documents_ui" in query for query in queries)
    assert all("{{client_jid}}" in query for query in queries)


def test_client_bianalytic_dashboard_uses_hashed_unique_keys() -> None:
    dashboard = json.loads(Path("metabase_dashboards/08_client_bianalytic.json").read_text(encoding="utf-8"))
    queries = _native_queries(dashboard)

    assert dashboard["name"] == "Клиентский дашборд. BI-аналитика ЭМД"
    assert any(p["name"] == "JID клиники" for p in dashboard["parameters"])
    assert all("public.v_rpt_client_documents_ui" in query for query in queries)
    assert all("{{client_jid}}" in query for query in queries)
    # Уникальный счёт пациентов/врачей идёт через hash-колонки, не через masked-имена.
    assert any("patient_hash" in q for q in queries)
    assert any("doctor_hash" in q for q in queries)


def test_client_dashboards_field_filters_are_bound_to_client_view() -> None:
    for path_name in ("07_client_service.json", "08_client_bianalytic.json"):
        dashboard = json.loads(Path(f"metabase_dashboards/{path_name}").read_text(encoding="utf-8"))
        for card in dashboard["cards"]:
            if card.get("dataset_query", {}).get("type") != "native":
                continue
            tags = card["dataset_query"]["native"]["template-tags"]
            filters = card.get("metabase-field-filters", {})

            assert tags["client_jid"]["type"] == "text", f"{path_name}: JID должен быть text-тегом, иначе JID-фильтр не работает"
            assert tags["client_jid"]["required"] is True, f"{path_name}: JID должен быть required"
            assert tags["client_period"]["type"] == "dimension"
            assert tags["client_document_type"]["type"] == "dimension"
            assert filters["client_period"] == {
                "table_ref": "public.v_rpt_client_documents_ui",
                "field_name": "document_ts",
            }
            assert filters["client_document_type"] == {
                "table_ref": "public.v_rpt_client_documents_ui",
                "field_name": "document_type",
            }


def test_client_dashboard_dwh_view_masks_patient_fields_and_exposes_hashes() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")

    assert "CREATE OR REPLACE VIEW public.v_rpt_client_documents_ui" in sql
    assert "patient_name_masked" in sql
    assert "snils_masked" in sql
    assert "doctor_name" in sql
    # surrogate-ID для BI-дашборда: считать уникальных пациентов/врачей по hash без раскрытия ФИО/СНИЛС
    assert "patient_hash" in sql
    assert "doctor_hash" in sql


def test_all_pie_charts_have_v2_binding() -> None:
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for card in dashboard.get("cards", []):
            if card.get("display") != "pie":
                continue
            name = card.get("name", path.name)
            viz = card.get("visualization_settings", {})
            assert viz.get("pie.metric"), f"{path.name} / {name}: missing pie.metric"
            dims = viz.get("pie.dimension") or viz.get("graph.dimensions")
            assert dims, f"{path.name} / {name}: missing pie.dimension"
            assert viz.get("graph.metrics"), f"{path.name} / {name}: missing graph.metrics"
            assert viz.get("graph.dimensions"), f"{path.name} / {name}: missing graph.dimensions"
            for row in viz.get("pie.rows", []):
                assert row.get("enabled") is True, f"{path.name} / {name}: pie.rows without enabled"
                assert row.get("originalName"), f"{path.name} / {name}: pie.rows without originalName"


def test_dashboard_text_uses_measurement_entities_not_kpi() -> None:
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        blob = dashboard.get("description", "")
        for card in dashboard.get("cards", []):
            if card.get("display") == "text":
                blob += "\n" + card.get("text", "")
        assert "KPI" not in blob, f"{path.name} still mentions KPI in user-facing text"


def test_shared_cards_use_identical_queries_across_dashboards() -> None:
    """Дашборды переиспользуют одну Metabase-карточку по имени — SQL должен совпадать."""
    by_name: dict[str, list[tuple[str, str]]] = {}
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for card in dashboard.get("cards", []):
            if card.get("display") == "text" or not card.get("name"):
                continue
            dq = card.get("dataset_query", {})
            if dq.get("type") not in ("native", "query"):
                continue
            query = _card_query_fingerprint(card)
            by_name.setdefault(card["name"], []).append((path.name, query))

    mismatches: list[str] = []
    for name, instances in by_name.items():
        if len(instances) < 2:
            continue
        reference_query = instances[0][1]
        reference_file = instances[0][0]
        for path_name, query in instances[1:]:
            if query != reference_query:
                mismatches.append(f"{name!r}: {reference_file} vs {path_name}")
    assert not mismatches, "Shared card names must reference the same SQL: " + ", ".join(mismatches)


def test_shared_cards_use_identical_field_filters_across_dashboards() -> None:
    """Переиспользуемые карточки должны иметь одинаковые metabase-field-filters."""
    by_name: dict[str, list[tuple[str, dict]]] = {}
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for card in dashboard.get("cards", []):
            if card.get("display") == "text" or not card.get("name"):
                continue
            dq = card.get("dataset_query", {})
            if dq.get("type") != "native":
                continue
            filters = card.get("metabase-field-filters") or {}
            by_name.setdefault(card["name"], []).append((path.name, filters))

    mismatches: list[str] = []
    for name, instances in by_name.items():
        if len(instances) < 2:
            continue
        reference = json.dumps(instances[0][1], sort_keys=True, ensure_ascii=False)
        reference_file = instances[0][0]
        for path_name, filters in instances[1:]:
            if json.dumps(filters, sort_keys=True, ensure_ascii=False) != reference:
                mismatches.append(f"{name!r}: {reference_file} vs {path_name}")
    assert not mismatches, "Shared card names must use the same field filters: " + ", ".join(mismatches)


def test_dashboards_use_canonical_filter_slugs() -> None:
    """Общие фильтры JID/тип СЭМД — единые slug на всех дашбордах (кроме клиентских)."""
    legacy = {"top_clinic_filter", "top_semd_filter"}
    for path in _dashboard_paths():
        if path.name in ("07_client_service.json", "08_client_bianalytic.json"):
            continue
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        slugs = {p.get("slug") for p in dashboard.get("parameters", [])}
        hit = slugs & legacy
        assert not hit, f"{path.name} still uses legacy filter slug(s): {sorted(hit)}"


def test_dashboard_cards_use_canonical_jid_semd_template_tags() -> None:
    """SQL template-tags jid/semd_type — единые имена, без legacy top_clinic/top_semd."""
    legacy = {"top_clinic", "top_semd"}
    hits: list[str] = []
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for card in dashboard.get("cards", []):
            dq = card.get("dataset_query") or {}
            native = dq.get("native") or {}
            query = native.get("query") or ""
            tags = set((native.get("template-tags") or {}).keys())
            card_ref = f"{path.name} / {card.get('name', '?')}"
            if tags & legacy:
                hits.append(f"{card_ref}: template-tags {sorted(tags & legacy)}")
            for old in legacy:
                if f"{{{{{old}}}}}" in query:
                    hits.append(f"{card_ref}: query still has {{{{{old}}}}}")
    assert not hits, "Legacy jid/semd template tags: " + "; ".join(hits)


ERROR_PERIOD_CARD = "% ошибок. Тип ошибки × Клиника × Тип СЭМД"


def test_operational_error_period_card_uses_atomic_error_types() -> None:
    """Карточка error period — только атомарные виды из breakdown, без «Сводки ошибки»."""
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == ERROR_PERIOD_CARD)
    query = card["dataset_query"]["native"]["query"]
    assert "v_rpt_error_category_breakdown_ui" in query
    assert "Сводка ошибки" not in query
    assert "error_interpretations" not in query
    cols = {c["name"] for c in card["visualization_settings"].get("table.columns", [])}
    assert "Сводка ошибки" not in cols
    assert "Исходный текст ошибки" not in cols


def test_error_period_card_percent_is_share_within_clinic_semd() -> None:
    """«% ошибок» — доля вида среди всех ошибок в паре клиника × тип СЭМД."""
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == ERROR_PERIOD_CARD)
    query = card["dataset_query"]["native"]["query"]
    assert 'SUM(g."Ошибок") OVER (PARTITION BY g."JID клиники", g."Тип СЭМД (код · НСИ)")' in query


def test_error_period_card_avoids_documents_ui_rescan() -> None:
    """Карточка читает breakdown напрямую — без повторного скана v_rpt_documents_ui."""
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == ERROR_PERIOD_CARD)
    query = card["dataset_query"]["native"]["query"]
    assert "v_rpt_documents_ui" not in query
    assert "v_rpt_error_category_breakdown_ui b" not in query
    assert "[[AND {{jid}}]]" in query
    assert "dim_organizations" in query
    filters = card.get("metabase-field-filters") or {}
    assert filters["dwh_date"]["table_ref"] == "public.v_rpt_error_category_breakdown_ui"


def test_error_period_card_uses_canonical_filter_tags() -> None:
    """Карточка error period на дашборде 04 — теги jid/semd_type."""
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == ERROR_PERIOD_CARD)
    tags = card["dataset_query"]["native"]["template-tags"]
    assert "jid" in tags
    assert "semd_type" in tags
    param_slugs = {p["slug"] for p in dashboard["parameters"]}
    assert "jid_filter" in param_slugs
    assert "semd_type_filter" in param_slugs


def test_pie_charts_with_decimals_use_comma_separator() -> None:
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for card in dashboard.get("cards", []):
            if card.get("display") != "pie":
                continue
            viz = card.get("visualization_settings") or {}
            if viz.get("pie.decimal_places", 0) < 1:
                continue
            metric = viz.get("pie.metric") or (viz.get("graph.metrics") or [None])[0]
            assert metric, f"{path.name} / {card.get('name')}: pie without metric"
            key = f'["name","{metric}"]'
            settings = (viz.get("column_settings") or {}).get(key) or {}
            percent = (viz.get("column_settings") or {}).get('["name","_percentage"]') or {}
            assert percent.get("number_separators") == ", ", (
                f"{path.name} / {card.get('name')}: pie slice percent needs comma decimal separator"
            )
            assert percent.get("decimals") == 1, (
                f"{path.name} / {card.get('name')}: pie slice percent needs 1 decimal place"
            )
            assert viz.get("pie.decimal_places") == 1, (
                f"{path.name} / {card.get('name')}: pie.decimal_places must be 1"
            )
            if settings:
                assert settings.get("number_separators") == ", ", (
                    f"{path.name} / {card.get('name')}: pie count metric needs RU separator"
                )
                assert settings.get("decimals") == 0, (
                    f"{path.name} / {card.get('name')}: pie count metric must be integer"
                )


def test_error_types_pie_hides_misleading_total() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == "Виды ошибок по категориям")
    assert card["display"] == "row"


def test_dashboard_numeric_formatting_uses_ru_default() -> None:
    """Целые — без дробной части; проценты — до десятых; разделитель 000 000 / запятая."""
    text_markers = (
        "Клиника", "клиника", "Вид ошибки", "Категория ошибки", "Тип ошибки", "Тип СЭМД",
        "Наименование", "Код СЭМД", "Статус", "День", "Дата", "Час", "lbl", "code",
        "Сегмент", "Тип сетевой", "Категория", "Сигнал", "localUid", "OID", "ИНН",
        "СНИЛС", "ФИО", "текст", "Сводка", "Исходный", "Хост", "Сообщение", "emdrid",
        "Рег. номер", "relatesTo", "Врач", "Пациент", "document_type",
    )
    bad: list[str] = []
    missing: list[str] = []
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for card in dashboard.get("cards", []):
            card_name = card.get("name", "(text)")
            cs = (card.get("visualization_settings") or {}).get("column_settings") or {}
            for col_key, settings in cs.items():
                if not isinstance(settings, dict):
                    continue
                if not any(k in settings for k in ("decimals", "number_separators", "suffix")):
                    continue
                if any(marker in col_key for marker in text_markers) and not col_key.endswith('"_percentage"]'):
                    if settings.get("suffix") != " %":
                        continue
                if "number_separators" not in settings:
                    missing.append(f"{path.name} / {card_name} / {col_key}")
                    continue
                if settings["number_separators"] != ", ":
                    bad.append(
                        f"{path.name} / {card_name} / {col_key}: {settings['number_separators']!r}"
                    )
                    continue
                if settings.get("suffix") == " %" or col_key.endswith('"_percentage"]'):
                    expected_decimals = 1
                elif "/" in col_key or ", мин" in col_key or "Дней в ожидании" in col_key:
                    expected_decimals = 1
                elif (
                    "₽ за успешный СЭМД" in col_key
                    and card_name == "Эфф. цена успешного СЭМД, ₽"
                ):
                    expected_decimals = 1
                else:
                    expected_decimals = 0
                if settings.get("decimals") != expected_decimals:
                    bad.append(
                        f"{path.name} / {card_name} / {col_key}: decimals={settings.get('decimals')}"
                    )
    assert not bad, "Non-standard numeric formatting: " + ", ".join(bad)
    assert not missing, "Numeric columns missing separator: " + ", ".join(missing)


def test_network_errors_view_exposes_canonical_local_uid() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert 'AS "localUid СЭМД"' in sql
    assert "CREATE OR REPLACE VIEW public.v_rpt_network_errors_detail_ui" in sql


def test_no_response_view_exposes_wait_segments() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    dashboard = _legacy_dashboard("03_documents_no_response.json")

    assert '>30 дней' in sql
    assert '>7 дней' in sql
    assert '>3 дней' in sql
    assert 'до 3 дней' in sql
    assert '"Сегмент ожидания"' in sql
    assert '"Дней в ожидании"' in sql

    names = {c.get("name") for c in dashboard["cards"]}
    assert "Сегменты ожидания" in names
    assert "Зависших >3 дней" in names
    assert "Зависших >7 дней" in names
    assert "Зависших >30 дней" in names
    assert "Зависшие — детализация" not in names
    assert "Возраст зависших документов" not in names

    table = next(c for c in dashboard["cards"] if c.get("name") == "Очередь без ответа")
    formatting = table["visualization_settings"]["table.column_formatting"]
    values = {rule["value"] for rule in formatting}
    assert values == {">3 дней", ">7 дней", ">30 дней"}


def test_scalar_cards_use_appropriate_display_format() -> None:
    executive = json.loads(Path("metabase_dashboards/05_executive.json").read_text(encoding="utf-8"))
    clinic_card = next(c for c in executive["cards"] if c.get("name") == "Клиник без единого успеха")
    clinic_fmt = clinic_card["visualization_settings"]["column_settings"]['["name","Клиник без успеха"]']
    assert clinic_fmt["decimals"] == 0
    assert "suffix" not in clinic_fmt
    assert clinic_fmt["number_separators"] == ", "

    client = json.loads(Path("metabase_dashboards/07_client_service.json").read_text(encoding="utf-8"))
    by_name = {c.get("name"): c for c in client["cards"]}
    for text_scalar in (
        "Успешно зарегистрирован",
        "Ошибка асинхронного ответа РЭМД",
        "Ошибка связи",
        "Среднее время регистрации СЭМД",
    ):
        viz = by_name[text_scalar]["visualization_settings"]
        assert "column_settings" not in viz, f"{text_scalar} returns pre-formatted text"

    bi = json.loads(Path("metabase_dashboards/08_client_bianalytic.json").read_text(encoding="utf-8"))
    ratio = next(c for c in bi["cards"] if c.get("name") == "ЭМД на пациента (среднее)")
    ratio_fmt = ratio["visualization_settings"]["column_settings"]['["name","ЭМД/пациент"]']
    assert ratio_fmt["decimals"] == 1

    names = {p.name for p in _dashboard_paths()}
    # старые версии переименованных дашбордов должны быть удалены, иначе setup-dashboards.sh
    # импортирует дубликаты в коллекцию.
    assert "01_integration_egisz.json" in names
    assert "01_operational.json" not in names
    assert "02_service.json" not in names
    assert "03_documents_no_response.json" not in names
    assert "04_quality_and_errors.json" not in names
    assert "05_executive.json" in names
    assert "07_client_service.json" in names
    assert "08_client_bianalytic.json" in names


def test_integration_dashboard_has_tabs_and_legacy_card_coverage() -> None:
    dashboard = _integration_dashboard()
    assert dashboard["name"] == "Интеграция с ЕГИСЗ"
    assert dashboard.get("width") == "full"
    tabs = dashboard.get("tabs") or []
    assert len(tabs) == 5
    assert [tab["name"] for tab in tabs] == [
        "Оперативный",
        "Сервис",
        "Очередь без ответа",
        "Ошибки",
        "Архив СЭМД",
    ]
    by_tab: dict[str, int] = {}
    for card in dashboard["cards"]:
        tab = card.get("tab")
        assert tab, f"card {card.get('name', '?')} missing tab"
        by_tab[tab] = by_tab.get(tab, 0) + 1
    assert by_tab["operational"] == 7
    assert by_tab["service"] == 12
    assert by_tab["queue"] == 8
    assert by_tab["errors"] == 8
    assert by_tab["archive"] == 6


def test_errors_and_service_tabs_have_no_scalar_kpi_row() -> None:
    dashboard = _integration_dashboard()
    removed = {
        "Документов с ошибкой",
        "Доля ошибок, %",
        "Клиник с ошибками",
        "Ошибок регистрации в РЭМД",
        "Сводка прокси-БД и очереди",
        "Документов за период",
        "Доля ошибок за период, %",
        "Успешно за период",
        "В обработке",
    }
    for tab in ("errors", "service"):
        names = {c.get("name") for c in dashboard["cards"] if c.get("tab") == tab}
        assert not names & removed, f"{tab} still has scalar KPI cards: {names & removed}"


def test_archive_tab_uses_same_clinic_volume_card_as_operational() -> None:
    dashboard = _integration_dashboard()
    operational = next(
        c for c in dashboard["cards"]
        if c.get("tab") == "operational" and c.get("name") == "Объём по клиникам"
    )
    archive = next(
        c for c in dashboard["cards"]
        if c.get("tab") == "archive" and c.get("name") == "Объём по клиникам"
    )
    query = operational["dataset_query"]["native"]["query"]
    assert operational["dataset_query"]["type"] == "native"
    assert archive["dataset_query"]["type"] == "native"
    assert _card_query_fingerprint(operational) == _card_query_fingerprint(archive)
    assert 'AS "%"' in query
    assert "NULLIF((SELECT total FROM totals), 0)" in query
    assert archive["row"] == 6 and archive["col"] == 12


def test_archive_tab_matches_standalone_dashboard_cards() -> None:
    integration = _integration_dashboard()
    standalone = json.loads(Path("metabase_dashboards/06_semd_archive.json").read_text(encoding="utf-8"))
    archive_names = {
        c["name"]
        for c in integration["cards"]
        if c.get("tab") == "archive" and c.get("display") != "text"
    }
    standalone_names = {
        c["name"]
        for c in standalone["cards"]
        if c.get("display") != "text"
    }
    assert archive_names == standalone_names


def test_operational_tab_restores_legacy_slice_cards() -> None:
    dashboard = _legacy_dashboard("01_operational.json")
    names = {c.get("name") for c in dashboard["cards"] if c.get("display") != "text"}
    for required in (
        "Ошибки по клиникам: объём и %",
        "Ошибки по типу",
        "Ошибок по СЭМД",
        "Топ по типу СЭМД",
    ):
        assert required in names


def test_metabase_models_catalog_exists() -> None:
    models = sorted(Path("metabase_models").glob("*.json"))
    assert len(models) == 6
    documents = json.loads(Path("metabase_models/01_documents.json").read_text(encoding="utf-8"))
    assert documents["table_ref"] == "public.v_rpt_documents_ui"
    assert "Статус" in documents["fields"]
    assert "Статус (код)" in documents["hidden_fields"]
    no_response = json.loads(Path("metabase_models/04_no_response.json").read_text(encoding="utf-8"))
    assert "Сегмент ожидания" in no_response["fields"]


def test_qb_support_views_exist_in_dwh_sql() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE VIEW public.v_rpt_clinic_semd_slice_ui" in sql
    assert "CREATE OR REPLACE VIEW public.v_rpt_client_kpi_daily_ui" in sql


def test_quality_qb_cards_use_models_and_archive_click() -> None:
    dashboard = _legacy_dashboard("01_operational.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == "Объём по клиникам")
    query = card["dataset_query"]["native"]["query"]
    assert card["dataset_query"]["type"] == "native"
    assert 'AS "%"' in query
    _assert_archive_tab_click(card)


def test_operational_volume_card_shows_share_of_total() -> None:
    dashboard = _legacy_dashboard("01_operational.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == "Объём по клиникам")
    query = card["dataset_query"]["native"]["query"]
    assert card["dataset_query"]["type"] == "native"
    assert 'AS "%"' in query
    enabled = {
        col["name"]
        for col in card["visualization_settings"]["table.columns"]
        if col.get("enabled", True)
    }
    assert "%" in enabled
    assert "Документов" in enabled
