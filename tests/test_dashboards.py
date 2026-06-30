from __future__ import annotations

import json
from pathlib import Path

INTEGRATION_DASHBOARD = Path("metabase_dashboards/01_integration_egisz.json")
TAB_BY_LEGACY = {
    "01_operational.json": "operational",
    "02_service.json": "service",
    "03_documents_no_response.json": "queue",
    "04_quality_and_errors.json": "errors",
}


def _archive_click_target(card: dict) -> dict:
    return card.get("click_behavior") or {}


def _archive_tab_cards(dashboard: dict | None = None) -> list[dict]:
    dash = dashboard or _integration_dashboard()
    return [c for c in dash["cards"] if c.get("tab") == "archive" and c.get("display") != "text"]


def _operational_tab_cards(dashboard: dict | None = None) -> list[dict]:
    dash = dashboard or _integration_dashboard()
    return [c for c in dash["cards"] if c.get("tab") == "operational" and c.get("display") != "text"]


def _assert_documents_drill_through(card: dict, expected_slugs: set[str] | None = None) -> None:
    click = _archive_click_target(card)
    assert click.get("targetDashboard") == "Интеграция с ЕГИСЗ"
    assert click.get("tab") == "archive"
    mapping = click.get("parameterMapping") or {}
    if expected_slugs is not None:
        assert expected_slugs <= set(mapping), f"missing mappings: {expected_slugs - set(mapping)}"
    else:
        assert mapping, f"{card.get('name')} drill-through must map dashboard parameters"


def _assert_model_drill_through(
    card: dict,
    model_ref: str,
    expected_fields: set[str],
    *,
    contains_fields: set[str] | None = None,
) -> None:
    click = _archive_click_target(card)
    assert click.get("linkType") == "question"
    assert click.get("targetModel") == model_ref
    mapping = click.get("parameterMapping") or {}
    fields = {
        (spec.get("target") or {}).get("field_name") or key
        for key, spec in mapping.items()
    }
    assert expected_fields <= fields, f"missing field mappings: {expected_fields - fields}"
    # Тип ошибки матчится по вхождению в полный список error_types модели «Документы» —
    # документ с несколькими ошибками не теряется.
    for field_name in contains_fields or set():
        spec = next(
            (
                value
                for value in mapping.values()
                if (value.get("target") or {}).get("field_name") == field_name
            ),
            None,
        )
        assert spec is not None, f"missing contains mapping for {field_name!r}"
        assert (spec.get("target") or {}).get("operator") == "contains"


def _assert_archive_tab_click(card: dict) -> None:
    _assert_documents_drill_through(card)


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

    assert "network_error_type" in query
    assert "public.network_error_type(r.error_text) AS network_error_type" in sql
    assert "per_kind AS" in query
    assert "[[AND {{ips_date}}]]" in query
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
    card = next(card for card in dashboard["cards"] if card.get("name") == "Топ категорий и типов ошибки")
    query = card["dataset_query"]["native"]["query"]

    assert card["display"] == "row"
    assert "rpt_error_breakdown" in query
    assert "error_category" in query
    assert '"Тип ошибки"' in query
    assert "public.documents doc" in sql
    assert "INNER JOIN public.rpt_documents r ON r.dwh_id = a.dwh_id" in sql
    assert "doc.status IN ('async_error', 'network_error')" in sql
    tables_sql = Path("db/parts/10_tables.sql").read_text(encoding="utf-8")
    assert "'Ошибка связи'" in tables_sql


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
        else:
            token = stripped.split()[0]
            if token.isidentifier():
                columns.add(token)
    return columns


def _load_model(model_file: str) -> dict:
    return json.loads(Path(f"metabase_models/{model_file}").read_text(encoding="utf-8"))


def _model_display_names(model_file: str) -> set[str]:
    model = _load_model(model_file)
    return {
        meta["display_name"]
        for meta in model["fields"].values()
        if meta.get("display_name")
    }


DOCUMENTS_TABLE_LEGACY_LABELS = {
    "Клиника",
    "Host Клиники (ГОСТ VPN)",
    "Host",
    "ИНН Клиники",
    "OID Клиники",
    "Тип ошибки",
    "СЭМД",
}


def test_operational_latest_operations_table_matches_documents_view() -> None:
    dashboard = _legacy_dashboard("01_operational.json")
    card = next(card for card in dashboard["cards"] if card["name"] == "Последние операции")
    # rpt_documents = SELECT * FROM rpt_document_versions WHERE is_current_version —
    # колоночный проекшн (с алиасами) живёт в базовой rpt_document_versions.
    view_columns = _view_column_names("rpt_document_versions")
    allowed_columns = _model_display_names("01_documents.json") | DOCUMENTS_TABLE_LEGACY_LABELS | view_columns
    configured_columns = {
        column["name"]
        for column in card["visualization_settings"]["table.columns"]
        if column.get("enabled", True)
    }

    assert configured_columns.issubset(allowed_columns), sorted(configured_columns - allowed_columns)
    assert "processed_at" in configured_columns or "Дата обработки" in configured_columns
    assert "Клиника" in configured_columns
    assert "СЭМД" in configured_columns
    assert "Код СЭМД" not in configured_columns
    assert "Наименование СЭМД" not in configured_columns
    assert "День" not in configured_columns
    assert "error_type" in configured_columns or "Тип ошибки" in configured_columns or "Сводка ошибки" in configured_columns
    assert "Host Клиники (ГОСТ VPN)" in configured_columns or "Host" in configured_columns
    query = card["dataset_query"]["query"]
    assert "fields" in query
    field_refs = {f[1] for f in query["fields"]}
    assert "Документы:semd_label" in field_refs
    assert "Документы:semd_code" not in field_refs
    assert "Документы:error_summary" in field_refs
    assert "Документы:error_type" not in field_refs


def test_documents_ui_reads_document_grain_without_view_side_filters() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    transform_sql = Path("db/parts/50_transform.sql").read_text(encoding="utf-8")

    assert "NULLIF(btrim(d.dwh_id), '') IS NOT NULL" in sql
    assert "NULLIF(btrim(e.semd_local_uid), '') IS NOT NULL" not in sql
    assert "NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL" in transform_sql
    assert "egisz_xml_text" not in transform_sql
    assert "clinic_inn" in sql
    assert "error_text" in sql
    assert "error_summary" in sql


def test_service_dashboard_trends_are_hourly_with_period_filter() -> None:
    dashboard = _legacy_dashboard("02_service.json")
    card = next(
        c for c in dashboard["cards"]
        if c.get("name") == "Отказы по часам: связь и асинхронный ответ"
    )
    query = card["dataset_query"]["native"]["query"]
    assert "date_trunc('hour', ips_date)" in query
    assert "[[AND {{ips_date}}]]" in query
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
    assert "CASE status" in refusal_pie["dataset_query"]["native"]["query"]
    assert refusal_pie["visualization_settings"]["pie.metric"] == "Документов"
    assert by_name["Контроль качества данных"]["row"] == table["row"]


def test_service_transport_block_layout() -> None:
    dashboard = _legacy_dashboard("02_service.json")
    by_name = {c.get("name"): c for c in dashboard["cards"]}
    hourly = by_name["Отказы по часам: связь и асинхронный ответ"]
    pie = by_name["РЭМД vs связь"]
    assert hourly["row"] == 0
    assert hourly["row"] < by_name["Тренд ошибок связи по дням"]["row"]
    assert hourly["sizeX"] + pie["sizeX"] == 24
    assert hourly["row"] == pie["row"]
    assert by_name["Сбоев связи за период"]["display"] == "scalar"
    assert "rpt_network_errors" in by_name["Тренд ошибок связи по дням"]["dataset_query"]["native"]["query"]


def test_operational_status_breakdown_uses_four_canonical_statuses() -> None:
    dashboard = _legacy_dashboard("01_operational.json")
    latest_card = next(card for card in dashboard["cards"] if card["name"] == "Последние операции")
    card = next(card for card in dashboard["cards"] if card["name"] == "Статусы за период")
    trend_card = next(card for card in dashboard["cards"] if card.get("name") == "Транзакции по дням и статусам")
    trend_query = trend_card["dataset_query"]["native"]["query"]
    rows = card["visualization_settings"]["pie.rows"]
    row_keys = {row["key"] for row in rows}

    assert latest_card.get("query_tier") == "query_builder"
    assert card.get("query_tier") == "query_builder"
    assert card["source_model"] == "Документы"
    assert "public.v_egisz_transactions_enriched_ui" not in trend_query
    assert latest_card["metabase-parameter-targets"]["ips_date"] == {
        "model_ref": "Документы",
        "field_name": "ips_date",
    }
    assert card["dataset_query"]["query"]["breakout"] == [["field", "Документы:status_label", None]]
    assert card["visualization_settings"]["pie.metric"] == "Документов"
    assert "Успешно зарегистрирован" in row_keys
    assert "Ошибка асинхронного ответа РЭМД" in row_keys
    assert "Ошибка связи" in row_keys
    assert "Отправлено" in row_keys
    assert "Успешный ответ" not in row_keys
    assert "Неизвестная ошибка" not in row_keys
    assert "Нераспознан" not in row_keys
    # Тренд по дням: текущий статус документа и день из ips_date.
    assert "public.rpt_documents" in trend_query
    assert "ips_date::date" in trend_query
    assert "status_label" in trend_query
    assert "public.transactions" not in trend_query
    assert "WHERE \"Статус\" IN ('success', 'error')" not in trend_query
    assert "CREATE OR REPLACE VIEW public.rpt_documents" in Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert "FROM public.rpt_documents" in Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert trend_card["metabase-field-filters"]["ips_date"] == {
        "table_ref": "public.rpt_documents",
        "field_name": "ips_date",
    }


def test_documents_view_exposes_canonical_status_label_and_code() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    core = Path("db/parts/70_views_core.sql").read_text(encoding="utf-8")
    tables_sql = Path("db/parts/10_tables.sql").read_text(encoding="utf-8")

    # Канонические RU-лейблы задаются один раз в dim_document_status.
    assert "'Успешно зарегистрирован'" in tables_sql
    assert "'Ошибка асинхронного ответа РЭМД'" in tables_sql
    assert "'Ошибка связи'" in tables_sql
    assert "'Отправлено'" in tables_sql
    assert "ds.label AS status_label" in sql
    assert "d.status" in sql
    assert "COALESCE(d.last_callback_at, d.registered_at, d.first_sent_at) AS ips_date" in sql
    assert "ips_date" in sql
    assert "status," in sql
    assert "status_label," in sql


def test_documents_view_unifies_logid_msgid_naming() -> None:
    """Транспорт СЭМД: request_*/result_* + COALESCE LOGID (у «Отправлено» не пусто) +
    request_msgid из document_attributes (README §«Парсинг»)."""
    rpt = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    core = Path("db/parts/70_views_core.sql").read_text(encoding="utf-8")
    transform = Path("db/parts/50_transform.sql").read_text(encoding="utf-8")

    # LOGID состояния: исход если есть, иначе отправка — больше не NULL для «Отправлено».
    assert "COALESCE(d.result_logid, d.request_logid)::text AS logid" in rpt
    assert "d.request_logid::text AS request_logid" in rpt
    assert "d.result_logid::text AS result_logid" in rpt
    # MSGID: запрос (из document_attributes) + ответ + relatesTo.
    assert "a.request_msgid" in rpt
    assert "d.result_msgid" in rpt
    assert "AS relates_to_msgid" in rpt
    # Корпус результатов выводится из status напрямую — отдельного флага is_resolved нет.
    assert "is_resolved" not in rpt
    # request_msgid наполняется из source-транзакции в document_attributes.
    assert "request_msgid text" in core
    assert "AS request_msgid" in core
    # Stored-схема использует новые имена (старые callback_log_id/source_logid отсутствуют).
    assert "callback_log_id" not in transform
    assert "source_logid" not in transform


def test_documents_model_exposes_transport_fields() -> None:
    documents = json.loads(Path("metabase_models/01_documents.json").read_text(encoding="utf-8"))
    for field in ("request_logid", "result_logid", "request_msgid", "result_msgid",
                  "relates_to_msgid", "logid"):
        assert field in documents["fields"], field
    assert "message_id" not in documents["hidden_fields"]
    # Корпус — из status; отдельной сущности is_resolved быть не должно.
    assert "is_resolved" not in documents["fields"]


def test_status_distribution_cards_exclude_waiting() -> None:
    """Статус-карточки строятся только на документах с вердиктом РЭМД: «Отправлено»
    (status='waiting') не входит в распределение (статистика не искажается)."""
    dashboard = _integration_dashboard()
    by_name = {c.get("name"): c for c in dashboard["cards"]}

    trend = by_name["Транзакции по дням и статусам"]
    trend_sql = trend["dataset_query"]["native"]["query"]
    assert "status <> 'waiting'" in trend_sql
    assert "Отправлено" not in trend.get("visualization_settings", {}).get("series_settings", {})

    # Фильтр по видимому status_label, а не по скрытому status: скрытые поля модели не
    # резолвятся импортёром Metabase и фильтр молча отбрасывается (карточка показывала
    # «Отправлено»). status_label виден (используется в breakout) и резолвится корректно.
    donut = by_name["Статусы за период"]
    flt = donut["dataset_query"]["query"].get("filter")
    assert flt == ["!=", ["field", "Документы:status_label", None], "Отправлено"]


def test_quality_error_slices_use_documents_ui_not_legacy_error_status() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    queries = _native_queries(dashboard)
    assert all("REMOVED_ENRICHED_UI" not in q for q in queries)
    assert all('"Статус" = \'error\'' not in q for q in queries)


def test_quality_error_totals_use_async_and_network_codes() -> None:
    dashboard = _integration_dashboard()
    card = next(card for card in dashboard["cards"] if card.get("name") == "Тепловая карта: клиника × день")
    query = card["dataset_query"]["native"]["query"]
    viz = card["visualization_settings"]
    assert card["display"] == "table"
    assert viz.get("table.pivot") is True
    assert viz.get("table.pivot_column") == "День"
    assert viz.get("table.pivot_row") == "Клиника"
    assert viz.get("table.cell_column") == "Доля ошибок, %"
    assert "clinic_label" in query
    assert "status IN ('async_error', 'network_error')" in query


def test_quality_success_slices_are_uncapped() -> None:
    dashboard = _integration_dashboard()
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
    assert "rpt_documents" in query
    # Срез по парам клиника × тип СЭМД с долей ошибок по документному универсуму.
    assert "GROUP BY 1, 3" in query
    assert "status IN ('async_error', 'network_error')" in query
    assert "status IN ('success', 'async_error', 'network_error')" in query
    assert "COUNT(DISTINCT dwh_id)" in query
    # Показываем только пары с хотя бы одной ошибкой и без ограничения числа строк.
    assert "HAVING COUNT(DISTINCT dwh_id) FILTER (WHERE status IN ('async_error', 'network_error')) > 0" in query
    assert "ORDER BY 4 DESC" in query
    assert "semd_code" in query
    assert card["metabase-field-filters"]["ips_date"] == {
        "table_ref": "public.rpt_documents",
        "field_name": "ips_date",
    }


def test_quality_error_rate_error_kind_by_semd_card() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == "% ошибок: тип ошибки × тип СЭМД")
    query = card["dataset_query"]["native"]["query"]

    assert card["display"] == "table"
    assert card["sizeX"] == 12
    assert card["col"] == 12
    assert card["row"] == 0
    assert "rpt_error_breakdown" in query
    assert "WITH pairs AS" in query
    # Знаменатель «% ошибок» — документы (COUNT DISTINCT) по типу СЭМД, без двойного
    # счёта мульти-ошибочных документов (раньше был SUM вхождений через window).
    assert "semd_totals AS" in query
    assert "SUM(docs) OVER (PARTITION BY semd)" not in query
    assert "semd_code" in query
    assert "COUNT(DISTINCT dwh_id)" in query
    assert "ORDER BY 3 DESC" in query
    assert "LIMIT" not in query
    assert card["metabase-field-filters"]["ips_date"] == {
        "table_ref": "public.rpt_error_breakdown",
        "field_name": "ips_date",
    }


def test_semd_volume_table_shows_share_of_total() -> None:
    dashboard = _integration_dashboard()
    card = next(c for c in _operational_tab_cards(dashboard) if c.get("name") == "Объём по СЭМД")
    assert card["display"] == "table"
    query = card["dataset_query"]["native"]["query"]
    assert "COUNT(DISTINCT dwh_id)" in query
    assert 'AS "%"' in query
    columns = {col["name"] for col in card["visualization_settings"]["table.columns"]}
    assert {"Код СЭМД", "Документов", "%"} <= columns


def test_semd_volume_uses_same_document_universe_as_total() -> None:
    dashboard = _integration_dashboard()
    top = next(
        card for card in _operational_tab_cards(dashboard)
        if card["name"] == "Объём по СЭМД"
    )
    top_query = top["dataset_query"]["native"]["query"]

    assert "semd_code" in top_query
    assert "rpt_documents" in top_query
    assert "COUNT(DISTINCT dwh_id)" in top_query


def test_transform_backfills_semd_code_from_transactions() -> None:
    sql = Path("db/parts/50_transform.sql").read_text(encoding="utf-8")
    assert "UPDATE public.documents d" in sql
    assert "FROM public.transactions t" in sql
    assert "NULLIF(btrim(d.semd_code), '') IS NULL" in sql
    assert "batch_docs AS" in sql
    # Отдельная O(архив)-функция backfill_semd_codes() удалена — backfill делает inline batch-блок.
    assert "CREATE OR REPLACE FUNCTION public.backfill_semd_codes()" not in sql


def test_document_metric_cards_count_distinct_dwh_id() -> None:
    document_views = (
        "rpt_documents",
        "rpt_network_errors",
        "rpt_error_breakdown",
        "rpt_documents",
    )
    allowed_count_star = {
        "01_integration_egisz.json": {"rpt_health_signals"},
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


def test_retired_error_interpretations_view_removed() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert "CREATE OR REPLACE VIEW public.v_rpt_error_interpretations_ui" not in sql
    assert "FROM public.rpt_documents" in sql


def test_archive_no_code_documents_are_qualified_by_status() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    dashboard = _integration_dashboard()
    card = next(card for card in _operational_tab_cards(dashboard) if card["name"] == "Объём по СЭМД")
    query = card["dataset_query"]["native"]["query"]

    assert '"СЭМД (архив)"' not in sql
    assert "semd_name" in sql
    assert "error_type" in sql
    assert "semd_code" in query
    assert "rpt_documents" in query
    assert "NULLIF(TRIM(semd_code" not in query


def test_document_views_use_document_grain_without_redundant_dedup() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")

    assert "ROW_NUMBER() OVER" not in sql
    assert "FROM public.documents d" in sql
    assert "FROM public.rpt_documents" in sql


def test_dashboards_do_not_expose_technical_dwh_id_fallbacks() -> None:
    payload = "\n".join(path.read_text(encoding="utf-8") for path in _dashboard_paths())
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")

    assert "document_group_key" not in payload
    assert "Ключ документа (группировка)" not in payload
    assert "egisz_document_identity_key" not in payload
    assert "MessageID; relatesToMessage" not in payload
    assert "PARTITION BY COALESCE(" not in sql
    assert "dwh_id" in sql


def test_only_recognized_documents_feed_non_queue_dashboards() -> None:
    transform_sql = Path("db/parts/50_transform.sql").read_text(encoding="utf-8")
    assert "NULLIF(btrim(tx.xml_local_uid), '') IS NOT NULL" in transform_sql
    assert "egisz_xml_text" not in transform_sql
    assert "pending_source AS" not in transform_sql

    quality = _legacy_dashboard("04_quality_and_errors.json")
    quality_queries = _native_queries(quality)
    assert any(
        "status IN ('success', 'async_error', 'network_error')" in q
        and "semd_code" in q
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

    assert any("rpt_error_breakdown" in query for query in queries)
    assert all("transactions" not in query for query in queries)
    assert all("\"Ошибки JSON raw\"" not in query for query in queries)


def test_document_volume_by_day_uses_first_sent_not_sent_at() -> None:
    dashboard = _integration_dashboard()
    card = next(
        c for c in dashboard["cards"]
        if c.get("name") == "Динамика документов по дням" and c.get("tab") == "archive"
    )
    query = card["dataset_query"]["native"]["query"]
    assert "first_sent_at" in Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert "first_sent_at" in query
    assert "FROM public.rpt_documents" in query
    assert "documents fd" not in query
    assert "COALESCE(fd.first_sent_at" not in query
    assert "processed_day AS" not in query
    assert " r " not in query
    assert card["metabase-field-filters"]["ips_date"]["field_name"] == "first_sent_at"


def test_quality_success_slices_sort_by_total_desc() -> None:
    dashboard = _integration_dashboard()
    clinic = next(c for c in dashboard["cards"] if c.get("name") == "Успешность по клиникам")
    semd = next(c for c in dashboard["cards"] if c.get("name") == "Успешность по типам СЭМД")
    assert "ORDER BY 2 DESC" in semd["dataset_query"]["native"]["query"]
    assert "ORDER BY 3 DESC" in clinic["dataset_query"]["native"]["query"]
    # Перенесены на вкладку «Оперативный мониторинг» (row 21).
    assert clinic["row"] == 21


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

    card = next(c for c in dashboard["cards"] if c.get("name") == "Топ категорий и типов ошибки")
    viz = card["visualization_settings"]
    query = card["dataset_query"]["native"]["query"]

    assert card["display"] == "row"
    assert card["sizeX"] >= 11
    assert "rpt_error_breakdown" in query
    assert "GROUP BY 1, 2" in query
    assert "LIMIT" not in query.upper()
    assert viz["graph.dimensions"] == ["Категория ошибки", "Тип ошибки"]
    assert viz["stackable.stack_type"] == "stacked"
    assert viz.get("graph.label_value_formatting") == "compact"
    assert viz.get("graph.y_axis.scale") == "linear"
    assert viz.get("graph.x_axis.title_text", "unset") == ""
    assert viz.get("graph.y_axis.title_text", "unset") == ""
    # Каждый тип окрашен в цвет своей категории (единая палитра, согласована с сунбёрстом).
    ss = viz["series_settings"]
    assert ss["Данные пациента не соответствуют ГИП"]["color"] == "#4E79A7"
    assert ss["Сетевая ошибка"]["color"] == "#499894"
    assert "click_behavior" not in card


def test_quality_semd_error_stacked_bar_hides_negligible_tail() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == "Топ типов СЭМД по видам ошибки")
    query = card["dataset_query"]["native"]["query"]

    assert card["display"] == "row"
    assert "semd_label" in query
    assert "rn <= 15" in query
    assert "rpt_error_breakdown" in query
    assert card["visualization_settings"]["graph.dimensions"] == ["СЭМД", "Тип ошибки"]
    assert card["visualization_settings"]["graph.metrics"] == ["Документов"]
    assert card["visualization_settings"]["stackable.stack_type"] == "stacked"
    assert card["visualization_settings"].get("graph.show_stack_values") == "total"
    assert card["visualization_settings"].get("graph.label_value_frequency") == "all"
    assert card["visualization_settings"].get("graph.x_axis.scale") == "ordinal"
    assert card["visualization_settings"].get("graph.x_axis.title_text", "unset") == ""
    assert card["visualization_settings"].get("graph.y_axis.title_text", "unset") == ""
    _assert_model_drill_through(card, "Разбивка ошибок", {"semd_label"})


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
    assert any("rpt_documents" in q for q in queries)
    assert all("rpt_documents_waiting" not in q for q in queries)

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
    assert any(p["name"] == "JID Клиники" for p in dashboard["parameters"])
    assert any(p["name"] == "Обработано IPS" and p.get("default") == "past7days~" for p in dashboard["parameters"])
    assert any(p["name"] == "Тип документа" for p in dashboard["parameters"])
    # Обзор/Документы читают rpt_documents, вкладка ошибок — rpt_error_breakdown (модель ошибок).
    assert all(
        "public.rpt_documents" in query or "public.rpt_error_breakdown" in query
        for query in queries
    )
    assert all("clinic_jid::text = {{client_jid}}" in query for query in queries)
    assert all("clinic_jid = {{client_jid}}" not in query for query in queries)
    dashboard = json.loads(Path("metabase_dashboards/08_client_bianalytic.json").read_text(encoding="utf-8"))
    queries = _native_queries(dashboard)

    assert dashboard["name"] == "Клиентский дашборд. BI-аналитика ЭМД"
    assert any(p["name"] == "JID Клиники" for p in dashboard["parameters"])
    assert all("public.rpt_documents" in query for query in queries)
    assert all("clinic_jid::text = {{client_jid}}" in query for query in queries)
    assert all("clinic_jid = {{client_jid}}" not in query for query in queries)
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
            # Field-filters периода/типа биндятся к source-витрине карточки: карточки ошибок
            # читают rpt_error_breakdown, остальные — rpt_documents (обе несут ips_date/semd_label).
            # Неиспользуемые в SQL фильтры карточки вычищены (prune), поэтому проверяем по наличию.
            query = card["dataset_query"]["native"]["query"]
            source_view = (
                "public.rpt_error_breakdown"
                if "public.rpt_error_breakdown" in query
                else "public.rpt_documents"
            )
            if "ips_date" in tags:
                assert tags["ips_date"]["type"] == "dimension"
                assert filters["ips_date"] == {
                    "table_ref": source_view,
                    "field_name": "ips_date",
                }
            if "client_document_type" in tags:
                assert tags["client_document_type"]["type"] == "dimension"
                assert filters["client_document_type"] == {
                    "table_ref": source_view,
                    "field_name": "semd_label",
                }


def test_client_service_dashboard_has_tabs_and_error_analytics() -> None:
    """07: три вкладки (Обзор/Ошибки регистрации ЭМД/Документы), аналитика ошибок и журнал."""
    dashboard = json.loads(Path("metabase_dashboards/07_client_service.json").read_text(encoding="utf-8"))
    tabs = dashboard.get("tabs") or []
    assert [t["name"] for t in tabs] == ["Обзор", "Ошибки регистрации ЭМД", "Документы"]

    names = {c.get("name") for c in dashboard["cards"]}
    # Мёртвая карточка среднего времени удалена: delivery_seconds почти всегда NULL.
    assert "Среднее время регистрации СЭМД" not in names
    # Разбор ошибок по категориям/типам + журнал документов.
    assert "Объёмы ошибок по категориям — клиент" in names
    assert "Топ типов ошибок — клиент" in names
    assert "Журнал документов — клиент" in names

    tab_ids = {t["id"] for t in tabs}
    for card in dashboard["cards"]:
        assert card.get("tab") in tab_ids, f"card {card.get('name', '(text)')!r} without valid tab"

    error_cards = [
        card for card in dashboard["cards"]
        if card.get("dataset_query", {}).get("type") == "native"
        and "public.rpt_error_breakdown" in card["dataset_query"]["native"]["query"]
    ]
    assert error_cards, "вкладка ошибок должна читать rpt_error_breakdown"
    assert all(card.get("tab") == "errors" for card in error_cards)


def test_client_dashboard_dwh_view_masks_patient_fields_and_exposes_hashes() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")

    assert "CREATE OR REPLACE VIEW public.rpt_documents" in sql
    assert "FROM public.rpt_documents" in sql
    assert "patient_name_masked" in sql
    assert "snils_masked" in sql
    assert "doctor_name" in sql
    # surrogate-ID для BI-дашборда: считать уникальных пациентов/врачей по hash без раскрытия ФИО/СНИЛС
    assert "patient_hash" in sql
    assert "doctor_hash" in sql
    assert "organization_oid" not in sql
    assert "clinic_oid" in sql
    assert "clinic_jid_mismatch" in sql
    assert "JID (EGISZ_LICENSES)" not in sql
    assert "Токен gost" not in sql


def test_all_pie_charts_have_v2_binding() -> None:
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for card in dashboard.get("cards", []):
            if card.get("display") != "pie":
                continue
            name = card.get("name", path.name)
            viz = card.get("visualization_settings", {})
            assert viz.get("pie.metric"), f"{path.name} / {name}: missing pie.metric"
            dims = viz.get("pie.dimension")
            assert dims, f"{path.name} / {name}: missing pie.dimension"
            assert "graph.metrics" not in viz, f"{path.name} / {name}: pie must not use graph.metrics"
            assert "graph.dimensions" not in viz, f"{path.name} / {name}: pie must not use graph.dimensions"
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


def test_card_names_are_unique_across_dashboard_files() -> None:
    """Metabase хранит одну карточку на имя в коллекции — имена не должны пересекаться между JSON."""
    by_name: dict[str, set[str]] = {}
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for card in dashboard.get("cards", []):
            if card.get("display") == "text" or not card.get("name"):
                continue
            by_name.setdefault(card["name"], set()).add(path.name)
    mismatches = [
        f"{name!r}: {sorted(files)}"
        for name, files in sorted(by_name.items())
        if len(files) > 1
    ]
    assert not mismatches, "Card names must be unique per dashboard file: " + "; ".join(mismatches)


def test_client_dashboards_use_client_document_type_filter_tag() -> None:
    for path_name in ("07_client_service.json", "08_client_bianalytic.json"):
        dashboard = json.loads(Path(f"metabase_dashboards/{path_name}").read_text(encoding="utf-8"))
        for card in dashboard.get("cards", []):
            dq = card.get("dataset_query", {})
            if dq.get("type") != "native":
                continue
            query = dq["native"]["query"]
            tags = dq["native"]["template-tags"]
            assert "client_semd_code_name" not in query, f"{path_name}/{card['name']}: stale filter tag"
            if "[[AND {{client_document_type}}]]" in query:
                assert "client_document_type" in tags, card["name"]


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
    client_dashboards = {"07_client_service.json", "08_client_bianalytic.json"}
    for name, instances in by_name.items():
        if len(instances) < 2:
            continue
        paths = {path_name for path_name, _ in instances}
        if not (paths <= client_dashboards or paths.isdisjoint(client_dashboards)):
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


def test_native_dimension_filters_have_field_filter_bindings() -> None:
    """Импорт Metabase: dimension template-tags должны иметь metabase-field-filters для field-mapping."""
    violations: list[str] = []
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for card in dashboard.get("cards", []):
            if card.get("display") == "text":
                continue
            dq = card.get("dataset_query") or {}
            if dq.get("type") != "native":
                continue
            tags = (dq.get("native") or {}).get("template-tags") or {}
            filters = card.get("metabase-field-filters") or {}
            for tag_name, tag in tags.items():
                if tag.get("type") != "dimension":
                    continue
                binding = filters.get(tag_name) or {}
                if binding.get("model_ref"):
                    continue
                if not binding.get("table_ref") or not binding.get("field_name"):
                    violations.append(f"{path.name} / {card.get('name', '?')} / {tag_name}")
    assert not violations, (
        "Native dimension filters need metabase-field-filters for dashboard import: "
        + "; ".join(violations)
    )


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


ERROR_PERIOD_CARD = "Ошибки: тип × клиника"


def test_operational_error_period_card_uses_atomic_error_types() -> None:
    """Карточка error period — rpt_error_breakdown, без «Сводки ошибки»."""
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == ERROR_PERIOD_CARD)
    query = card["dataset_query"]["native"]["query"]
    assert card["dataset_query"]["type"] == "native"
    assert "rpt_error_breakdown" in query
    assert "error_type" in query
    assert "clinic_label" in query
    cols = {c["name"] for c in card["visualization_settings"].get("table.columns", [])}
    assert "Сводка ошибки" not in cols
    assert "Паттерн ошибки" in cols
    assert "% ошибок" in cols


def test_error_period_card_groups_by_error_type_and_clinic() -> None:
    """По умолчанию — группировка тип ошибки × клиника, метрика distinct по документу."""
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == ERROR_PERIOD_CARD)
    query = card["dataset_query"]["native"]["query"]
    assert "COUNT(DISTINCT dwh_id)" in query
    assert "period_docs" in query
    assert query.count("[[AND {{ips_date}}]]") == 1
    assert 'AS "Тип ошибки"' in query
    assert 'AS "Клиника"' in query
    assert 'AS "JID Клиники"' in query
    assert 'AS "Паттерн ошибки"' in query
    assert 'AS "% ошибок"' in query
    # Паттерн ошибки = match_code + match_pattern из dim_error_rules.
    assert "rule_patterns" in query
    assert "dim_error_rules" in query
    assert "match_code" in query
    assert "match_pattern" in query


def test_error_period_card_uses_breakdown_model() -> None:
    """Карточка читает rpt_error_breakdown через period_docs из rpt_documents."""
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == ERROR_PERIOD_CARD)
    query = card["dataset_query"]["native"]["query"]
    assert card["dataset_query"]["type"] == "native"
    assert "rpt_error_breakdown" in query
    assert "rpt_documents" in query
    assert card.get("metabase-field-filters")["ips_date"] == {
        "table_ref": "public.rpt_documents",
        "field_name": "ips_date",
    }


def test_error_period_card_uses_canonical_filter_tags() -> None:
    """Карточка error period — field filters и drill в модель «Документы» (contains)."""
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == ERROR_PERIOD_CARD)
    filters = card.get("metabase-field-filters") or {}
    assert filters["jid"]["field_name"] == "clinic_label"
    assert filters["jid"]["table_ref"] == "public.rpt_documents"
    assert filters["semd_type"]["field_name"] == "semd_label"
    assert filters["error_type"]["field_name"] == "error_type"
    assert filters["error_type"]["table_ref"] == "public.rpt_error_breakdown"
    query = card["dataset_query"]["native"]["query"]
    assert "[[AND {{error_type}}]]" in query
    param_slugs = {p["slug"] for p in dashboard["parameters"]}
    assert "jid_filter" in param_slugs
    assert "semd_type_filter" in param_slugs
    assert "error_type_filter" in param_slugs
    _assert_model_drill_through(
        card,
        "Документы",
        {"error_types", "clinic_jid"},
        contains_fields={"error_types"},
    )
    drill_params = card.get("metabase-model-drill-params") or {}
    assert drill_params.get("ips_date") == "ips_date"
    assert drill_params.get("semd_type") == "semd_label"
    assert drill_params.get("jid") == "clinic_label"


def test_pie_cards_do_not_keep_graph_dimensions() -> None:
    dashboard = _integration_dashboard()
    for card in dashboard["cards"]:
        if card.get("display") != "pie":
            continue
        viz = card.get("visualization_settings") or {}
        assert "graph.dimensions" not in viz, card.get("name")
        assert "graph.metrics" not in viz, card.get("name")


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
    card = next(c for c in dashboard["cards"] if c.get("name") == "Топ категорий и типов ошибки")
    assert card["display"] == "row"


def test_top_error_type_card_is_table_with_share() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == "Топ по типу ошибки")
    query = card["dataset_query"]["native"]["query"]
    viz = card["visualization_settings"]

    assert card["display"] == "table"
    assert "error_category" in query
    assert '"Тип ошибки"' in query
    # «%» — доля документов с этим типом от всех документов с ошибками.
    assert 'AS "%"' in query
    assert "NULLIF((SELECT total FROM totals), 0)" in query
    columns = [col["name"] for col in viz["table.columns"]]
    assert columns == ["Категория ошибки", "Тип ошибки", "Документов", "%"]


def test_top_semd_by_errors_uses_semd_label() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == "Топ типов СЭМД по ошибкам")
    query = card["dataset_query"]["native"]["query"]
    assert "semd_label" in query
    assert 'AS "СЭМД"' in query
    assert "semd_code_name" not in query


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
    assert "semd_local_uid" in sql
    assert "CREATE OR REPLACE VIEW public.rpt_network_errors" in sql


def test_no_response_view_exposes_wait_segments() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    dashboard = _legacy_dashboard("03_documents_no_response.json")

    assert '>30 дней' in sql
    assert '>7 дней' in sql
    assert '>3 дней' in sql
    assert 'до 3 дней' in sql
    assert "wait_segment" in sql
    assert "waiting_days" in sql

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


def test_executive_mrr_queries_do_not_compare_jid_to_empty_string() -> None:
    dashboard = json.loads(Path("metabase_dashboards/05_executive.json").read_text(encoding="utf-8"))
    for card in dashboard["cards"]:
        dq = card.get("dataset_query", {})
        if dq.get("type") != "native":
            continue
        query = dq["native"]["query"]
        assert "NULLIF(clinic_jid, '')" not in query, card.get("name")
    executive = json.loads(Path("metabase_dashboards/05_executive.json").read_text(encoding="utf-8"))
    clinic_card = next(c for c in executive["cards"] if c.get("name") == "Клиник без единого успеха")
    clinic_fmt = clinic_card["visualization_settings"]["column_settings"]['["name","Клиник без успеха"]']
    assert clinic_fmt["decimals"] == 0
    assert "suffix" not in clinic_fmt
    assert clinic_fmt["number_separators"] == ", "

    client = json.loads(Path("metabase_dashboards/07_client_service.json").read_text(encoding="utf-8"))
    by_name = {c.get("name"): c for c in client["cards"]}
    # «Среднее время регистрации СЭМД» удалена: delivery_seconds почти всегда NULL.
    for text_scalar in (
        "Успешно зарегистрирован",
        "Ошибка асинхронного ответа РЭМД",
        "Ошибка связи",
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
    assert "06_semd_archive.json" not in names


def test_integration_dashboard_has_tabs_and_legacy_card_coverage() -> None:
    dashboard = _integration_dashboard()
    assert dashboard["name"] == "Интеграция с ЕГИСЗ"
    assert dashboard.get("width") == "full"
    tabs = dashboard.get("tabs") or []
    assert len(tabs) == 5
    assert [tab["name"] for tab in tabs] == [
        "Оперативный мониторинг",
        "Сервис интеграции",
        "Отправленные",
        "Анализ ошибок",
        "Архив СЭМД",
    ]
    by_tab: dict[str, int] = {}
    for card in dashboard["cards"]:
        tab = card.get("tab")
        assert tab, f"card {card.get('name', '?')} missing tab"
        by_tab[tab] = by_tab.get(tab, 0) + 1
    assert by_tab["operational"] == 9
    assert by_tab["service"] == 11
    assert by_tab["queue"] == 8
    assert by_tab["errors"] == 7
    assert by_tab["archive"] == 5


def test_integration_dashboard_default_period_is_current_month() -> None:
    dashboard = _integration_dashboard()
    period = next(p for p in dashboard["parameters"] if p.get("slug") == "ips_date_filter")
    assert period.get("default") == "thismonth"
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
        "Отправлено",
    }
    for tab in ("errors", "service"):
        names = {c.get("name") for c in dashboard["cards"] if c.get("tab") == tab}
        assert not names & removed, f"{tab} still has scalar KPI cards: {names & removed}"


def test_archive_tab_layout_matches_grid() -> None:
    dashboard = _integration_dashboard()
    expected = {
        "Всего документов": (0, 0, 4, 2),
        "Всего клиник": (0, 4, 4, 2),
        "Динамика документов по дням": (0, 8, 16, 8),
        "Объём по клиникам": (2, 0, 8, 6),
        "Архив СЭМД": (8, 0, 24, 10),
    }
    for name, (row, col, size_x, size_y) in expected.items():
        card = next(
            c for c in dashboard["cards"]
            if c.get("tab") == "archive" and c.get("name") == name
        )
        assert card["row"] == row, name
        assert card["col"] == col, name
        assert card["sizeX"] == size_x, name
        assert card["sizeY"] == size_y, name


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
    assert archive["row"] == 2 and archive["col"] == 0 and archive["sizeX"] == 8 and archive["sizeY"] == 6


def test_archive_tab_has_five_cards() -> None:
    archive_names = {c["name"] for c in _archive_tab_cards()}
    assert archive_names == {
        "Всего документов",
        "Всего клиник",
        "Динамика документов по дням",
        "Архив СЭМД",
        "Объём по клиникам",
    }


def test_errors_tab_has_error_slice_cards() -> None:
    dashboard = _legacy_dashboard("04_quality_and_errors.json")
    names = {c.get("name") for c in dashboard["cards"] if c.get("display") != "text"}
    for required in (
        "Топ по типу ошибки",
        "Топ категорий и типов ошибки",
        "Топ типов СЭМД по ошибкам",
        "Ошибки: тип × клиника",
    ):
        assert required in names


def test_operational_tab_has_core_cards() -> None:
    dashboard = _integration_dashboard()
    names = {c.get("name") for c in dashboard["cards"] if c.get("tab") == "operational" and c.get("display") != "text"}
    assert names == {
        "Последние операции",
        "Статусы за период",
        "Транзакции по дням и статусам",
        "Объём по клиникам",
        "Объём по СЭМД",
        "Успешность по клиникам",
        "Успешность по типам СЭМД",
        "Объём ошибок по клиникам",
        "Тепловая карта: клиника × день",
    }


def test_operational_tab_has_no_scalar_kpi_row() -> None:
    dashboard = _integration_dashboard()
    names = {c.get("name") for c in dashboard["cards"] if c.get("tab") == "operational"}
    assert not names & {"Всего документов", "Всего клиник", "Отправлено"}
    scalars = [
        c for c in dashboard["cards"]
        if c.get("tab") == "operational" and c.get("display") == "scalar"
    ]
    assert not scalars


def test_metabase_models_catalog_exists() -> None:
    models = sorted(Path("metabase_models").glob("*.json"))
    assert [p.name for p in models] == [
        "01_documents.json",
        "02_error_breakdown.json",
        "03_no_response.json",
        "04_network_errors.json",
    ]
    documents = json.loads(Path("metabase_models/01_documents.json").read_text(encoding="utf-8"))
    assert documents["table_ref"] == "public.rpt_documents"
    assert documents["name"] == "Документы"
    assert "status_label" in documents["fields"]
    assert "semd_name" in documents["fields"]
    assert "delivery_seconds" in documents["fields"]
    assert "status" in documents["hidden_fields"]
    no_response = json.loads(Path("metabase_models/03_no_response.json").read_text(encoding="utf-8"))
    assert no_response["name"] == "Очередь без ответа"
    assert "wait_segment" in no_response["fields"]
    network_errors = json.loads(Path("metabase_models/04_network_errors.json").read_text(encoding="utf-8"))
    assert network_errors["name"] == "Сбои транспорта"


def test_retired_qb_support_views_removed_from_dwh_sql() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    for retired in (
        "v_rpt_clinic_semd_slice_ui",
        "v_rpt_client_kpi_daily_ui",
        "v_rpt_client_documents_ui",
        "v_rpt_error_interpretations_ui",
    ):
        assert f"CREATE OR REPLACE VIEW public.{retired}" not in sql
    assert "CREATE OR REPLACE VIEW public.rpt_documents" in sql
    assert "CREATE MATERIALIZED VIEW public.rpt_error_breakdown" in sql


def test_quality_qb_cards_use_models_and_archive_click() -> None:
    dashboard = _legacy_dashboard("01_operational.json")
    card = next(c for c in dashboard["cards"] if c.get("name") == "Объём по клиникам")
    query = card["dataset_query"]["native"]["query"]
    assert card["dataset_query"]["type"] == "native"
    assert 'AS "%"' in query
    _assert_model_drill_through(card, "Документы", {"clinic_jid"})


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


def test_dashboard_emdr_filter_uses_canonical_column_name() -> None:
    dashboard = json.loads(INTEGRATION_DASHBOARD.read_text(encoding="utf-8"))
    emdr_param = next(p for p in dashboard["parameters"] if p.get("slug") == "emdr_id_filter")
    assert emdr_param["name"] == "Рег. Номер РЭМД"
    assert "(emdrid)" not in emdr_param["name"]
    relates_param = next(p for p in dashboard["parameters"] if p.get("slug") == "relates_to_filter")
    assert relates_param["name"] == "Связанное сообщение"
    for card in dashboard.get("cards", []):
        filters = card.get("metabase-field-filters") or {}
        if "emdr_id" in filters:
            assert filters["emdr_id"]["field_name"] == "semd_emdr_id"


def test_archive_detail_uses_documents_model() -> None:
    dashboard = _integration_dashboard()
    card = next(c for c in dashboard["cards"] if c.get("name") == "Архив СЭМД" and c.get("tab") == "archive")
    assert card.get("query_tier") == "query_builder"
    assert card.get("source_model") == "Документы"
    assert card["dataset_query"]["query"]["source-table"] == "model:Документы"
    columns = {col["name"]: col.get("enabled", True) for col in card["visualization_settings"]["table.columns"]}
    assert columns.get("dwh_id") is False
    assert columns.get("localUid СЭМД") is True
    assert columns.get("Клиника") is True
    assert columns.get("JID Клиники") is False
    column_settings = card["visualization_settings"].get("column_settings") or {}
    assert not any("(emdrid)" in str(v) for v in column_settings.values())


def test_service_quality_has_jid_mismatch_check() -> None:
    dashboard = json.loads(INTEGRATION_DASHBOARD.read_text(encoding="utf-8"))
    card = next(c for c in dashboard["cards"] if c.get("name") == "Контроль качества данных")
    query = card["dataset_query"]["native"]["query"]
    assert "Расхождение OID и JID" in query
    assert "clinic_jid_mismatch = true" in query
    assert '"Расхождение источников JID" = \'да\'' not in query


def test_integration_native_sql_uses_real_column_names() -> None:
    dashboard = json.loads(INTEGRATION_DASHBOARD.read_text(encoding="utf-8"))
    by_name = {c.get("name"): c for c in dashboard["cards"]}

    queue_sql = by_name["Очередь без ответа"]["dataset_query"]["native"]["query"]
    assert 'sent_at AS "Дата отправки"' in queue_sql
    assert 'waiting_days AS "Дней в ожидании"' in queue_sql
    assert 'wait_segment AS "Сегмент ожидания"' in queue_sql
    assert ', "Дата отправки"' not in queue_sql

    network_sql = by_name["Последние сбои транспорта"]["dataset_query"]["native"]["query"]
    assert "LEFT(error_text, 140)" in network_sql
    assert '"Текст сетевой ошибки"' not in network_sql

    detail_sql = by_name["Детализация контроля качества"]["dataset_query"]["native"]["query"]
    assert 'clinic_jid_mismatch AS "Расхождение источников JID"' in detail_sql
    assert "FROM public.rpt_documents r" not in detail_sql
    assert "rpt_document_lineage" in detail_sql
    assert '"OID из XML"' in detail_sql
    assert '"OID из JPERSONS"' in detail_sql
    assert '"OID из лицензий"' in detail_sql

    for card_name in (
        "Зависших >3 дней",
        "Зависших >7 дней",
        "Зависших >30 дней",
        "В очереди (всего)",
        "Очередь без ответа",
        "Топ клиник в очереди по документам",
        "Сегменты ожидания",
        "Топ типов СЭМД в очереди",
    ):
        filters = by_name[card_name].get("metabase-field-filters") or {}
        assert filters.get("wait_segment", {}).get("field_name") == "wait_segment", card_name


def test_service_quality_detail_lists_all_rule_violations() -> None:
    dashboard = _legacy_dashboard("02_service.json")
    by_name = {c.get("name"): c for c in dashboard["cards"]}
    summary = by_name["Контроль качества данных"]
    detail = by_name["Детализация контроля качества"]
    assert detail["row"] == summary["row"] + summary["sizeY"]
    assert detail["sizeX"] == 24
    query = detail["dataset_query"]["native"]["query"]
    assert '"Нарушения"' in query
    assert "расхождение OID/JID" in query
    assert "LIMIT 1000" in query
    enabled = {
        col["name"]
        for col in detail["visualization_settings"]["table.columns"]
        if col.get("enabled", True)
    }
    assert "Нарушения" in enabled
    assert "OID из XML" in enabled
    assert "JID Клиники" in enabled
    formatting = detail["visualization_settings"].get("table.column_formatting") or []
    violation_rules = [
        rule
        for rule in formatting
        if "Нарушения" in (rule.get("columns") or [])
    ]
    assert violation_rules, "expected conditional formatting on Нарушения column"
    assert violation_rules[0].get("operator") == "!="


def test_document_attributes_table_has_no_legacy_labels() -> None:
    core_sql = Path("db/parts/70_views_core.sql").read_text(encoding="utf-8")
    for legacy_name in (
        "Идентификатор документа (localUid)",
        "JID из журнала (gost, число)",
        "JID из gost в REPLYTO",
        "JID (EGISZ_LICENSES)",
        "Токен gost (REPLYTO)",
        "Токен gost (нецифр., для отображения)",
        "Медицинская организация",
        "Регистрационный номер РЭМД",
        "Рег. Номер РЭМД (emdrid)",
    ):
        assert legacy_name not in core_sql
    assert "'нет'::text AS \"Расхождение источников JID\"" not in core_sql


def test_connectivity_view_no_stale_jid_coalesce() -> None:
    rpt_sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert "JID из журнала" not in rpt_sql
    assert "JID Клиники (ключ)" not in rpt_sql
    assert "Ответы РЭМД: успех (документов)" not in rpt_sql


def test_clinic_error_volume_chart_uses_clinic_name_not_jid_label() -> None:
    dashboard = _integration_dashboard()
    card = next(c for c in dashboard["cards"] if c.get("name") == "Объём ошибок по клиникам")
    query = card["dataset_query"]["native"]["query"]
    assert "clinic_name" in query.split("per_clinic")[1].split("bounds")[0]
    assert "Прочие (" in query
    assert "SUM(r.errs)" in query
    assert 'ROUND(100.0 * t.errs / NULLIF(t.total, 0)' in query
    assert card["display"] == "combo"
    assert card["visualization_settings"]["series_settings"]["Документов"]["display"] == "line"
    assert card["visualization_settings"]["graph.dimensions"] == ["Клиника"]
    assert card["visualization_settings"].get("graph.max_categories") == 20
    # Перенесён на вкладку «Оперативный мониторинг» (полная ширина).
    assert card["tab"] == "operational" and card["col"] == 0 and card["sizeX"] == 24


def test_success_slice_tables_have_default_column_widths() -> None:
    dashboard = _integration_dashboard()
    clinic = next(c for c in dashboard["cards"] if c.get("name") == "Успешность по клиникам")
    semd = next(c for c in dashboard["cards"] if c.get("name") == "Успешность по типам СЭМД")
    assert clinic["visualization_settings"]["table.column_widths"] == [88, 300, 88, 88]
    assert semd["visualization_settings"]["table.column_widths"] == [88, 120, 88, 88, 88]


def test_archive_documents_model_drops_error_type_filter() -> None:
    """Архив (грейн документа) больше не фильтруется по типу ошибки: модель «Документы»
    не имеет колонки error_type, отбор по типу идёт на грейне «Разбивка ошибок»."""
    dashboard = _integration_dashboard()
    card = next(c for c in dashboard["cards"] if c.get("name") == "Архив СЭМД" and c.get("tab") == "archive")
    targets = card.get("metabase-parameter-targets") or {}
    assert "error_type" not in targets
    assert "error_types" not in targets


def test_clinic_volume_sql_uses_clinic_label_in_filtered_cte() -> None:
    dashboard = _integration_dashboard()
    card = next(
        c for c in dashboard["cards"]
        if c.get("name") == "Объём по клиникам" and c.get("tab") == "operational"
    )
    query = card["dataset_query"]["native"]["query"]
    assert "clinic_jid, clinic_label, dwh_id" in query
    assert 'SELECT "JID Клиники", "Клиника"' in query
    assert "clinic_name" not in query.split("per_clinic")[0]
    enabled = {
        col["name"]
        for col in card["visualization_settings"]["table.columns"]
        if col.get("enabled", True)
    }
    assert "Клиника" in enabled
    assert "Наименование клиники" not in enabled


def _card_rect(card: dict) -> tuple[int, int, int, int]:
    return (
        int(card.get("row", 0)),
        int(card.get("col", 0)),
        int(card.get("sizeX", 1)),
        int(card.get("sizeY", 1)),
    )


def _rects_overlap(a: tuple[int, int, int, int], b: tuple[int, int, int, int]) -> bool:
    ar, ac, aw, ah = a
    br, bc, bw, bh = b
    return not (ar + ah <= br or br + bh <= ar or ac + aw <= bc or bc + bw <= ac)


def test_integration_dashboard_cards_do_not_overlap_per_tab() -> None:
    dashboard = _integration_dashboard()
    by_tab: dict[str, list[tuple[str, tuple[int, int, int, int]]]] = {}
    for card in dashboard["cards"]:
        tab = card.get("tab") or "_root"
        by_tab.setdefault(tab, []).append((card.get("name", "?"), _card_rect(card)))
    overlaps: list[str] = []
    for tab, rects in by_tab.items():
        for i, (name_a, rect_a) in enumerate(rects):
            for name_b, rect_b in rects[i + 1 :]:
                if _rects_overlap(rect_a, rect_b):
                    overlaps.append(f"{tab}: {name_a} vs {name_b}")
    assert not overlaps, "Overlapping cards: " + "; ".join(overlaps)


def test_native_card_columns_match_visualization() -> None:
    dashboard = _integration_dashboard()
    violations: list[str] = []
    for card in dashboard["cards"]:
        dq = card.get("dataset_query", {})
        if dq.get("type") != "native":
            continue
        query = dq.get("native", {}).get("query", "")
        viz = card.get("visualization_settings") or {}
        columns = [
            col["name"]
            for col in viz.get("table.columns", [])
            if col.get("enabled", True)
        ]
        if not columns:
            continue
        for col in columns:
            if f'AS "{col}"' not in query and f'"{col}"' not in query:
                violations.append(f"{card.get('name')}: missing {col!r}")
    assert not violations, "; ".join(violations)


def test_click_behavior_source_columns_exist_in_sql() -> None:
    dashboard = _integration_dashboard()
    violations: list[str] = []
    for card in dashboard["cards"]:
        behavior = card.get("click_behavior") or {}
        if behavior.get("type") != "link":
            continue
        if behavior.get("linkType") == "question":
            continue
        mapping = behavior.get("parameterMapping") or {}
        if dq := card.get("dataset_query", {}):
            query = (
                dq.get("native", {}).get("query", "")
                if dq.get("type") == "native"
                else ""
            )
        else:
            query = ""
        if not query:
            continue
        for _slug, spec in mapping.items():
            source = spec.get("source") or {}
            col = source.get("name")
            if not col:
                continue
            if f'AS "{col}"' not in query and f'"{col}"' not in query:
                violations.append(f"{card.get('name')}: drill column {col!r}")
    assert not violations, "; ".join(violations)


# --- Единая модель ips_date + конфайн фильтров + label-поиск + drill в модель ----------

_LOOKUP_FILTERS = {"local_uid", "relates_to", "emdr_id", "log_id"}
_COMMON_FILTERS = {"ips_date", "jid", "semd_type"}


def _card_filter_keys(card: dict) -> set[str]:
    return set((card.get("metabase-field-filters") or {})) | set(
        (card.get("metabase-parameter-targets") or {})
    )


def test_no_legacy_date_tokens_anywhere() -> None:
    """ips_date — единственное имя бизнес-даты: ни dwh_date/processed_at/processed_day/arrival_day,
    ни двойного сдвига AT TIME ZONE в документных rpt-витринах, ни в дашбордах."""
    for path in _dashboard_paths():
        blob = path.read_text(encoding="utf-8")
        for tok in ("dwh_date", "processed_at", "processed_day", "arrival_day", "mgmt_period", "client_period"):
            assert tok not in blob, f"{path.name}: stale date token {tok!r}"
    views = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert "COALESCE(d.last_callback_at, d.registered_at, d.first_sent_at) AS ips_date" in views
    assert "AT TIME ZONE 'Europe/Moscow'" not in views  # день берём из полной даты, стек МСК-pinned
    assert "AS processed_at" not in views and "AS processed_day" not in views
    tables = Path("db/parts/10_tables.sql").read_text(encoding="utf-8")
    assert "DROP COLUMN sent_at" in Path("db/parts/60_drop_dependents.sql").read_text(encoding="utf-8")
    assert "loaded_at timestamptz DEFAULT now()" in tables  # transactions.processed_at → loaded_at


def test_common_filters_wired_on_every_tab() -> None:
    """Период/JID/Код СЭМД — общие сквозные фильтры: привязаны ≥1 карточкой на каждой вкладке 01."""
    dash = _integration_dashboard()
    tabs = {t["id"] for t in dash.get("tabs", [])}
    by_tab: dict[str, set[str]] = {t: set() for t in tabs}
    for card in dash["cards"]:
        if card.get("display") == "text":
            continue
        by_tab.setdefault(card.get("tab", ""), set()).update(_card_filter_keys(card))
    for tab in tabs:
        assert _COMMON_FILTERS <= by_tab[tab], f"tab {tab}: missing {_COMMON_FILTERS - by_tab[tab]}"


def test_lookup_filters_confined_to_archive() -> None:
    """Document-lookup фильтры конфайнятся: агрегаты op/service/errors их не несут; на queue
    допустим только localUid (поиск зависшего документа), полный набор — на archive."""
    dash = _integration_dashboard()
    for card in dash["cards"]:
        if card.get("display") == "text" or card.get("tab") == "archive":
            continue
        # queue — тоже lookup-контекст: localUid там уместен, остальные lookup-фильтры нет.
        forbidden = _LOOKUP_FILTERS - {"local_uid"} if card.get("tab") == "queue" else _LOOKUP_FILTERS
        leaked = _card_filter_keys(card) & forbidden
        assert not leaked, f"{card.get('name')} (tab={card.get('tab')}): lookup filters leaked {leaked}"
    archive_keys: set[str] = set()
    for card in dash["cards"]:
        if card.get("tab") == "archive" and card.get("display") != "text":
            archive_keys |= _card_filter_keys(card)
    assert _LOOKUP_FILTERS <= archive_keys, f"archive missing lookup filters {_LOOKUP_FILTERS - archive_keys}"


def test_jid_semd_status_bound_to_label_columns() -> None:
    """Поиск по «код или наименование»: JID/СЭМД/Статус привязаны к label-колонкам во всех дашбордах."""
    label_field = {"jid": "clinic_label", "semd_type": "semd_label", "status": "status_label"}
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for card in dashboard["cards"]:
            bindings = {
                **(card.get("metabase-field-filters") or {}),
                **(card.get("metabase-parameter-targets") or {}),
            }
            for key, expected in label_field.items():
                if key in bindings and "field_name" in bindings[key]:
                    assert bindings[key]["field_name"] == expected, (
                        f"{path.name}/{card.get('name')}: {key} -> {bindings[key]['field_name']} (ждали {expected})"
                    )


def test_grain_cards_drill_into_model_not_archive() -> None:
    """Дрилл из агрегатов идёт в модель (linkType=question), а не на вкладку «Архив»."""
    dash = _integration_dashboard()
    by_name = {c.get("name"): c for c in dash["cards"]}
    for name in ("Объём по клиникам", "Объём по СЭМД", "Топ типов СЭМД по ошибкам", "Топ клиник в очереди по документам"):
        click = by_name[name].get("click_behavior") or {}
        assert click.get("linkType") == "question", f"{name}: ждали drill в модель"
        assert "targetDashboard" not in click and click.get("tab") != "archive"
