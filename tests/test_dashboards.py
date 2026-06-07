from __future__ import annotations

import json
from pathlib import Path


def _dashboard_paths() -> list[Path]:
    return sorted(Path("metabase_dashboards").glob("*.json"))


def _native_queries(dashboard: dict) -> list[str]:
    return [
        card["dataset_query"]["native"]["query"]
        for card in dashboard["cards"]
        if card.get("dataset_query", {}).get("type") == "native"
    ]


def test_all_dashboards_default_to_full_width() -> None:
    dashboards = _dashboard_paths()
    assert dashboards, "Expected dashboard JSON files in metabase_dashboards/"

    for path in dashboards:
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload.get("width") == "full", f"{path.name} must default to full width"


def test_service_network_top_groups_by_typed_label() -> None:
    dashboard = json.loads(Path("metabase_dashboards/02_service.json").read_text(encoding="utf-8"))
    card = next(c for c in dashboard["cards"] if c["name"] == "02 · Топ сетевых формулировок")
    query = card["dataset_query"]["native"]["query"]
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")

    assert '"Тип сетевой ошибки"' in query
    assert 'public.egisz_network_error_type(d.error_text) AS "Тип сетевой ошибки"' in sql
    assert "GROUP BY 1" in query


def test_quality_network_pie_groups_by_typed_label() -> None:
    dashboard = json.loads(Path("metabase_dashboards/04_quality_and_errors.json").read_text(encoding="utf-8"))
    card = next(
        c for c in dashboard["cards"]
        if c["name"] == "04 · Ошибки связи: доля по формулировке LOGSTATE=3"
    )
    query = card["dataset_query"]["native"]["query"]

    assert '"Тип сетевой ошибки"' in query
    assert "per_kind AS" in query


def test_operational_error_types_include_network_slice() -> None:
    dashboard = json.loads(Path("metabase_dashboards/01_operational.json").read_text(encoding="utf-8"))
    card = next(card for card in dashboard["cards"] if card["name"] == "Ошибки по типу")
    query = card["dataset_query"]["native"]["query"]
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")

    assert "public.v_rpt_error_category_breakdown_ui" in query
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
    dashboard = json.loads(Path("metabase_dashboards/01_operational.json").read_text(encoding="utf-8"))
    card = next(card for card in dashboard["cards"] if card["name"] == "Последние операции")
    view_columns = _view_column_names("v_rpt_documents_ui")
    configured_columns = {
        column["name"]
        for column in card["visualization_settings"]["table.columns"]
        if column.get("enabled", True)
    }

    assert configured_columns.issubset(view_columns), sorted(configured_columns - view_columns)
    assert "Дата обработки" in configured_columns
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
    dashboard = json.loads(Path("metabase_dashboards/02_service.json").read_text(encoding="utf-8"))
    for name in ("Сетевые ошибки (тренд)", "Ошибки регистрации в РЭМД ЕГИСЗ (тренд)"):
        card = next(c for c in dashboard["cards"] if c["name"] == name)
        query = card["dataset_query"]["native"]["query"]
        assert "date_trunc('hour', \"Дата обработки\")" in query
        assert "[[AND {{dwh_date}}]]" in query
        assert card["visualization_settings"]["graph.dimensions"] == ["Час"]


def test_service_async_vs_network_pie_uses_canonical_status_colors() -> None:
    dashboard = json.loads(Path("metabase_dashboards/02_service.json").read_text(encoding="utf-8"))
    card = next(c for c in dashboard["cards"] if c["name"] == "async vs network")
    query = card["dataset_query"]["native"]["query"]
    assert 'SELECT "Статус"' in query
    assert '"Статус" AS "Тип"' not in query
    rows = card["visualization_settings"]["pie.rows"]
    colors = {row["key"]: row["color"] for row in rows}
    assert colors["Ошибка асинхронного ответа РЭМД"] == "#A989C5"
    assert colors["Ошибка связи"] == "#F2994A"
    assert card["visualization_settings"]["pie.dimension"] == ["Статус"]


def test_service_healthcheck_pie_matches_signals_table_scope() -> None:
    dashboard = json.loads(Path("metabase_dashboards/02_service.json").read_text(encoding="utf-8"))
    pie = next(c for c in dashboard["cards"] if c["name"] == "Healthcheck")
    table = next(c for c in dashboard["cards"] if c["name"] == "02 · Сигналы healthcheck")
    scope = "\"Код сигнала\" NOT IN ('queue_24h', 'pending_backlog_24h')"
    assert scope in pie["dataset_query"]["native"]["query"]
    assert scope in table["dataset_query"]["native"]["query"]


def test_operational_status_breakdown_uses_four_canonical_statuses() -> None:
    dashboard = json.loads(Path("metabase_dashboards/01_operational.json").read_text(encoding="utf-8"))
    latest_card = next(card for card in dashboard["cards"] if card["name"] == "Последние операции")
    card = next(card for card in dashboard["cards"] if card["name"] == "Статусы за период")
    trend_card = next(card for card in dashboard["cards"] if card["name"] == "01 · Транзакции по дням и статусам")
    latest_query = latest_card["dataset_query"]["native"]["query"]
    query = card["dataset_query"]["native"]["query"]
    trend_query = trend_card["dataset_query"]["native"]["query"]
    rows = card["visualization_settings"]["pie.rows"]
    row_keys = {row["key"] for row in rows}

    assert "public.v_rpt_documents_ui" in query
    assert "public.v_rpt_documents_ui" in latest_query
    assert "public.v_egisz_transactions_enriched_ui" not in latest_query
    assert latest_card["metabase-field-filters"]["dwh_date"] == {
        "table_ref": "public.v_rpt_documents_ui",
        "field_name": "Дата обработки",
    }
    # Единый универсум: статусный пирог не отсекает «В обработке», слайсы суммируются
    # к общему числу документов; нотификация берётся из канонической колонки «Статус».
    assert "WHERE \"Статус\" IN ('success', 'error')" not in query
    assert "WHERE 1=1" in query
    assert "SELECT \"Статус\", COUNT(DISTINCT \"Документ (ключ учёта)\")::bigint" in query
    assert "COUNT(DISTINCT \"Документ (ключ учёта)\")::bigint" in query
    assert card["metabase-field-filters"]["dwh_date"] == {
        "table_ref": "public.v_rpt_documents_ui",
        "field_name": "Дата обработки",
    }
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
    dashboard = json.loads(Path("metabase_dashboards/04_quality_and_errors.json").read_text(encoding="utf-8"))
    queries = _native_queries(dashboard)
    assert all("v_egisz_documents_enriched_ui" not in q for q in queries)
    assert all('"Статус" = \'error\'' not in q for q in queries)


def test_quality_error_totals_use_async_and_network_codes() -> None:
    dashboard = json.loads(Path("metabase_dashboards/04_quality_and_errors.json").read_text(encoding="utf-8"))
    card = next(card for card in dashboard["cards"] if card["name"] == "04 · Итоги по ошибкам за период")
    query = card["dataset_query"]["native"]["query"]
    assert "v_rpt_documents_ui" in query
    assert '"Статус (код)" IN (\'async_error\', \'network_error\')' in query
    assert "WHERE 1=1" in query


def test_archive_top_semd_uses_same_document_universe_as_total() -> None:
    dashboard = json.loads(Path("metabase_dashboards/06_semd_archive.json").read_text(encoding="utf-8"))
    total = next(card for card in dashboard["cards"] if card["name"] == "06 · Всего документов")
    top = next(card for card in dashboard["cards"] if card["name"] == "06 · Топ по типу СЭМД")
    clinic = next(card for card in dashboard["cards"] if card["name"] == "06 · Топ по клинике")
    assert "NULLIF(TRIM(\"Код СЭМД\"), '') IS NOT NULL" not in top["dataset_query"]["native"]["query"]
    assert '"СЭМД (архив)"' in top["dataset_query"]["native"]["query"]
    assert "v_rpt_semd_archive_ui" in total["dataset_query"]["native"]["query"]
    assert "v_rpt_semd_archive_ui" in top["dataset_query"]["native"]["query"]
    for query in (
        top["dataset_query"]["native"]["query"],
        clinic["dataset_query"]["native"]["query"],
    ):
        assert "LIMIT 8" not in query
        assert "Остальные (" in query
        assert "COUNT(DISTINCT \"Документ (ключ учёта)\")" in query


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
        "02_service.json": {"v_health_signals_ui"},
        "05_executive.json": {"active_jid"},
        "08_client_bianalytic.json": {"per_patient"},
    }
    violations: list[str] = []
    for path in _dashboard_paths():
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        for card in dashboard["cards"]:
            dq = card.get("dataset_query", {})
            if dq.get("type") != "native":
                continue
            query = dq["native"]["query"]
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
    card = next(card for card in dashboard["cards"] if card["name"] == "06 · Топ по типу СЭМД")
    query = card["dataset_query"]["native"]["query"]

    assert '"СЭМД (архив)"' in sql
    assert "Документ с ошибкой и не определён код" in sql
    assert '"Тип ошибки"' in sql
    assert '"СЭМД (архив)"' in query
    assert 'NULLIF(TRIM("Код СЭМД"), \'\') IS NOT NULL' not in query
    assert "Наименование СЭМД" not in query


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

    quality = json.loads(Path("metabase_dashboards/04_quality_and_errors.json").read_text(encoding="utf-8"))
    quality_queries = _native_queries(quality)
    assert any(
        '"Статус (код)" IN (\'success\', \'async_error\', \'network_error\')' in q
        and "Тип СЭМД (код · НСИ)" in q
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
    dashboard = json.loads(Path("metabase_dashboards/04_quality_and_errors.json").read_text(encoding="utf-8"))
    queries = _native_queries(dashboard)

    assert any("v_rpt_error_category_breakdown_ui" in query for query in queries)
    assert all("fact_egisz_transactions" not in query for query in queries)
    assert all("\"Ошибки JSON raw\"" not in query for query in queries)


def test_executive_dashboard_mixes_ops_and_finance_metrics() -> None:
    dashboard = json.loads(Path("metabase_dashboards/05_executive.json").read_text(encoding="utf-8"))
    queries = _native_queries(dashboard)

    assert dashboard["name"] == "05 Управленческий дашборд"

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

    assert dashboard["name"] == "07 Клиентский дашборд. Мониторинг сервиса интеграции с ЕГИСЗ"
    assert any(p["name"] == "JID клиники" for p in dashboard["parameters"])
    assert any(p["name"] == "Период" and p.get("default") == "past7days~" for p in dashboard["parameters"])
    assert any(p["name"] == "Тип документа" for p in dashboard["parameters"])
    assert all("public.v_rpt_client_documents_ui" in query for query in queries)
    assert all("{{client_jid}}" in query for query in queries)


def test_client_bianalytic_dashboard_uses_hashed_unique_keys() -> None:
    dashboard = json.loads(Path("metabase_dashboards/08_client_bianalytic.json").read_text(encoding="utf-8"))
    queries = _native_queries(dashboard)

    assert dashboard["name"] == "08 Клиентский дашборд. BI-аналитика ЭМД"
    assert any(p["name"] == "JID клиники" for p in dashboard["parameters"])
    assert all("public.v_rpt_client_documents_ui" in query for query in queries)
    assert all("{{client_jid}}" in query for query in queries)
    # Уникальный счёт пациентов/врачей идёт через hash-колонки, не через masked-имена.
    assert any("patient_hash" in q for q in queries)
    assert any("doctor_hash" in q for q in queries)
    # Финансы только количественные с пометкой «потенциальная выручка».
    assert any("Потенциальная выручка" in q for q in queries)


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


def test_no_retired_dashboard_files_remain() -> None:
    names = {p.name for p in _dashboard_paths()}
    # старые версии переименованных дашбордов должны быть удалены, иначе setup-dashboards.sh
    # импортирует дубликаты в коллекцию.
    assert "07_service_audit.json" not in names
    assert "08_client.json" not in names
    assert "05_executive.json" in names
    assert "07_client_service.json" in names
    assert "08_client_bianalytic.json" in names
