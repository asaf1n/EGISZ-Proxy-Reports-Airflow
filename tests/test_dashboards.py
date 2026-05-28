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


def test_operational_error_types_include_network_slice() -> None:
    dashboard = json.loads(Path("metabase_dashboards/01_operational.json").read_text(encoding="utf-8"))
    card = next(card for card in dashboard["cards"] if card["name"] == "Ошибки по типу")
    query = card["dataset_query"]["native"]["query"]
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")

    assert "public.v_rpt_error_category_breakdown_ui" in query
    assert "public.v_stg_channel_network_errors_by_document" in sql
    assert "'Сетевая ошибка'::text" in sql


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
    assert "NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'localUid')), '') IS NOT NULL" in transform_sql
    assert '"ИНН клиники"' in sql
    assert '"Исходный текст ошибки"' in sql


def test_operational_status_breakdown_uses_three_recognized_statuses() -> None:
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
    assert "WHERE \"Статус\" IN ('success', 'error')" in query
    assert "WHEN \"Статус\" = 'success' THEN 'Успешный ответ'" in query
    assert "WHEN \"Статус\" = 'error' AND \"Тип ошибки\" = 'Сетевая ошибка' THEN 'Ошибка связи'" in query
    assert "WHEN \"Статус\" = 'error' THEN 'Ошибка регистрации'" in query
    assert "COUNT(DISTINCT \"Документ (ключ учёта)\")::bigint" in query
    assert "отказы РЭМД (status=error)" not in query
    assert card["metabase-field-filters"]["dwh_date"] == {
        "table_ref": "public.v_rpt_documents_ui",
        "field_name": "Дата обработки",
    }
    assert "Успешный ответ" in row_keys
    assert "Ошибка регистрации" in row_keys
    assert "Ошибка связи" in row_keys
    assert "В обработке" not in row_keys
    assert "Отправлен" not in row_keys
    assert "Неизвестная ошибка" not in row_keys
    assert "Документы в ожидании" not in row_keys
    assert "Нераспознан" not in row_keys
    assert "public.v_rpt_semd_archive_ui" in trend_query
    assert "CREATE OR REPLACE VIEW public.v_rpt_documents_ui" in Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert "FROM public.v_rpt_documents_ui" in Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    assert "Успешный ответ" in trend_query
    assert "Ошибка регистрации" in trend_query
    assert trend_card["metabase-field-filters"]["dwh_date"] == {
        "table_ref": "public.v_rpt_semd_archive_ui",
        "field_name": "Дата обработки",
    }


def test_archive_no_code_documents_are_qualified_by_status() -> None:
    sql = Path("db/parts/80_views_rpt.sql").read_text(encoding="utf-8")
    dashboard = json.loads(Path("metabase_dashboards/06_semd_archive.json").read_text(encoding="utf-8"))
    card = next(card for card in dashboard["cards"] if card["name"] == "06 · Топ по типу СЭМД")
    query = card["dataset_query"]["native"]["query"]

    assert '"СЭМД (архив)"' in sql
    assert "Документ с ошибкой и не определён код" in sql
    assert '"Тип ошибки"' in sql
    assert 'NULLIF(TRIM("Код СЭМД"), \'\') IS NOT NULL' in query
    assert "Наименование СЭМД" in query
    assert 'TRIM("СЭМД (архив)")' not in query


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
    assert "NULLIF(btrim(public.egisz_xml_text(sr.msgtext, 'localUid')), '') IS NOT NULL" in transform_sql
    assert "pending_source AS" not in transform_sql

    quality = json.loads(Path("metabase_dashboards/04_quality_and_errors.json").read_text(encoding="utf-8"))
    quality_queries = _native_queries(quality)
    assert any('"Статус" IN (\'success\', \'error\')' in q and "Тип СЭМД (код · НСИ)" in q for q in quality_queries)

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
    assert any(p["name"] == "Период" and p.get("default") == "past7days" for p in dashboard["parameters"])
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
