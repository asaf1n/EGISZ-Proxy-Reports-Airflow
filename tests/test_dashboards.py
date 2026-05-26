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
    # Заглушечные таблицы (clients/subscriptions/billing/tickets/sla_metrics/
    # sed_transfers/churn_events/client_costs_monthly) и v_rpt_service_audit_*
    # удалены вместе с дашбордами, которые их использовали — никаких ссылок не должно быть.
    assert all("v_rpt_service_audit_" not in q for q in queries), (
        "05_executive must not reference removed v_rpt_service_audit_* views"
    )
    for placeholder in ("clients", "subscriptions", "billing", "tickets",
                        "sla_metrics", "sed_transfers", "churn_events",
                        "client_costs_monthly"):
        assert all(f"FROM {placeholder}" not in q and f"from {placeholder}" not in q for q in queries), (
            f"05_executive must not reference removed placeholder table '{placeholder}'"
        )

    # Источники только реальные.
    assert any("v_egisz_transactions_enriched_ui" in q for q in queries)
    assert any("v_rpt_documents_no_response_ui" in q for q in queries)

    # Фикс-тариф 10 000 ₽/JID/мес зашит явно в SQL карточек (раньше прятался в view-константе).
    assert any("10000" in q for q in queries), "MRR formula must use the fixed 10 000 ₽/JID/month tariff"

    # Новый статус pending должен учитываться в KPI-карточках.
    assert any("'pending'" in q for q in queries)


def test_executive_dashboard_uses_section_headers() -> None:
    dashboard = json.loads(Path("metabase_dashboards/05_executive.json").read_text(encoding="utf-8"))
    text_cards = [card for card in dashboard["cards"] if card.get("display") == "text"]
    assert len(text_cards) >= 3, "Управленческий дашборд должен сегментироваться text-заголовками разделов"
    for card in text_cards:
        assert "text" in card and card["text"].strip(), "text-карточка должна содержать содержимое"


def test_client_service_dashboard_uses_jid_stub_and_client_view() -> None:
    dashboard = json.loads(Path("metabase_dashboards/07_client_service.json").read_text(encoding="utf-8"))
    queries = _native_queries(dashboard)

    assert dashboard["name"] == "07 Клиентский дашборд. Мониторинг сервиса интеграции с ЕГИСЗ"
    assert any(p["name"] == "JID (заглушка авторизации)" for p in dashboard["parameters"])
    assert any(p["name"] == "Период" and p.get("default") == "past7days" for p in dashboard["parameters"])
    assert any(p["name"] == "Тип документа" for p in dashboard["parameters"])
    assert all("public.v_rpt_client_documents_ui" in query for query in queries)
    assert all("{{client_jid_stub}}" in query for query in queries)


def test_client_bianalytic_dashboard_uses_hashed_unique_keys() -> None:
    dashboard = json.loads(Path("metabase_dashboards/08_client_bianalytic.json").read_text(encoding="utf-8"))
    queries = _native_queries(dashboard)

    assert dashboard["name"] == "08 Клиентский дашборд. BI-аналитика ЭМД"
    assert any(p["name"] == "JID (заглушка авторизации)" for p in dashboard["parameters"])
    assert all("public.v_rpt_client_documents_ui" in query for query in queries)
    assert all("{{client_jid_stub}}" in query for query in queries)
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

            assert tags["client_jid_stub"]["type"] == "text", f"{path_name}: JID должен быть text-тегом, иначе фильтр-заглушка не работает"
            assert tags["client_jid_stub"]["required"] is True, f"{path_name}: JID должен быть required"
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


def test_no_legacy_dashboard_files_remain() -> None:
    names = {p.name for p in _dashboard_paths()}
    # старые версии переименованных дашбордов должны быть удалены, иначе setup-dashboards.sh
    # импортирует дубликаты в коллекцию.
    assert "07_service_audit.json" not in names
    assert "08_client.json" not in names
    assert "05_executive.json" in names
    assert "07_client_service.json" in names
    assert "08_client_bianalytic.json" in names
