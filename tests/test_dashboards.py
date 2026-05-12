from __future__ import annotations

import json
from pathlib import Path


def _dashboard_paths() -> list[Path]:
    return sorted(Path("metabase_dashboards").glob("*.json"))


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

    assert "public.v_stg_channel_network_errors_by_document" in query
    assert "'Сетевая ошибка'::text AS \"Тип ошибки\"" in query


def test_error_analytics_use_raw_json_column_for_grouping() -> None:
    dashboard = json.loads(Path("metabase_dashboards/04_quality_and_errors.json").read_text(encoding="utf-8"))
    queries = [card["dataset_query"]["native"]["query"] for card in dashboard["cards"] if card.get("dataset_query", {}).get("type") == "native"]

    assert any("v_rpt_error_interpretations_ui" in query for query in queries)
    assert all("fact_egisz_transactions" not in query for query in queries)
    assert all("\"Ошибки JSON raw\"" not in query for query in queries)
