from __future__ import annotations

import json
import re
from pathlib import Path


DASHBOARDS_DIR = Path("metabase_dashboards")

EXPECTED_DASHBOARDS = {
    "01_overview.json": "A · Общая картина сервиса",
    "02_errors_quality.json": "B · Ошибки и качество",
    "03_orgs.json": "C · Клиники",
    "04_semd_types.json": "D · Типы СЭМД",
    "05_semd_archive.json": "E · Архив СЭМД",
    "06_operational.json": "F · Оперативный мониторинг",
}

# Set of public.* objects that must exist in db/dwh_init.sql for dashboards to import.
# Used as a contract check between Metabase JSON and DWH schema.
PUBLIC_OBJECT_RE = re.compile(r"public\.([a-zA-Z_][a-zA-Z0-9_]*)")


def _dashboard_paths() -> list[Path]:
    return sorted(DASHBOARDS_DIR.glob("*.json"))


def _dwh_init_sql() -> str:
    return (Path(__file__).resolve().parents[1] / "db" / "dwh_init.sql").read_text(encoding="utf-8")


def _dag_source() -> str:
    return (Path(__file__).resolve().parents[1] / "airflow" / "dags" / "egisz_elt_dag.py").read_text(encoding="utf-8")


def test_dashboard_files_match_expected_set() -> None:
    actual = {path.name: json.loads(path.read_text(encoding="utf-8"))["name"] for path in _dashboard_paths()}
    assert actual == EXPECTED_DASHBOARDS


def test_all_dashboards_default_to_full_width() -> None:
    for path in _dashboard_paths():
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload.get("width") == "full", f"{path.name} must default to full width"


def test_dashboard_public_objects_are_defined_in_dwh_init() -> None:
    sql = _dwh_init_sql()
    referenced: set[str] = set()
    for path in _dashboard_paths():
        for match in PUBLIC_OBJECT_RE.findall(path.read_text(encoding="utf-8")):
            referenced.add(match)

    missing = sorted(name for name in referenced if name not in sql)
    assert not missing, f"Dashboards reference public objects absent from db/dwh_init.sql: {missing}"


def test_reporting_views_keep_hybrid_refresh_contract_in_dwh_init() -> None:
    sql = _dwh_init_sql()

    assert "CREATE MATERIALIZED VIEW public.v_egisz_transactions_enriched_ui AS" in sql
    assert "CREATE MATERIALIZED VIEW public.v_docs_no_response_ui AS" in sql
    assert "CREATE MATERIALIZED VIEW public.v_stg_channel_errors_by_document AS" not in sql
    assert "CREATE OR REPLACE VIEW public.v_stg_channel_errors_by_document AS" in sql
    assert "idx_v_egisz_transactions_enriched_ui_transaction_id" in sql
    assert "idx_v_docs_no_response_ui_egmid" in sql


def test_dag_refresh_task_supports_hybrid_matview_and_view_contract() -> None:
    dag_source = _dag_source()

    assert "SELECT c.relname, c.relkind" in dag_source
    assert "Skipped refresh for regular view(s)" in dag_source
    assert "v_docs_no_response_ui" in dag_source
    assert 'if relkind == "m":' in dag_source
    assert 'elif relkind == "v":' in dag_source
    # Empty batches must short-circuit before issuing REFRESH MATERIALIZED VIEW —
    # otherwise the 5-minute DAG takes share-update-exclusive locks on idle runs.
    assert "Skipping reporting view refresh" in dag_source


def test_overview_dashboard_uses_kpi_summary_view() -> None:
    """01_overview должен агрегировать KPI через v_kpi_summary_ui, а не из фактов."""
    overview_text = (DASHBOARDS_DIR / "01_overview.json").read_text(encoding="utf-8")

    assert "v_kpi_summary_ui" in overview_text, (
        "01_overview.json must read aggregates from public.v_kpi_summary_ui — "
        "ad-hoc SELECTs over fact_egisz_transactions defeat the KPI rollup."
    )


def test_operational_dashboard_uses_service_health_and_no_response_views() -> None:
    """06_operational должен опираться на v_service_health_ui и матвью v_docs_no_response_ui."""
    operational_text = (DASHBOARDS_DIR / "06_operational.json").read_text(encoding="utf-8")

    assert "v_service_health_ui" in operational_text, (
        "06_operational.json must read pipeline freshness from public.v_service_health_ui"
    )
    assert "v_docs_no_response_ui" in operational_text, (
        "06_operational.json must read pending documents from materialized public.v_docs_no_response_ui"
    )
