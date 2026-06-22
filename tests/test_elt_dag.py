from __future__ import annotations

from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"


def _read(dag_file: str) -> str:
    return (DAGS_DIR / dag_file).read_text(encoding="utf-8")


def test_monolith_dag_is_split_into_three_files() -> None:
    assert not (DAGS_DIR / "egisz_elt_dag.py").exists()
    assert (DAGS_DIR / "egisz_extract_dag.py").exists()
    assert (DAGS_DIR / "egisz_dimensions_dag.py").exists()
    assert (DAGS_DIR / "egisz_reconcile_dag.py").exists()


def test_extract_dag_uses_entity_named_tasks_and_metadata_only_xcom() -> None:
    src = _read("egisz_extract_dag.py")

    assert 'dag_id="egisz_extract_dag"' in src
    # Entity-named tasks replace timing/granularity names.
    assert "def load_exchangelog_batch" in src
    assert "def build_document_facts" in src
    assert "def refresh_materialized_views" in src
    assert "def advance_logid_watermark" in src
    assert "def extract_and_load_batch" not in src
    assert "def analyze_staging" not in src
    assert "def transform_data" not in src
    assert "def update_watermark" not in src

    # ANALYZE folded into the load tail (the analyze_staging node is gone).
    assert "ANALYZE public.exchangelog_raw" in src

    # metadata-only XCom: rows never travel through XCom.
    assert '"rows":' not in src
    assert "load_raw_logs(pg_conn, log_rows)" in src

    # Schedule and batch size come from Variables, not body literals.
    assert 'Variable.get("egisz_extract_schedule", default_var="*/5 * * * *")' in src
    assert 'Variable.get("egisz_batch_size", default_var=50000)' in src
    assert 'Variable.get("egisz_max_load_rounds", default_var=200)' in src
    assert "while rounds < max_rounds" in src
    assert "BATCH_SIZE = 5000" not in src


def test_dimensions_dag_owns_dimension_sync_and_mart_maintenance() -> None:
    src = _read("egisz_dimensions_dag.py")

    assert 'dag_id="egisz_dimensions_dag"' in src
    assert "def sync_dimensions" in src
    assert "def maintain_enriched_ui" in src
    assert 'Variable.get("egisz_dimensions_schedule", default_var="@hourly")' in src


def test_reconcile_dag_does_full_constancy_check_without_moving_watermark() -> None:
    src = _read("egisz_reconcile_dag.py")

    assert 'dag_id="egisz_reconcile_dag"' in src
    assert "def reconcile_proxy_raw" in src
    assert "def reconcile_late_arrivals" not in src

    # Full source↔raw set-diff, not a banded window under the watermark.
    assert "fetch_exchangelog_logids(" in src
    assert "get_all_raw_logids(" in src
    assert "source_logids - raw_logids" in src
    assert "fetch_exchangelog_logids_in_band" not in src
    assert "get_raw_logids_in_band" not in src
    assert "RECONCILE_WATERMARK_LOOKBACK_LOGIDS" not in src

    # Watermark is never advanced by reconcile — GREATEST stays a forward-only writer.
    assert "update_cursors" not in src

    # Memory guard: hard-skip above the configured LOGID volume.
    assert 'Variable.get("egisz_reconcile_max_logids"' in src
    assert "count_exchangelog_rows(" in src

    # Schedule and window gap come from Variables.
    assert 'Variable.get("egisz_reconcile_schedule", default_var="@daily")' in src
    assert 'Variable.get("egisz_reconcile_window_max_gap", default_var=500)' in src
