from __future__ import annotations

import os
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"
DWH_POOL = "dwh_postgres"
_AIRFLOW_TEST_DB = Path(__file__).resolve().parent / ".pytest_airflow.db"

os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault(
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
    f"sqlite:///{_AIRFLOW_TEST_DB.as_posix()}",
)


def _init_airflow_test_db() -> None:
    from airflow.utils.db import initdb

    initdb()


def _ensure_dwh_pool() -> None:
    from airflow.models.pool import Pool
    from airflow.utils.session import create_session

    _init_airflow_test_db()
    with create_session() as session:
        if session.query(Pool).filter(Pool.pool == DWH_POOL).first() is None:
            session.add(
                Pool(
                    pool=DWH_POOL,
                    slots=1,
                    description="Exclusive DWH transform / reconcile / enriched mart maintenance",
                    include_deferred=False,
                )
            )


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
    assert "def extract_exchangelog" in src
    assert "def transform_exchangelog" in src
    assert "def load_exchangelog_batch" not in src
    assert "def process_exchangelog_batch" not in src
    assert "def has_new_exchangelog_rows" not in src
    assert "def build_document_facts" not in src
    assert "def refresh_materialized_views" not in src
    assert "def advance_logid_watermark" not in src
    assert "def extract_and_load_batch" not in src
    assert "def analyze_staging" not in src
    assert "def transform_data" not in src
    assert "def update_watermark" not in src

    assert "extract.extract_exchangelog" in src
    assert "extract.transform_exchangelog" in src
    assert "pending_transform_tail" not in src
    assert "bounded_transform_to_logid" not in src
    assert "transform_raw_to_facts" not in src
    assert "load_raw_logs" not in src

    assert '"rows":' not in src
    assert 'Variable.get("extract_schedule", default_var="*/5 * * * *")' in src
    assert 'Variable.get("extract_raw_rows", default_var=2000)' in src
    assert 'Variable.get("extract_raw_rounds", default_var=3)' in src
    assert 'Variable.get("transform_rows", default_var=5000)' in src
    assert 'Variable.get("transform_rounds", default_var=6)' in src
    assert 'Variable.get("egisz_' not in src
    assert 'pool="dwh_postgres"' in src or "pool=DWH_POOL" in src
    assert "BATCH_SIZE = 5000" not in src
    assert "@task.short_circuit" not in src

    assert "transform_exchangelog(extracted)" in src
    # TaskFlow выстраивает зависимость через передачу XCom; явный ">>" избыточен.
    assert "extracted >> transformed" not in src
    assert "get_current_context" not in src


def test_dimensions_dag_owns_dimension_sync_and_mart_maintenance() -> None:
    src = _read("egisz_dimensions_dag.py")

    assert 'dag_id="egisz_dimensions_dag"' in src
    assert "def sync_dimensions" in src
    assert "def dimensions_changed" not in src
    assert "def maintain_enriched_ui" not in src
    assert "sync_directories" in src
    assert "reconcile_document_attributes_ui" in src
    assert 'Variable.get("dimensions_schedule", default_var="@hourly")' in src
    assert "@task.short_circuit" not in src
    assert "pool=DWH_POOL" in src or 'pool="dwh_postgres"' in src


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

    # Memory guard: hard-fail above the configured LOGID volume (no silent skip).
    assert 'Variable.get("reconcile_max_logids"' in src
    assert "count_exchangelog_rows(" in src
    assert "raise RuntimeError" in src

    # Schedule comes from Variables; lookback/window gap are derived in SQL/Python.
    assert 'Variable.get("reconcile_schedule", default_var="@daily")' in src
    assert 'Variable.get("egisz_reconcile_window_max_gap"' not in src
    assert 'Variable.get("egisz_gdf_lookback_logids"' not in src
    assert "pending_transform_tail" in src
    assert "AirflowSkipException" in src
    assert "backfill_semd_codes" not in src


def test_all_dag_files_compile() -> None:
    import py_compile

    for path in sorted(DAGS_DIR.glob("egisz_*.py")):
        py_compile.compile(str(path), doraise=True)


def test_up_ps1_provisions_dwh_postgres_pool() -> None:
    src = Path(__file__).resolve().parents[1].joinpath("up.ps1").read_text(encoding="utf-8")
    assert "Initialize-AirflowDwhPool" in src
    assert "Remove-LegacyAirflowVariables" not in src
    assert "variables delete" not in src
    assert "egisz_extract_schedule" not in src
    assert "pools', 'set', $DwhPoolName" in src or "pools set" in src
    assert "dwh_postgres" in src
    assert "Initialize-EgiszDags" not in src
    assert "dags', 'unpause" not in src
    assert "egisz-airflow-worker:latest" in src
    assert "egisz-metabase:latest" in src
    assert "Get-DashboardsManifestHash" in src
    assert 'egisz-metabase:${metabaseTag}' in src
    assert "Get-LoadBalancerPortForwardConflict" in src
    assert "Sync-MetabaseDashboardArtifacts" in src
    assert "Test-MetabaseIntegrationDashboard" in src
    assert "verify_metabase_integration.py" in src
    assert "Test-MetabaseManifestUnchanged" in src
    assert "metabase-deployed-manifest" in src


def test_dag_bag_loads_egisz_dags() -> None:
    import importlib.util
    import sys

    if importlib.util.find_spec("airflow.models") is None:
        pytest.fail("apache-airflow is required: pip install -e '.[dev]'")

    from airflow.models import DagBag

    repo_root = DAGS_DIR.parents[1]
    src = str(repo_root / "src")
    if src not in sys.path:
        sys.path.insert(0, src)

    _ensure_dwh_pool()
    dagbag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    assert not dagbag.import_errors, dagbag.import_errors

    pool_warnings = [
        msg for msg in getattr(dagbag, "warning_messages", []) if "non-existent pools" in msg
    ]
    assert not pool_warnings, pool_warnings

    extract = dagbag.dags["egisz_extract_dag"]
    dimensions = dagbag.dags["egisz_dimensions_dag"]
    reconcile = dagbag.dags["egisz_reconcile_dag"]

    assert {t.task_id for t in extract.tasks} == {
        "extract_exchangelog",
        "transform_exchangelog",
    }
    assert {t.task_id for t in dimensions.tasks} == {"sync_dimensions"}
    assert {t.task_id for t in reconcile.tasks} == {"reconcile_proxy_raw"}

    assert extract.task_dict["extract_exchangelog"].downstream_task_ids == {
        "transform_exchangelog"
    }
    assert reconcile.task_dict["reconcile_proxy_raw"].downstream_task_ids == set()
