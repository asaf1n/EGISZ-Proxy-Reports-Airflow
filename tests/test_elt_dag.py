from __future__ import annotations

from pathlib import Path


def test_elt_dag_does_not_put_exchangelog_rows_in_xcom_contract() -> None:
    dag_source = (Path(__file__).resolve().parents[1] / "airflow" / "dags" / "egisz_elt_dag.py").read_text(
        encoding="utf-8"
    )

    assert "def extract_and_load_batch" in dag_source
    assert "def extract_cursor_batches" not in dag_source
    assert "def load_to_dwh" not in dag_source
    assert "SOURCE_MIN_CREATED_AT" not in dag_source
    assert '"rows":' not in dag_source
    assert "load_raw_logs(pg_conn, log_rows)" in dag_source
