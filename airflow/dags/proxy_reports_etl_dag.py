from __future__ import annotations

"""
Airflow DAG for proxy-reports ETL (Firebird → Postgres).

Config is passed via env (K8s Secret) to keep Airflow config surface minimal.
"""

import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="proxy_reports_firebird_to_postgres",
    description="Minimal incremental ETL: Firebird → Postgres (raw table + watermark).",
    schedule_interval=os.environ.get("PROXY_REPORTS_ETL_SCHEDULE", "@hourly"),
    start_date=__import__("datetime").datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["egisz", "firebird", "postgres", "etl"],
) as dag:
    test_connections = BashOperator(
        task_id="test_connections",
        bash_command="python -m proxy_reports_etl.cli test-connections",
    )

    sync = BashOperator(
        task_id="sync",
        bash_command="python -m proxy_reports_etl.cli sync",
    )

    test_connections >> sync
