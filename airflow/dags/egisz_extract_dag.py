from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from egisz_elt.common import (
    DWH_CONN_ID,
    PROXY_CONN_ID,
    BatchMetadata,
    PipelineBatchInfo,
    connect_fb,
    connect_pg,
)
from egisz_elt import extract

DWH_POOL = "dwh_postgres"


def _dwh_connection():
    return connect_pg(BaseHook.get_connection(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(BaseHook.get_connection(PROXY_CONN_ID))


@dag(
    dag_id="egisz_extract_dag",
    schedule=Variable.get("extract_schedule", default_var="*/5 * * * *"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "extract"],
)
def egisz_extract_pipeline() -> None:
    @task
    def extract_exchangelog() -> BatchMetadata:
        pg_conn = _dwh_connection()
        fb_conn = _proxy_connection()
        try:
            return extract.extract_exchangelog(
                pg_conn,
                fb_conn,
                raw_rows=int(Variable.get("extract_raw_rows", default_var=2000)),
                raw_rounds=int(Variable.get("extract_raw_rounds", default_var=3)),
            )
        finally:
            fb_conn.close()
            pg_conn.close()

    @task(pool=DWH_POOL)
    def transform_exchangelog(load_info: BatchMetadata) -> PipelineBatchInfo:
        pg_conn = _dwh_connection()
        try:
            return extract.transform_exchangelog(
                pg_conn,
                load_info,
                transform_rows=int(Variable.get("transform_rows", default_var=5000)),
                transform_rounds=int(Variable.get("transform_rounds", default_var=6)),
            )
        finally:
            pg_conn.close()

    extracted = extract_exchangelog()
    transform_exchangelog(extracted)


egisz_extract_pipeline()
