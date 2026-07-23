from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

from egisz_elt.airflow_vars import get_int, get_str
from egisz_elt.common import (
    DWH_CONN_ID,
    PROXY_CONN_ID,
    BatchMetadata,
    PipelineBatchInfo,
    connect_fb,
    connect_pg,
)
from egisz_elt import common, extract

DWH_POOL = "dwh_postgres"


def _dwh_connection():
    return connect_pg(BaseHook.get_connection(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(BaseHook.get_connection(PROXY_CONN_ID))


@dag(
    dag_id="egisz_extract_dag",
    schedule=get_str("extract_schedule"),
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
                raw_rows=get_int("extract_raw_rows"),
                raw_rounds=get_int("extract_raw_rounds"),
            )
        finally:
            fb_conn.close()
            pg_conn.close()

    # Ретраи гасят транзиентный DeadlockDetected: maintenance-прогоны схемы (полный
    # reconcile_document_attributes / recompute в 90-й части) пересекаются с 5-минутным
    # батчем по блокировкам documents/document_attributes; откат + повтор безопасны
    # (transform идемпотентен, watermark двигается только после успеха).
    @task(pool=DWH_POOL, retries=2, retry_delay=timedelta(minutes=1))
    def transform_exchangelog(load_info: BatchMetadata) -> PipelineBatchInfo:
        pg_conn = _dwh_connection()
        try:
            return extract.transform_exchangelog(
                pg_conn,
                load_info,
                transform_rows=get_int("transform_rows"),
                transform_rounds=get_int("transform_rounds"),
            )
        finally:
            pg_conn.close()

    # Недельные витрины отделены от transform: их refresh не должен ретраить
    # парсинг батча, а гейт transformed > 0 повторяет условие inline-refresh
    # rpt_error_breakdown (см. extract.transform_exchangelog).
    @task(pool=DWH_POOL, retries=2, retry_delay=timedelta(minutes=1))
    def refresh_weekly_reports(batch: PipelineBatchInfo) -> None:
        if int(batch.get("transformed", 0) or 0) <= 0:
            return
        pg_conn = _dwh_connection()
        try:
            common.refresh_weekly_reports(pg_conn)
        finally:
            pg_conn.close()

    extracted = extract_exchangelog()
    transformed = transform_exchangelog(extracted)
    refresh_weekly_reports(transformed)


egisz_extract_pipeline()
