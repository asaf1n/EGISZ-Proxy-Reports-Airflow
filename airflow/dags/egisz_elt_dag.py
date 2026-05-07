from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Импорт клиентской логики из вашего пакета
from egisz_elt.fb_client import connect_fb
from egisz_elt.pg_client import (
    connect_pg,
    load_raw_logs,
    sync_directory,
    update_cursors,
)

log = logging.getLogger(__name__)

# Константы окружения
PIPELINE = "main"
DWH_CONN_ID = "dwh_egisz_pg"
PROXY_CONN_ID = "proxy_egisz_fb"
# Путь к SQL файлам внутри контейнера Airflow
SQL_PATH = os.path.join(os.path.dirname(__file__), 'sql')

@dag(
    dag_id="egisz_elt_dag",
    schedule=os.getenv("EGISZ_ELT_SCHEDULE", "@hourly"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    template_searchpath=[SQL_PATH], # Airflow будет искать SQL файлы здесь
    tags=['egisz', 'elt', 'dwh'],
)
def egisz_elt_pipeline():

    # 1. Инициализация структуры DWH (Bootstrap)
    # Выполняет 001_dwh_bootstrap.sql: создает таблицы, функции и витрины
    bootstrap_dwh = PostgresOperator(
        task_id="bootstrap_dwh",
        postgres_conn_id=DWH_CONN_ID,
        sql="001_dwh_bootstrap.sql",
    )

    @task
    def sync_dimensions_task():
        """Синхронизация справочников (организации, лицензии) из Firebird в Postgres."""
        fb_conn = connect_fb(BaseHook.get_connection(PROXY_CONN_ID))
        pg_conn = connect_pg(BaseHook.get_connection(DWH_CONN_ID))
        try:
            log.info("Starting JPERSONS sync to dim_organizations...")
            sync_directory(fb_conn, pg_conn, "JPERSONS", "dim_organizations")
            log.info("Dimensions sync completed successfully.")
        finally:
            fb_conn.close()
            pg_conn.close()

    @task
    def extract_from_proxy_task() -> dict[str, Any]:
        """Извлечение новых записей из Firebird шлюза."""
        # Здесь должна быть ваша логика извлечения (fb_client.fetch_new_logs)
        # Для примера возвращаем структуру:
        return {
            "count": 0, 
            "max_id": 0, 
            "rows": []
        }

    @task
    def load_to_dwh_task(extraction_result: dict[str, Any]) -> dict[str, Any]:
        """Загрузка сырых данных в стейджинг (egisz_raw)."""
        if extraction_result["count"] == 0:
            return extraction_result

        pg_conn = connect_pg(BaseHook.get_connection(DWH_CONN_ID))
        try:
            load_raw_logs(pg_conn, extraction_result["rows"])
            log.info(f"Loaded {extraction_result['count']} rows to staging.")
        finally:
            pg_conn.close()
        return extraction_result

    @task
    def transform_data_task(load_info: dict[str, Any]) -> dict[str, Any]:
        """Запуск SQL-трансформации внутри Postgres (ELT)."""
        if load_info["count"] == 0:
            return load_info

        pg_conn = connect_pg(BaseHook.get_connection(DWH_CONN_ID))
        try:
            with pg_conn.cursor() as cur:
                # Вызываем функцию, созданную шагом bootstrap
                cur.execute("SELECT public.egisz_transform_raw_to_facts(%s);", (load_info["max_id"],))
                transformed = cur.fetchone()[0]
                log.info(f"ELT Transformation: {transformed} rows moved to facts.")
        finally:
            pg_conn.close()
        return load_info

    @task
    def update_watermark_task(cursor_info: dict[str, Any]) -> None:
        """Обновление курсора (watermark), чтобы не качать данные повторно."""
        if cursor_info["max_id"] == 0:
            return
            
        pg_conn = connect_pg(BaseHook.get_connection(DWH_CONN_ID))
        try:
            update_cursors(pg_conn, PIPELINE, log_id=cursor_info["max_id"])
            log.info(f"Watermark updated to ID: {cursor_info['max_id']}")
        finally:
            pg_conn.close()

    # --- Определение цепочки выполнения (Pipeline) ---

    # Сначала создаем базу и обновляем справочники
    init_db = bootstrap_dwh >> sync_dimensions_task()

    # Затем основной цикл ELT
    extraction = extract_from_proxy_task()
    loading = load_to_dwh_task(extraction)
    transformation = transform_data_task(loading)
    watermark = update_watermark_task(transformation)

    # Весь ELT-цикл должен ждать завершения инициализации базы
    init_db >> extraction

# Запуск пайплайна
egisz_elt_pipeline()