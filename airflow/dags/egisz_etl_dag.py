from __future__ import annotations
import logging
from typing import Any
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException

# ... импорты клиентов и константы остаются без изменений ...

# 1. Применяем декоратор @dag к основной функции
@dag(
    dag_id="egisz_etl_dag",
    schedule_interval=timedelta(minutes=15), # Укажите нужный интервал
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["egisz"]
)
def egisz_etl():
    
    # 2. Применяем @task (с multiple_outputs=True, так как возвращаем словари)
    @task(multiple_outputs=True)
    def get_cursors() -> dict[str, str]:
        # ... тело функции ...

    @task(multiple_outputs=True)
    def extract_from_proxy(cursors: dict[str, str]) -> dict[str, list[dict[str, Any]]]:
        # ... тело функции ...

    @task(multiple_outputs=True)
    def transform_and_resolve_identity(extract_data: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
        # ... тело функции ...

    @task(multiple_outputs=True)
    def load_to_dwh(transformed_data: dict[str, list[dict[str, Any]]]) -> dict[str, str | None]:
        # ... тело функции ...

    @task() # Здесь multiple_outputs не нужен, функция ничего не возвращает
    def update_watermarks(old_cursors: dict[str, str], new_cursors: dict[str, str | None]):
        # ... тело функции ...

    # Определение зависимостей (остается без изменений)
    cursors = get_cursors()
    extracted = extract_from_proxy(cursors)
    transformed = transform_and_resolve_identity(extracted)
    loaded_cursors = load_to_dwh(transformed)
    update_watermarks(cursors, loaded_cursors)

dag_instance = egisz_etl()