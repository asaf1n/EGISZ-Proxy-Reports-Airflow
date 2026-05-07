from __future__ import annotations
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

from egisz_elt.fb_client import connect_fb
from egisz_elt.pg_client import connect_pg, ensure_tables, update_cursors, upsert_facts, sync_directory, load_raw_logs
from egisz_elt.normalize import normalize_exchange_row

log = logging.getLogger(__name__)
PIPELINE = "main"
BATCH_SIZE = 1000

@dag(dag_id="egisz_elt_dag", schedule_interval=timedelta(minutes=15), start_date=datetime(2023, 1, 1), catchup=False)
def egisz_elt():

    @task
    def setup_db():
        pg_conn = connect_pg(BaseHook.get_connection("postgres_dwh"))
        ensure_tables(pg_conn)
        pg_conn.close()

    @task
    def sync_dims():
        fb_uri = BaseHook.get_connection("firebird_proxy").get_uri()
        pg_conn = connect_pg(BaseHook.get_connection("postgres_dwh"))
        fb_conn = connect_fb(fb_uri)
        try:
            with fb_conn.cursor() as cur:
                cur.execute("SELECT JID, NAME, INN, ADDRESS FROM JPERSONS")
                sync_directory(pg_conn, "dim_organizations", cur.fetchall())
            with fb_conn.cursor() as cur:
                cur.execute("SELECT ID, SERVICE_TYPE, JID, MO_UID, MO_DOMEN, BDATE, FDATE, KIND, MODIFYDATE FROM EGISZ_LICENSES")
                sync_directory(pg_conn, "dim_licenses", cur.fetchall())
        finally:
            fb_conn.close(); pg_conn.close()

    @task
    def extract_and_load_raw():
        """Шаг 1: Извлекаем из Firebird и сохраняем Raw в Postgres"""
        pg_conn = connect_pg(BaseHook.get_connection("postgres_dwh"))
        with pg_conn.cursor() as cur:
            cur.execute("SELECT last_log_id FROM elt_state WHERE pipeline = %s", (PIPELINE,))
            row = cur.fetchone()
            last_log_id = row[0] if row else 0
        
        fb_conn = connect_fb(BaseHook.get_connection("firebird_proxy").get_uri())
        with fb_conn.cursor() as cur:
            cur.execute(f"SELECT FIRST {BATCH_SIZE} LOGID, LOGDATE, MSGID, LOGSTATE, LOGTEXT, MSGTEXT FROM EXCHANGELOG WHERE LOGID > {last_log_id} ORDER BY LOGID")
            rows = cur.fetchall()
        fb_conn.close()

        if rows:
            load_raw_logs(pg_conn, rows)
            return {"count": len(rows), "max_id": max(r[0] for r in rows)}
        return {"count": 0, "max_id": last_log_id}

    @task
    def transform_raw_to_facts(raw_info: dict):
        """Шаг 2: Трансформируем Raw данные, уже лежащие в Postgres, в таблицу фактов"""
        if raw_info["count"] == 0: return
        
        pg_conn = connect_pg(BaseHook.get_connection("postgres_dwh"))
        # Выбираем не обработанные логи
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT logid, logdate, msgid, logstate, logtext, msgtext 
                FROM proxy_reports_raw r
                WHERE NOT EXISTS (SELECT 1 FROM fact_egisz_transactions f WHERE f.exchangelog_log_id = r.logid)
                LIMIT %s
            """, (BATCH_SIZE,))
            raw_rows = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]
        
        # Парсинг
        fact_rows = [norm for r in raw_rows if (norm := normalize_exchange_row(r))]
        
        # Загрузка фактов и обновление курсора
        upsert_facts(pg_conn, fact_rows)
        update_cursors(pg_conn, PIPELINE, log_id=raw_info["max_id"], egmid=0) # EGMID добавим по аналогии
        pg_conn.close()

    setup_db() >> sync_dims() >> transform_raw_to_facts(extract_and_load_raw())

dag_instance = egisz_elt()