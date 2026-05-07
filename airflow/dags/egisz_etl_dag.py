from __future__ import annotations
import logging
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowSkipException

from egisz_etl.fb_client import connect_fb
from egisz_etl.pg_client import connect_pg, ensure_tables, update_cursors, upsert_facts, sync_directory
from egisz_etl.normalize import normalize_exchange_row

log = logging.getLogger(__name__)
PIPELINE = "main"
BATCH_SIZE = 1000

@dag(
    dag_id="egisz_etl_dag",
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["egisz"]
)
def egisz_etl():

    @task
    def setup_db():
        pg_conn = connect_pg(BaseHook.get_connection("postgres_dwh"))
        ensure_tables(pg_conn) # Создает и таблицы, и вьюхи
        pg_conn.close()

    @task
    def sync_dictionaries():
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
            fb_conn.close()
            pg_conn.close()

    @task(multiple_outputs=True)
    def extract_and_transform():
        pg_conn = connect_pg(BaseHook.get_connection("postgres_dwh"))
        with pg_conn.cursor() as cur:
            cur.execute("SELECT last_log_id, last_egmid FROM etl_state WHERE pipeline = %s", (PIPELINE,))
            row = cur.fetchone()
            last_log_id, last_egmid = row if row else (0, 0)
        pg_conn.close()

        fb_conn = connect_fb(BaseHook.get_connection("firebird_proxy").get_uri())
        try:
            with fb_conn.cursor() as cur:
                cur.execute(f"SELECT FIRST {BATCH_SIZE} LOGID, LOGDATE, MSGID, LOGSTATE, LOGTEXT, MSGTEXT FROM EXCHANGELOG WHERE LOGID > {last_log_id} ORDER BY LOGID")
                raw_logs = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]
            
            with fb_conn.cursor() as cur:
                cur.execute(f"SELECT FIRST {BATCH_SIZE} EGMID, JID, KIND, createdate FROM EGISZ_MESSAGES WHERE EGMID > {last_egmid} ORDER BY EGMID")
                raw_messages = cur.fetchall()

            if not raw_logs and not raw_messages: raise AirflowSkipException("No data")

            fact_rows = [norm for r in raw_logs if (norm := normalize_exchange_row(r))]
            new_log_id = max([r['LOGID'] for r in raw_logs] + [last_log_id])
            new_egmid = max([m[0] for m in raw_messages] + [last_egmid])

            return {"fact_rows": fact_rows, "raw_messages": raw_messages, "new_log_id": new_log_id, "new_egmid": new_egmid}
        finally:
            fb_conn.close()

    @task
    def load_data(data: dict):
        pg_conn = connect_pg(BaseHook.get_connection("postgres_dwh"))
        try:
            upsert_facts(pg_conn, data['fact_rows'])
            if data['raw_messages']:
                with pg_conn.cursor() as cur:
                    from psycopg2.extras import execute_values
                    execute_values(cur, "INSERT INTO fact_proxy_exchange (egmid, jid, kind, created_at) VALUES %s ON CONFLICT (egmid) DO NOTHING", data['raw_messages'])
            update_cursors(pg_conn, PIPELINE, data['new_log_id'], data['new_egmid'])
        finally:
            pg_conn.close()

    setup_db() >> sync_dictionaries() >> load_data(extract_and_transform())

dag_instance = egisz_etl()