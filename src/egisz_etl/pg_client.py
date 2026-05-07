from __future__ import annotations
import logging
import psycopg2
from psycopg2.extras import execute_values
from airflow.models import Connection

log = logging.getLogger(__name__)

def connect_pg(conn_params: Connection | str) -> psycopg2.extensions.connection:
    if isinstance(conn_params, str):
        return psycopg2.connect(conn_params)
    return psycopg2.connect(
        host=conn_params.host,
        port=conn_params.port,
        user=conn_params.login,
        password=conn_params.password,
        database=conn_params.schema
    )

def ensure_tables(con: psycopg2.extensions.connection) -> None:
    with con.cursor() as cur:
        # 1. Метаданные ETL
        cur.execute("""
            CREATE TABLE IF NOT EXISTS etl_state (
                pipeline text PRIMARY KEY,
                last_log_id bigint DEFAULT 0,
                last_egmid bigint DEFAULT 0,
                updated_at timestamptz DEFAULT now()
            );
        """)

        # 2. Таблицы фактов и справочников
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_egisz_transactions (
                exchangelog_log_id bigint PRIMARY KEY,
                log_date timestamptz,
                message_id text,
                relates_to_id text,
                local_uid_semd text,
                emdr_id text,
                doc_number text,
                org_oid text,
                status text,
                error_message text,
                callback_url text,
                loaded_at timestamptz DEFAULT now()
            );
            
            CREATE TABLE IF NOT EXISTS fact_proxy_exchange (
                egmid bigint PRIMARY KEY,
                jid integer,
                kind text,
                created_at timestamptz,
                loaded_at timestamptz DEFAULT now()
            );

            CREATE TABLE IF NOT EXISTS dim_organizations (
                jid integer PRIMARY KEY,
                name text,
                inn text,
                address text,
                updated_at timestamptz DEFAULT now()
            );

            CREATE TABLE IF NOT EXISTS dim_licenses (
                id bigint PRIMARY KEY,
                service_type integer,
                jid integer,
                mo_uid text,
                mo_domen text,
                bdate date,
                fdate date,
                kind text,
                modifydate timestamptz,
                updated_at timestamptz DEFAULT now()
            );
        """)

        # 3. ПРЕДСТАВЛЕНИЯ (Views) для Metabase
        # На основе анализа AGENTS.md из egisz-monitor-corp
        cur.execute("""
            -- Ключевая витрина: Транзакции с данными организаций
            CREATE OR REPLACE VIEW v_egisz_transactions_full AS
            SELECT 
                t.*,
                o.name as org_name,
                o.inn as org_inn,
                l.kind as license_kind,
                l.fdate as license_expiry
            FROM fact_egisz_transactions t
            LEFT JOIN dim_organizations o ON t.org_oid = o.mo_uid OR (t.org_oid IS NULL AND o.jid = (SELECT jid FROM fact_proxy_exchange WHERE egmid = t.exchangelog_log_id LIMIT 1))
            LEFT JOIN dim_licenses l ON t.org_oid = l.mo_uid;

            -- Витрина только для сетевых ошибок (LOGSTATE=3)
            CREATE OR REPLACE VIEW v_rpt_network_errors_detail_ui AS
            SELECT * FROM fact_egisz_transactions 
            WHERE error_message LIKE 'Network Error%';

            -- Сводка по статусам для операционного дашборда
            CREATE OR REPLACE VIEW v_egisz_status_counts AS
            SELECT status, count(*) as cnt, date_trunc('day', log_date) as day
            FROM fact_egisz_transactions
            GROUP BY 1, 3;
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_fact_local_uid ON fact_egisz_transactions(local_uid_semd);")
        
    con.commit()
    log.info("DWH: Tables and Views ensured.")

def upsert_facts(con, rows: list[dict]):
    if not rows: return
    keys = ['log_id', 'log_date', 'msg_id', 'relates_to', 'local_uid', 'emdr_id', 'doc_num', 'org_oid', 'status', 'error_msg', 'callback_url']
    tuples = [tuple(r.get(k) for k in keys) for r in rows]
    with con.cursor() as cur:
        execute_values(cur, """
            INSERT INTO fact_egisz_transactions 
            (exchangelog_log_id, log_date, message_id, relates_to_id, local_uid_semd, emdr_id, doc_number, org_oid, status, error_message, callback_url)
            VALUES %s ON CONFLICT (exchangelog_log_id) DO UPDATE SET
                status = EXCLUDED.status,
                error_message = EXCLUDED.error_message,
                emdr_id = COALESCE(fact_egisz_transactions.emdr_id, EXCLUDED.emdr_id)
        """, tuples)
    con.commit()

def sync_directory(con, table_name: str, rows: list[tuple]):
    if not rows: return
    with con.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
        execute_values(cur, f"INSERT INTO {table_name} VALUES %s", rows)
    con.commit()

def update_cursors(con, pipeline: str, log_id: int, egmid: int):
    with con.cursor() as cur:
        cur.execute("""
            INSERT INTO etl_state (pipeline, last_log_id, last_egmid)
            VALUES (%s, %s, %s) ON CONFLICT (pipeline) DO UPDATE SET
                last_log_id = GREATEST(etl_state.last_log_id, EXCLUDED.last_log_id),
                last_egmid = GREATEST(etl_state.last_egmid, EXCLUDED.last_egmid),
                updated_at = now();
        """, (pipeline, log_id, egmid))
    con.commit()