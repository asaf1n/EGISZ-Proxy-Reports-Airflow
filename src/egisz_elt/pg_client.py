from __future__ import annotations
import logging
import psycopg2
from psycopg2.extras import execute_values
from airflow.models import Connection

log = logging.getLogger(__name__)

ALLOWED_SYNC_TABLES = {"dim_organizations", "dim_licenses"}


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
    """Инициализация таблиц и представлений ELT"""
    with con.cursor() as cur:
        # Состояние курсоров (двойной курсор)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS elt_state (
                pipeline text PRIMARY KEY,
                last_log_id bigint DEFAULT 0,
                last_egmid bigint DEFAULT 0,
                updated_at timestamptz DEFAULT now()
            );
        """)

        # Raw таблица (Сюда грузим всё ПЕРЕД парсингом)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS egisz_raw (
                logid bigint PRIMARY KEY,
                logdate timestamptz,
                msgid text,
                logstate integer,
                logtext text,
                msgtext text,
                loaded_at timestamptz DEFAULT now()
            );
        """)

        # Таблица фактов (Результат трансформации)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_egisz_transactions (
                exchangelog_log_id bigint PRIMARY KEY REFERENCES egisz_raw(logid),
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
                processed_at timestamptz DEFAULT now()
            );
        """)

        # Вспомогательная таблица сообщений (EGISZ_MESSAGES)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS egisz_messages_raw (
                egmid bigint PRIMARY KEY,
                jid integer,
                kind text,
                created_at timestamptz,
                loaded_at timestamptz DEFAULT now()
            );
        """)

        # Справочник лицензий (9 полей)
        cur.execute("""
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

        # Справочник организаций (JPERSONS)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_organizations (
                jid integer PRIMARY KEY,
                name text,
                inn text,
                address text,
                updated_at timestamptz DEFAULT now()
            );
        """)

        # ПРЕДСТАВЛЕНИЯ (VIEWS)
        cur.execute("""
            CREATE OR REPLACE VIEW v_egisz_transactions_full AS
            SELECT t.*, o.name as org_name, o.inn as org_inn, l.kind as license_kind
            FROM fact_egisz_transactions t
            LEFT JOIN dim_organizations o ON t.org_oid = o.mo_uid
            LEFT JOIN dim_licenses l ON t.org_oid = l.mo_uid;

            CREATE OR REPLACE VIEW v_rpt_network_errors_detail_ui AS
            SELECT * FROM fact_egisz_transactions WHERE error_message LIKE 'Network Error%';
        """)
    con.commit()

def load_raw_logs(con, rows: list[tuple]):
    """Загрузка сырых данных в PG"""
    with con.cursor() as cur:
        execute_values(cur, """
            INSERT INTO egisz_raw (logid, logdate, msgid, logstate, logtext, msgtext)
            VALUES %s ON CONFLICT (logid) DO NOTHING
        """, rows)
    con.commit()

def upsert_facts(con, rows: list[dict]):
    """Загрузка распарсенных фактов"""
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
    if table_name not in ALLOWED_SYNC_TABLES:
        raise ValueError(f"Unsupported directory table: {table_name}")
    with con.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table_name} CASCADE;")
        execute_values(cur, f"INSERT INTO {table_name} VALUES %s", rows)
    con.commit()

def update_cursors(con, pipeline: str, log_id: int = 0, egmid: int = 0):
    with con.cursor() as cur:
        cur.execute("""
            INSERT INTO elt_state (pipeline, last_log_id, last_egmid)
            VALUES (%s, %s, %s) ON CONFLICT (pipeline) DO UPDATE SET
                last_log_id = GREATEST(elt_state.last_log_id, EXCLUDED.last_log_id),
                last_egmid = GREATEST(elt_state.last_egmid, EXCLUDED.last_egmid),
                updated_at = now();
        """, (pipeline, log_id, egmid))
    con.commit()
