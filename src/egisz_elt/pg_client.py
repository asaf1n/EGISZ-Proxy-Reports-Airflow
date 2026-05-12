from __future__ import annotations

import logging
from importlib import resources
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

ALLOWED_SYNC_TABLES = {"dim_organizations", "dim_licenses"}
DIRECTORY_COLUMNS = {
    "dim_organizations": ("jid", "name", "inn", "address"),
    "dim_licenses": ("id", "service_type", "jid", "mo_uid", "mo_domen", "bdate", "fdate", "kind", "modifydate"),
}
DIRECTORY_PK_COLUMNS = {
    "dim_organizations": ("jid",),
    "dim_licenses": ("id",),
}
REQUIRED_DWH_OBJECTS = {
    "elt_state",
    "exchangelog_raw",
    "egisz_messages_raw",
    "dim_organizations",
    "dim_licenses",
    "dim_semd_types",
    "egisz_error_interpretation_rules",
    "fact_egisz_transactions",
    "egisz_xml_text",
    "egisz_normalize_message_id",
    "safe_cast_timestamptz",
    "egisz_clean_host",
    "egisz_extract_jid_from_endpoint",
    "egisz_clean_text_value",
    "egisz_normalize_semd_code",
    "egisz_transform_raw_to_facts",
    "egisz_semd_type_report_label",
    "egisz_error_interpretation_schematron_chunk",
    "egisz_error_interpretation_item",
    "egisz_error_interpretation_type",
    "egisz_error_interpretation_row",
    "egisz_error_messages_row",
    "egisz_xml_error_items",
    "egisz_build_errors_json",
    "v_egisz_transactions_enriched_ui",
    "v_egisz_transactions_full",
    "v_rpt_error_interpretations_ui",
    "v_rpt_documents_no_response_ui",
    "v_rpt_semd_archive_ui",
    "v_rpt_network_errors_detail_ui",
    "v_rpt_connectivity_global_daily_ui",
    "v_rpt_clinic_connectivity_daily_ui",
    "v_stg_channel_errors_by_document",
    "v_stg_channel_network_errors_by_document",
    "v_health_by_clinic_ui",
    "v_health_proxy_db_ui",
    "v_health_signals_ui",
}

REQUIRED_TABLE_COLUMNS = {
    "exchangelog_raw": {"logid", "logdate", "createdate", "msgid", "logstate", "logtext", "msgtext", "loaded_at"},
    "egisz_messages_raw": {"egmid", "jid", "kind", "created_at", "msgid", "reply_to", "document_id", "msgtext", "loaded_at"},
    "dim_organizations": {"jid", "name", "inn", "address", "updated_at"},
    "dim_licenses": {"id", "service_type", "jid", "mo_uid", "mo_domen", "bdate", "fdate", "kind", "modifydate"},
    "dim_semd_types": {
        "code",
        "type_code",
        "name",
        "level",
        "format_code",
        "start_date",
        "end_date",
        "implementation_guide",
        "git_link",
        "oid",
        "version",
        "source_url",
        "updated_at",
    },
    "egisz_error_interpretation_rules": {
        "rule_code",
        "priority",
        "match_code",
        "match_pattern",
        "interpretation",
        "is_active",
        "updated_at",
    },
    "fact_egisz_transactions": {
        "exchangelog_log_id",
        "log_date",
        "message_id",
        "relates_to_id",
        "local_uid_semd",
        "emdr_id",
        "doc_number",
        "org_oid",
        "status",
        "error_message",
        "callback_url",
        "egmid",
        "jid",
        "semd_code",
        "semd_name",
        "error_code",
        "errors_json",
        "creation_date",
        "processed_at",
    },
}

RAW_LOG_COLUMNS = ("logid", "logdate", "createdate", "msgid", "logstate", "logtext", "msgtext")
RAW_MESSAGE_COLUMNS = ("egmid", "jid", "kind", "created_at", "msgid", "reply_to", "document_id", "msgtext")
BOOTSTRAP_LOCK_TIMEOUT = "30s"
BOOTSTRAP_STATEMENT_TIMEOUT = "10min"
DIRECTORY_SYNC_LOCK_TIMEOUT = "15s"
DIRECTORY_SYNC_STATEMENT_TIMEOUT = "5min"
DIRECTORY_SYNC_PAGE_SIZE = 1000


def normalize_message_id(value: Any) -> Any:
    """Normalize EGISZ UUID wrappers while preserving empty/null values."""
    if value is None:
        return None
    text = str(value).strip()
    if text.startswith("<") and text.endswith(">"):
        text = text[1:-1].strip()
    if text.lower().startswith("urn:uuid:"):
        text = text[len("urn:uuid:") :]
    return text or None


def connect_pg(conn_params: Any) -> psycopg2.extensions.connection:
    if isinstance(conn_params, str):
        return psycopg2.connect(conn_params)
    return psycopg2.connect(
        host=conn_params.host,
        port=conn_params.port,
        user=conn_params.login,
        password=conn_params.password,
        database=conn_params.schema,
    )


def _read_bootstrap_sql() -> str:
    return resources.files("egisz_elt.sql").joinpath("001_dwh_bootstrap.sql").read_text(encoding="utf-8")


def list_missing_dwh_objects(con: psycopg2.extensions.connection) -> set[str]:
    """Return missing or incompatible DWH metadata for the external warehouse."""
    with con.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'

            UNION

            SELECT routine_name
            FROM information_schema.routines
            WHERE routine_schema = 'public'
              AND routine_type = 'FUNCTION'
            """
        )
        existing = {str(row[0]) for row in cur.fetchall()}
        issues = {f"missing:{name}" for name in REQUIRED_DWH_OBJECTS - existing}

        for table_name, required_columns in REQUIRED_TABLE_COLUMNS.items():
            if table_name not in existing:
                continue
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                """,
                (table_name,),
            )
            existing_columns = {str(row[0]) for row in cur.fetchall()}
            issues.update(f"missing_column:{table_name}.{name}" for name in required_columns - existing_columns)

        if "fact_egisz_transactions" in existing:
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_index i
                    JOIN pg_class t ON t.oid = i.indrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
                    WHERE n.nspname = 'public'
                      AND t.relname = 'fact_egisz_transactions'
                      AND i.indisprimary
                      AND a.attname = 'exchangelog_log_id'
                )
                """
            )
            if not bool(cur.fetchone()[0]):
                issues.add("incompatible_pk:fact_egisz_transactions")

    return issues


def ensure_tables(con: psycopg2.extensions.connection) -> None:
    """Initialize or heal DWH tables, SQL functions, and Metabase-facing views."""
    with con.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = %s", (BOOTSTRAP_LOCK_TIMEOUT,))
        cur.execute("SET LOCAL statement_timeout = %s", (BOOTSTRAP_STATEMENT_TIMEOUT,))
        cur.execute(_read_bootstrap_sql())
    con.commit()


def get_cursors(con: psycopg2.extensions.connection, pipeline: str) -> tuple[int, int]:
    """Read the last processed Firebird cursors for a pipeline."""
    with con.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(last_log_id), MAX(last_egmid)
            FROM elt_state
            WHERE pipeline IN (%s, 'main')
            """,
            (pipeline,),
        )
        row = cur.fetchone()
    if row is None:
        return (0, 0)
    return (int(row[0] or 0), int(row[1] or 0))


def load_raw_logs(con: psycopg2.extensions.connection, rows: list[dict[str, Any]] | list[tuple[Any, ...]]) -> None:
    """Load EXCHANGELOG rows into exchangelog_raw without transforming them in Python."""
    values: list[tuple[Any, ...]] = []
    for row in rows:
        if isinstance(row, dict):
            missing_columns = [column for column in RAW_LOG_COLUMNS if column not in row]
            if missing_columns:
                raise ValueError(f"Raw EXCHANGELOG row is missing required column(s): {', '.join(missing_columns)}")
            values.append(tuple(row[column] for column in RAW_LOG_COLUMNS))
        else:
            values.append(tuple(row))

    if not values:
        return

    with con.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO exchangelog_raw (logid, logdate, createdate, msgid, logstate, logtext, msgtext)
            VALUES %s
            ON CONFLICT (logid) DO UPDATE SET
                logdate = EXCLUDED.logdate,
                createdate = EXCLUDED.createdate,
                msgid = EXCLUDED.msgid,
                logstate = EXCLUDED.logstate,
                logtext = EXCLUDED.logtext,
                msgtext = EXCLUDED.msgtext,
                loaded_at = now()
            """,
            values,
        )
    con.commit()


def load_raw_messages(con: psycopg2.extensions.connection, rows: list[dict[str, Any]]) -> None:
    """Load EGISZ_MESSAGES rows into egisz_messages_raw."""
    values: list[tuple[Any, ...]] = []
    for row in rows:
        missing_columns = [column for column in RAW_MESSAGE_COLUMNS if column not in row]
        if missing_columns:
            raise ValueError(f"Raw EGISZ_MESSAGES row is missing required column(s): {', '.join(missing_columns)}")
        normalized = dict(row)
        normalized["msgid"] = normalize_message_id(normalized.get("msgid"))
        normalized["reply_to"] = normalize_message_id(normalized.get("reply_to"))
        values.append(tuple(normalized[column] for column in RAW_MESSAGE_COLUMNS))

    if not values:
        return

    with con.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO egisz_messages_raw (egmid, jid, kind, created_at, msgid, reply_to, document_id, msgtext)
            VALUES %s
            ON CONFLICT (egmid) DO UPDATE SET
                jid = EXCLUDED.jid,
                kind = EXCLUDED.kind,
                created_at = EXCLUDED.created_at,
                msgid = EXCLUDED.msgid,
                reply_to = EXCLUDED.reply_to,
                document_id = EXCLUDED.document_id,
                msgtext = EXCLUDED.msgtext,
                loaded_at = now()
            """,
            values,
        )
    con.commit()


def transform_raw_to_facts(
    con: psycopg2.extensions.connection,
    *,
    min_log_id: int,
    max_log_id: int,
    min_egmid: int = 0,
    max_egmid: int = 0,
) -> int:
    """Run the database-side ELT transform and refresh EGISZ materialized views if present."""
    with con.cursor() as cur:
        cur.execute(
            "SELECT public.egisz_transform_raw_to_facts(%s, %s, %s, %s)",
            (min_log_id, max_log_id, min_egmid, max_egmid),
        )
        transformed = int(cur.fetchone()[0] or 0)
        cur.execute(
            """
            SELECT schemaname, matviewname
            FROM pg_matviews
            WHERE schemaname = 'public'
              AND matviewname LIKE 'mv_egisz_%'
            """
        )
        for schema_name, view_name in cur.fetchall():
            cur.execute(f"REFRESH MATERIALIZED VIEW {schema_name}.{view_name}")
    con.commit()
    return transformed


def sync_directory(con: psycopg2.extensions.connection, table_name: str, rows: list[tuple[Any, ...]]) -> None:
    if table_name not in ALLOWED_SYNC_TABLES:
        raise ValueError(f"Unsupported directory table: {table_name}")
    columns = DIRECTORY_COLUMNS[table_name]
    column_sql = ", ".join(columns)
    pk_columns = DIRECTORY_PK_COLUMNS[table_name]
    conflict_sql = ", ".join(pk_columns)
    update_sql = ", ".join(
        f"{column_name} = EXCLUDED.{column_name}"
        for column_name in columns
        if column_name not in pk_columns
    )
    with con.cursor() as cur:
        cur.execute("SET LOCAL lock_timeout = %s", (DIRECTORY_SYNC_LOCK_TIMEOUT,))
        cur.execute("SET LOCAL statement_timeout = %s", (DIRECTORY_SYNC_STATEMENT_TIMEOUT,))
        if rows:
            execute_values(
                cur,
                f"""
                INSERT INTO {table_name} ({column_sql})
                VALUES %s
                ON CONFLICT ({conflict_sql}) DO UPDATE SET
                    {update_sql},
                    updated_at = now()
                """,
                rows,
                page_size=DIRECTORY_SYNC_PAGE_SIZE,
            )
    con.commit()


def update_cursors(
    con: psycopg2.extensions.connection,
    pipeline: str,
    log_id: int = 0,
    egmid: int = 0,
) -> None:
    with con.cursor() as cur:
        cur.execute(
            """
            INSERT INTO elt_state (pipeline, last_log_id, last_egmid)
            VALUES (%s, %s, %s)
            ON CONFLICT (pipeline) DO UPDATE SET
                last_log_id = GREATEST(elt_state.last_log_id, EXCLUDED.last_log_id),
                last_egmid = GREATEST(elt_state.last_egmid, EXCLUDED.last_egmid),
                updated_at = now();
            """,
            (pipeline, log_id, egmid),
        )
    con.commit()
