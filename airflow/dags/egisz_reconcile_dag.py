from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook

from egisz_elt.airflow_vars import get_int, get_str
from egisz_elt.common import (
    DWH_CONN_ID,
    PIPELINE,
    PROXY_CONN_ID,
    connect_fb,
    connect_pg,
    get_cursors,
    load_raw_logs,
    pending_transform_tail,
    reconcile_document_attributes_ui,
    refresh_error_breakdown,
    refresh_weekly_reports,
    run_analyze,
)
from egisz_elt.reconcile import (
    fetch_exchangelog_by_logids,
    fetch_reconcile_window_sets,
    transform_missing_windows,
)

log = logging.getLogger(__name__)

DWH_POOL = "dwh_postgres"


def _dwh_connection():
    return connect_pg(BaseHook.get_connection(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(BaseHook.get_connection(PROXY_CONN_ID))


@dag(
    dag_id="egisz_reconcile_dag",
    schedule=get_str("reconcile_schedule"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "reconcile"],
)
def egisz_reconcile_pipeline() -> None:
    @task(pool=DWH_POOL)
    def reconcile_proxy_raw() -> int:
        """Set-diff source↔raw for EXCHANGELOG LOGIDs within the configured lookback window."""
        max_logids = get_int("reconcile_max_logids")
        lookback_days = get_int("reconcile_lookback_days")

        pg_conn = _dwh_connection()
        try:
            last_logid = int(get_cursors(pg_conn, PIPELINE).get("last_logid", 0))
            if last_logid <= 0:
                log.info("Reconcile: watermark not advanced yet; nothing to reconcile.")
                return 0

            pending_rows, pending_max = pending_transform_tail(pg_conn, last_logid)
            if pending_rows > 0:
                raise AirflowSkipException(
                    f"Reconcile deferred: extract backlog has {pending_rows} raw row(s) "
                    f"above watermark LOGID={last_logid} (tail={pending_max}). "
                    "Run extract transform first."
                )

            fb_conn = _proxy_connection()
            try:
                since, source_logids, raw_logids, source_count = fetch_reconcile_window_sets(
                    pg_conn,
                    fb_conn,
                    lookback_days=lookback_days,
                    max_logids=max_logids,
                )
                missing = sorted(source_logids - raw_logids)
                if not missing:
                    log.info(
                        "Reconcile: all %s source LOGID(s) present in raw (%s-day window).",
                        source_count,
                        lookback_days,
                    )
                    return 0
                log.info(
                    "Reconcile: %s row(s) missing from raw in %s-day window, span %s..%s.",
                    len(missing),
                    lookback_days,
                    missing[0],
                    missing[-1],
                )
                late_rows = fetch_exchangelog_by_logids(fb_conn, missing)
            finally:
                fb_conn.close()

            load_raw_logs(pg_conn, late_rows)

            run_analyze(pg_conn, "ANALYZE public.exchangelog_raw")

            reconciled = transform_missing_windows(pg_conn, missing)

            run_analyze(pg_conn, "ANALYZE public.documents")
            refreshed = reconcile_document_attributes_ui(pg_conn)
            if reconciled > 0:
                refresh_error_breakdown(pg_conn)
                refresh_weekly_reports(pg_conn)
            log.info(
                "Reconcile: recovered %s late chain message(s); "
                "refreshed %s enriched row(s).",
                reconciled,
                refreshed,
            )
            return reconciled
        finally:
            pg_conn.close()

    reconcile_proxy_raw()


egisz_reconcile_pipeline()
