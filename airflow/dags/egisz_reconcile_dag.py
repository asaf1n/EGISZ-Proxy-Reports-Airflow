from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models import Variable

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
    run_analyze,
)
from egisz_elt.reconcile import (
    count_exchangelog_rows,
    fetch_exchangelog_by_logids,
    fetch_exchangelog_logids,
    get_all_raw_logids,
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
    schedule=Variable.get("reconcile_schedule", default_var="@daily"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "reconcile"],
)
def egisz_reconcile_pipeline() -> None:
    @task(pool=DWH_POOL)
    def reconcile_proxy_raw() -> int:
        """Full source↔raw constancy check: every EXCHANGELOG LOGID against exchangelog_raw."""
        max_logids = int(Variable.get("reconcile_max_logids", default_var=20000000))

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
                source_count = count_exchangelog_rows(fb_conn)
                if source_count > max_logids:
                    raise RuntimeError(
                        f"Reconcile aborted: source has {source_count} LOGID(s) > guard "
                        f"{max_logids}. Raise reconcile_max_logids or implement batched diff."
                    )

                source_logids = fetch_exchangelog_logids(fb_conn)
                raw_logids = get_all_raw_logids(pg_conn)
                missing = sorted(source_logids - raw_logids)
                if not missing:
                    log.info("Reconcile: all %s source LOGID(s) present in raw.", source_count)
                    return 0
                log.info(
                    "Reconcile: %s row(s) missing from raw across full range, span %s..%s.",
                    len(missing),
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
