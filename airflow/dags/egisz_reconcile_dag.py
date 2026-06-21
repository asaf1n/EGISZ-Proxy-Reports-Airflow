from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task
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
    reconcile_enriched_ui,
)
from egisz_elt.reconcile import (
    count_exchangelog_rows,
    fetch_exchangelog_by_logids,
    fetch_exchangelog_logids,
    get_all_raw_logids,
    transform_missing_windows,
)

log = logging.getLogger(__name__)


def _dwh_connection():
    return connect_pg(BaseHook.get_connection(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(BaseHook.get_connection(PROXY_CONN_ID))


@dag(
    dag_id="egisz_reconcile_dag",
    schedule=Variable.get("egisz_reconcile_schedule", default_var="@daily"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "reconcile"],
)
def egisz_reconcile_pipeline() -> None:
    @task
    def reconcile_proxy_raw() -> int:
        """Full source↔raw constancy check: every EXCHANGELOG LOGID against exchangelog_raw.

        The proxy journal materializes rows out of LOGID order — async СЭМД callbacks and
        gateway backfills appear *below* an already-advanced watermark, where the forward cursor
        (`LOGID > last_logid`) never re-reads them. Reconcile set-diffs the **whole** proxy LOGID
        set against the **whole** raw set, loads+transforms whatever is missing, and **never moves
        the watermark** (GREATEST stays the only writer, in the extract DAG). Steady state finds
        nothing and is a no-op. See README.md §«Полная сверка константности источник↔raw».
        """
        max_logids = int(Variable.get("egisz_reconcile_max_logids", default_var=20000000))
        window_max_gap = int(Variable.get("egisz_reconcile_window_max_gap", default_var=500))

        pg_conn = _dwh_connection()
        try:
            last_logid = int(get_cursors(pg_conn, PIPELINE).get("last_logid", 0))
            if last_logid <= 0:
                log.info("Reconcile: watermark not advanced yet; nothing to reconcile.")
                return 0

            fb_conn = _proxy_connection()
            try:
                # Memory guard: the whole-journal set-diff materializes every LOGID in the worker.
                # Above the expected volume we hard-skip rather than risk OOM-killing the worker.
                # Extension point: range-batched diff would lift this cap (not introduced here).
                source_count = count_exchangelog_rows(fb_conn)
                if source_count > max_logids:
                    log.warning(
                        "Reconcile: source has %s LOGID(s) > guard %s; skipping full set-diff "
                        "to avoid worker OOM. Raise egisz_reconcile_max_logids or batch by range.",
                        source_count,
                        max_logids,
                    )
                    return 0

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

            pg_conn.set_session(autocommit=True)
            with pg_conn.cursor() as cur:
                cur.execute("ANALYZE public.exchangelog_raw")
            pg_conn.set_session(autocommit=False)

            reconciled = transform_missing_windows(pg_conn, missing, max_gap=window_max_gap)

            pg_conn.set_session(autocommit=True)
            with pg_conn.cursor() as cur:
                cur.execute("ANALYZE public.fact_egisz_documents")
            reconcile_enriched_ui(pg_conn)
            with pg_conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.v_egisz_documents_daily_ui")
            log.info("Reconcile: recovered %s late chain message(s) into document facts.", reconciled)
            return reconciled
        finally:
            pg_conn.close()

    reconcile_proxy_raw()


egisz_reconcile_pipeline()
