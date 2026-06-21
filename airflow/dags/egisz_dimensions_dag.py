from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from egisz_elt.common import (
    DWH_CONN_ID,
    PROXY_CONN_ID,
    connect_fb,
    connect_pg,
    reconcile_enriched_ui,
)
from egisz_elt.dimensions import fetch_licenses, fetch_organizations, sync_directory

log = logging.getLogger(__name__)


def _dwh_connection():
    return connect_pg(BaseHook.get_connection(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(BaseHook.get_connection(PROXY_CONN_ID))


@dag(
    dag_id="egisz_dimensions_dag",
    schedule=Variable.get("egisz_dimensions_schedule", default_var="@hourly"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "dimensions"],
)
def egisz_dimensions_pipeline() -> None:
    @task
    def sync_dimensions() -> None:
        log.info("Starting dimension sync from proxy_egisz into dwh_egisz.")
        fb_conn = _proxy_connection()
        pg_conn = _dwh_connection()
        try:
            log.info("Fetching organizations directory from JPERSONS.")
            organization_rows = fetch_organizations(fb_conn)
            log.info("Fetched %s organization row(s); syncing dim_organizations.", len(organization_rows))
            sync_directory(pg_conn, "dim_organizations", organization_rows)

            log.info("Fetching licenses directory from EGISZ_LICENSES.")
            license_rows = fetch_licenses(fb_conn)
            log.info("Fetched %s license row(s); syncing dim_licenses.", len(license_rows))
            sync_directory(pg_conn, "dim_licenses", license_rows)
            log.info(
                "Synced %s organization row(s) and %s license row(s) into DWH dimensions.",
                len(organization_rows),
                len(license_rows),
            )
        finally:
            fb_conn.close()
            pg_conn.close()

    @task
    def maintain_enriched_ui() -> int:
        """Reconcile persistent enriched mart with live source view after dimension sync.

        Forward transform only refreshes rows touched in the current batch. A dimension change
        (clinic names, license-resolved JID) can alter display fields across the whole archive
        without bumping fact.updated_at — this task closes that gap. It runs on the dimension
        cadence because dimensions are what drive the drift; the full reconcile is O(archive).
        """
        pg_conn = _dwh_connection()
        try:
            refreshed = reconcile_enriched_ui(pg_conn)
            if refreshed <= 0:
                log.info("Enriched mart reconcile: no drift detected.")
                return 0

            pg_conn.set_session(autocommit=True)
            with pg_conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.v_egisz_documents_daily_ui")
                cur.execute("ANALYZE public.v_egisz_documents_enriched_ui")
                cur.execute("ANALYZE public.v_egisz_documents_daily_ui")
            log.info("Enriched mart reconcile: refreshed %s row(s) and daily rollup.", refreshed)
            return refreshed
        finally:
            pg_conn.close()

    sync_dimensions() >> maintain_enriched_ui()


egisz_dimensions_pipeline()
