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
    reconcile_document_attributes_ui,
    refresh_error_breakdown,
    run_analyze,
)
from egisz_elt.dimensions import fetch_licenses, fetch_organizations, sync_directories

log = logging.getLogger(__name__)

DWH_POOL = "dwh_postgres"


def _dwh_connection():
    return connect_pg(BaseHook.get_connection(DWH_CONN_ID))


def _proxy_connection():
    return connect_fb(BaseHook.get_connection(PROXY_CONN_ID))


@dag(
    dag_id="egisz_dimensions_dag",
    schedule=Variable.get("dimensions_schedule", default_var="@hourly"),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["egisz", "elt", "dwh", "dimensions"],
)
def egisz_dimensions_pipeline() -> None:
    @task(pool=DWH_POOL)
    def sync_dimensions() -> dict[str, int]:
        """Sync proxy directories into DWH; refresh enriched mart only when rows changed.

        ``reconcile_document_attributes_ui()`` scans the archive for display drift (clinic names,
        license-resolved JID). It is skipped when UPSERT touched zero rows.
        """
        fb_conn = _proxy_connection()
        pg_conn = _dwh_connection()
        try:
            organization_rows = fetch_organizations(fb_conn)
            license_rows = fetch_licenses(fb_conn)
            log.info(
                "Fetched %s organization and %s license row(s) from proxy.",
                len(organization_rows),
                len(license_rows),
            )

            changed = sync_directories(pg_conn, organization_rows, license_rows)
            refreshed = 0
            if changed <= 0:
                log.info("Dimension directories unchanged; skipping enriched mart reconcile.")
            else:
                log.info("%s dimension row(s) changed; reconciling enriched mart.", changed)
                refreshed = reconcile_document_attributes_ui(pg_conn)
                if refreshed > 0:
                    run_analyze(pg_conn, "ANALYZE public.document_attributes")
                    # Имена клиник в matview разбивки ошибок могли измениться.
                    refresh_error_breakdown(pg_conn)
                    log.info("Enriched mart reconcile refreshed %s row(s).", refreshed)
                else:
                    log.info("Enriched mart reconcile: no drift detected.")

            return {
                "organizations": len(organization_rows),
                "licenses": len(license_rows),
                "changed": changed,
                "refreshed": refreshed,
            }
        finally:
            fb_conn.close()
            pg_conn.close()

    sync_dimensions()


egisz_dimensions_pipeline()
