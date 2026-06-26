-- ============================================================================
-- 60_drop_dependents.sql — DROP dependent views/marts before re-creating them.
-- ============================================================================

DROP VIEW IF EXISTS public.rpt_health_by_clinic CASCADE;
DROP VIEW IF EXISTS public.rpt_health_signals CASCADE;
DROP VIEW IF EXISTS public.rpt_health_proxy_db CASCADE;
DROP VIEW IF EXISTS public.v_rpt_client_kpi_daily_ui CASCADE;
DROP VIEW IF EXISTS public.v_rpt_clinic_semd_slice_ui CASCADE;
DROP VIEW IF EXISTS public.v_rpt_connectivity_global_daily_ui CASCADE;
DROP VIEW IF EXISTS public.v_rpt_clinic_connectivity_daily_ui CASCADE;
DROP VIEW IF EXISTS public.v_rpt_client_documents_ui CASCADE;
DROP VIEW IF EXISTS public.rpt_network_errors CASCADE;
DROP VIEW IF EXISTS public.rpt_error_breakdown CASCADE;
DROP VIEW IF EXISTS public.v_rpt_error_interpretations_ui CASCADE;
DROP VIEW IF EXISTS public.v_rpt_semd_archive_ui CASCADE;
DROP VIEW IF EXISTS public.rpt_documents CASCADE;
DROP VIEW IF EXISTS public.rpt_documents_waiting CASCADE;
DROP VIEW IF EXISTS public.v_stg_channel_network_errors_by_document CASCADE;
DROP VIEW IF EXISTS public.v_stg_channel_errors_by_document CASCADE;
DROP TABLE IF EXISTS public.dim_exchangelog_refs CASCADE;
DROP VIEW IF EXISTS public.rpt_document_lineage CASCADE;
DROP TABLE IF EXISTS public.v_documents_enriched_ui CASCADE;
DROP VIEW IF EXISTS public.v_documents_enriched_ui CASCADE;
DROP VIEW IF EXISTS public.v_documents_enriched_src CASCADE;
