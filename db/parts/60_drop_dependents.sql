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
-- rpt_error_breakdown стал MATERIALIZED VIEW. DROP VIEW IF EXISTS / DROP MATERIALIZED VIEW
-- IF EXISTS подавляют только отсутствие объекта, но НЕ несовпадение типа (VIEW vs MATVIEW),
-- поэтому дропаем по фактическому relkind — идемпотентно при переходе и пересборках.
DO $$
DECLARE
    kind "char";
BEGIN
    SELECT c.relkind INTO kind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relname = 'rpt_error_breakdown';

    IF kind = 'm' THEN
        DROP MATERIALIZED VIEW public.rpt_error_breakdown CASCADE;
    ELSIF kind IS NOT NULL THEN
        DROP VIEW public.rpt_error_breakdown CASCADE;
    END IF;
END $$;
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
