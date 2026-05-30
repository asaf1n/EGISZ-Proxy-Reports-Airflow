-- ============================================================================
-- 60_drop_dependents.sql — DROP dependent views/marts before re-creating them.
-- Объекты с изменяемой сигнатурой колонок нельзя пересоздать через
-- CREATE OR REPLACE, поэтому снимаем их здесь в порядке leaf→base, после чего
-- 70..90 пересобирают их заново. См. AGENTS.md §4 (идемпотентный DDL-контракт).
-- ============================================================================

DROP VIEW IF EXISTS public.v_health_by_clinic_ui;
DROP VIEW IF EXISTS public.v_health_signals_ui;
DROP VIEW IF EXISTS public.v_health_proxy_db_ui;
DROP VIEW IF EXISTS public.v_rpt_connectivity_global_daily_ui;
DROP VIEW IF EXISTS public.v_rpt_clinic_connectivity_daily_ui;
DROP VIEW IF EXISTS public.v_rpt_client_documents_ui;
DROP VIEW IF EXISTS public.v_rpt_network_errors_detail_ui;
DROP VIEW IF EXISTS public.v_rpt_error_category_breakdown_ui;
DROP VIEW IF EXISTS public.v_rpt_error_interpretations_ui;
DROP VIEW IF EXISTS public.v_rpt_semd_archive_ui;
DROP VIEW IF EXISTS public.v_rpt_documents_ui;
DROP VIEW IF EXISTS public.v_rpt_documents_no_response_ui;
DROP VIEW IF EXISTS public.v_stg_channel_network_errors_by_document;
DROP VIEW IF EXISTS public.v_stg_channel_errors_by_document;
DROP MATERIALIZED VIEW IF EXISTS public.v_egisz_documents_daily_ui;
DROP TABLE IF EXISTS public.v_egisz_documents_enriched_ui CASCADE;
DROP VIEW IF EXISTS public.v_egisz_documents_enriched_src CASCADE;

-- Снятый staging журнала сообщений: СЭМД-сообщения разбираются напрямую из exchangelog_raw.
DROP TABLE IF EXISTS public.stg_egisz_messages CASCADE;
DROP TABLE IF EXISTS public.dim_egisz_message_refs CASCADE;
DROP TABLE IF EXISTS public.egisz_messages_raw CASCADE;
