-- ============================================================================
-- 60_drop_dependents.sql — DROP dependent views and legacy columns before re-creating views
-- Source: db/dwh_init.sql, lines [1254..1276).
-- Loaded by db/dwh_init.sql via \i db/parts/60_drop_dependents.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

DROP VIEW IF EXISTS public.v_health_by_clinic_ui;
DROP VIEW IF EXISTS public.v_health_signals_ui;
DROP VIEW IF EXISTS public.v_health_proxy_db_ui;
DROP VIEW IF EXISTS public.v_rpt_connectivity_global_daily_ui;
DROP VIEW IF EXISTS public.v_rpt_clinic_connectivity_daily_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_nrr_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_churn_risk_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_cohort_retention_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_semd_error_types_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_compliance_score_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_semd_deadline_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_semd_transfers_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_problem_clients_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_sla_compliance_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_mttr_incidents_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_ltv_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_cac_payback_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_gross_margin_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_arpu_segments_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_mrr_trend_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_client_month_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_cost_breakdown_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_financial_summary_ui;
DROP VIEW IF EXISTS public.v_rpt_service_audit_jid_month_ui;
DROP VIEW IF EXISTS public.v_rpt_client_documents_ui;
DROP VIEW IF EXISTS public.v_rpt_network_errors_detail_ui;
DROP VIEW IF EXISTS public.v_rpt_error_category_breakdown_ui;
DROP VIEW IF EXISTS public.v_stg_channel_network_errors_by_document;
DO $$ BEGIN DROP VIEW IF EXISTS public.v_stg_channel_errors_by_document CASCADE; EXCEPTION WHEN wrong_object_type THEN NULL; END $$;
DROP MATERIALIZED VIEW IF EXISTS public.v_stg_channel_errors_by_document;
DROP VIEW IF EXISTS public.v_rpt_error_interpretations_ui;
DROP VIEW IF EXISTS public.v_rpt_semd_archive_ui;
DO $$ BEGIN DROP VIEW IF EXISTS public.v_rpt_documents_no_response_ui CASCADE; EXCEPTION WHEN wrong_object_type THEN NULL; END $$;
DROP MATERIALIZED VIEW IF EXISTS public.v_rpt_documents_no_response_ui;  -- in case it was previously created as MV
DROP VIEW IF EXISTS public.v_egisz_transactions_full;
DO $$ BEGIN DROP VIEW IF EXISTS public.v_egisz_transactions_enriched_ui CASCADE; EXCEPTION WHEN wrong_object_type THEN NULL; END $$;
DROP MATERIALIZED VIEW IF EXISTS public.v_egisz_transactions_enriched_ui;

-- Drop legacy columns after dependent views are gone.
ALTER TABLE egisz_messages_raw DROP COLUMN IF EXISTS jid;
ALTER TABLE egisz_messages_raw DROP COLUMN IF EXISTS kind;
ALTER TABLE egisz_messages_raw DROP COLUMN IF EXISTS msgtext;

-- Drop legacy "service audit" placeholder tables that were never wired to any
-- ELT source. Dashboards/views referencing them have been removed; CASCADE
-- сметает оставшиеся зависимости в один шаг (это идемпотентно).
DROP TABLE IF EXISTS public.client_costs_monthly CASCADE;
DROP TABLE IF EXISTS public.churn_events        CASCADE;
DROP TABLE IF EXISTS public.sed_transfers       CASCADE;
DROP TABLE IF EXISTS public.sla_metrics         CASCADE;
DROP TABLE IF EXISTS public.tickets             CASCADE;
DROP TABLE IF EXISTS public.billing             CASCADE;
DROP TABLE IF EXISTS public.subscriptions       CASCADE;
DROP TABLE IF EXISTS public.clients             CASCADE;
