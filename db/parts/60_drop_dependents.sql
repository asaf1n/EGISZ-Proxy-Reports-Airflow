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
DROP VIEW IF EXISTS public.v_rpt_network_errors_detail_ui;
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

