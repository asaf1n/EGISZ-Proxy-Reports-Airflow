-- ============================================================================
-- 75_views_stg.sql — v_stg_channel_errors_by_document (mat-view) + network alias view
-- Source: db/dwh_init.sql.
-- Loaded by db/dwh_init.sql via \i db/parts/75_views_stg.sql.
-- ============================================================================

CREATE MATERIALIZED VIEW public.v_stg_channel_errors_by_document AS
SELECT
    id,
    created_at,
    error_code,
    message,
    error_top_type,
    error_global_subcategory,
    error_group_label_ru,
    exchangelog_log_id,
    journal_msgid,
    egisz_messages_egmid,
    relates_to_hint,
    local_uid_hint,
    emdr_id_hint,
    document_group_key,
    relates_to_id
FROM public.fact_egisz_channel_errors
WITH NO DATA;

CREATE UNIQUE INDEX ON public.v_stg_channel_errors_by_document (id);
CREATE INDEX ON public.v_stg_channel_errors_by_document (error_top_type);
CREATE INDEX ON public.v_stg_channel_errors_by_document (document_group_key);
CREATE INDEX ON public.v_stg_channel_errors_by_document (journal_msgid);
CREATE INDEX ON public.v_stg_channel_errors_by_document (created_at);

CREATE OR REPLACE VIEW public.v_stg_channel_network_errors_by_document AS
SELECT *
FROM public.v_stg_channel_errors_by_document
WHERE error_top_type = 'network';
