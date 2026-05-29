-- ============================================================================
-- 75_views_stg.sql — compatibility views over document-grain facts
-- Source: db/dwh_init.sql.
-- Loaded by db/dwh_init.sql via \i db/parts/75_views_stg.sql.
-- ============================================================================

-- Совместимость с дашбордами 02/04: сетевые ошибки живут в fact_egisz_documents
-- (status=network_error), без отдельной fact_egisz_channel_errors.
CREATE OR REPLACE VIEW public.v_stg_channel_errors_by_document AS
SELECT
    COALESCE(d.callback_log_id, d.source_logid) AS id,
    COALESCE(d.last_callback_at, d.sent_at, d.updated_at) AS created_at,
    'INTEGRATION_LOGSTATE_3'::text AS error_code,
    COALESCE(NULLIF(btrim(d.error_text), ''), NULLIF(btrim(d.error_summary), ''), 'Сетевая ошибка') AS message,
    'network'::text AS error_top_type,
    'Сетевая ошибка'::text AS error_global_subcategory,
    'Ошибка связи'::text AS error_group_label_ru,
    COALESCE(d.callback_log_id, d.source_logid) AS exchangelog_log_id,
    d.message_id AS journal_msgid,
    d.relates_to_id AS relates_to_hint,
    d.local_uid AS local_uid_hint,
    d.emdr_id AS emdr_id_hint,
    d.document_key,
    d.jid,
    d.relates_to_id
FROM public.fact_egisz_documents d
WHERE d.status = 'network_error';

CREATE OR REPLACE VIEW public.v_stg_channel_network_errors_by_document AS
SELECT *
FROM public.v_stg_channel_errors_by_document
WHERE error_top_type = 'network';
