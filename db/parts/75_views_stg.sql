-- ============================================================================
-- 75_views_stg.sql — v_stg_channel_errors_by_document (mat-view) + network alias view
-- Source: db/dwh_init.sql, lines [1423..1506).
-- Loaded by db/dwh_init.sql via \i db/parts/75_views_stg.sql.
-- See AGENTS.md §4 for the contract: idempotent DDL (CREATE ... IF NOT EXISTS,
-- CREATE OR REPLACE, ALTER ... IF EXISTS).
-- ============================================================================

CREATE MATERIALIZED VIEW public.v_stg_channel_errors_by_document AS
SELECT
    r.logid AS id,
    COALESCE(r.createdate, r.loaded_at) AS created_at,
    CASE WHEN r.logstate = 3 THEN 'INTEGRATION_LOGSTATE_3' ELSE 'PARSE_ERROR' END AS error_code,
    COALESCE(NULLIF(r.logtext, ''), NULLIF(r.msgtext, ''), '(без текста)') AS message,
    CASE WHEN r.logstate = 3 THEN 'network' ELSE 'async_response' END AS error_top_type,
    CASE WHEN r.logstate = 3 THEN 'Сетевая ошибка' ELSE 'Неизвестная ошибка' END AS error_global_subcategory,
    CASE WHEN r.logstate = 3 THEN 'Ошибка связи' ELSE 'Неизвестная ошибка' END AS error_group_label_ru,
    r.logid AS exchangelog_log_id,
    r.msgid AS journal_msgid,
    m.egmid AS egisz_messages_egmid,
    COALESCE(
        x.relates_to_message_msgtext,
        x.relates_to_msgtext,
        x.relates_to_message_logtext
    ) AS relates_to_hint,
    COALESCE(
        x.local_uid_msgtext,
        x.document_id_msgtext,
        m.document_id
    ) AS local_uid_hint,
    x.emdr_id_msgtext AS emdr_id_hint,
    COALESCE(
        x.local_uid_msgtext,
        x.document_id_msgtext,
        x.emdr_id_msgtext,
        x.relates_to_message_msgtext,
        x.relates_to_msgtext,
        m.document_id,
        r.msgid,
        r.logid::text
    ) AS document_group_key,
    COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext) AS relates_to_id
FROM exchangelog_raw r
LEFT JOIN LATERAL (
    SELECT
        public.egisz_xml_text(r.msgtext, 'relatesToMessage') AS relates_to_message_msgtext,
        public.egisz_xml_text(r.msgtext, 'relatesTo') AS relates_to_msgtext,
        public.egisz_xml_text(r.logtext, 'relatesToMessage') AS relates_to_message_logtext,
        public.egisz_xml_text(r.msgtext, 'localUid') AS local_uid_msgtext,
        public.egisz_xml_text(r.msgtext, 'DOCUMENTID') AS document_id_msgtext,
        public.egisz_xml_text(r.msgtext, 'emdrId') AS emdr_id_msgtext
) x ON TRUE
LEFT JOIN LATERAL (
    SELECT em.*
    FROM egisz_messages_raw em
    WHERE lower(NULLIF(btrim(em.document_id), '')) IN (
            lower(NULLIF(btrim(x.local_uid_msgtext), '')),
            lower(NULLIF(btrim(x.document_id_msgtext), '')),
            lower(NULLIF(btrim(x.emdr_id_msgtext), ''))
          )
       OR public.egisz_normalize_message_id(em.msgid) = public.egisz_normalize_message_id(COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext))
       OR public.egisz_normalize_message_id(em.msgid) = public.egisz_normalize_message_id(r.msgid)
    ORDER BY
        CASE
            WHEN lower(NULLIF(btrim(em.document_id), '')) IN (
                lower(NULLIF(btrim(x.local_uid_msgtext), '')),
                lower(NULLIF(btrim(x.document_id_msgtext), '')),
                lower(NULLIF(btrim(x.emdr_id_msgtext), ''))
            ) THEN 0
            WHEN public.egisz_normalize_message_id(em.msgid) = public.egisz_normalize_message_id(COALESCE(x.relates_to_message_msgtext, x.relates_to_msgtext)) THEN 1
            ELSE 2
        END,
        em.egmid DESC
    LIMIT 1
) m ON TRUE
WHERE r.logstate = 3
   OR COALESCE(r.msgtext, '') ILIKE '%error%'
   OR COALESCE(r.logtext, '') ILIKE '%error%'
   OR COALESCE(r.logtext, '') ILIKE '%ошиб%'
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
