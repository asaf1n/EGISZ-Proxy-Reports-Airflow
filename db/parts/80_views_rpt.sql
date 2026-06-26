-- ============================================================================
-- 80_views_rpt.sql — reporting views for Metabase (rpt_*)
-- Loaded by db/dwh_init.sql via \i db/parts/80_views_rpt.sql.
-- ============================================================================

CREATE OR REPLACE VIEW public.rpt_documents AS
SELECT
    d.dwh_id,
    COALESCE(d.last_callback_at, d.sent_at, d.document_created_at) AS processed_at,
    (
        COALESCE(d.last_callback_at, d.sent_at, d.document_created_at)
        AT TIME ZONE 'Europe/Moscow'
    )::date AS processed_day,
    d.status,
    ds.label AS status_label,
    ds.sort_order AS status_sort,
    NULLIF(
        btrim(
            split_part(
                COALESCE(NULLIF(btrim(d.error_type), ''), 'Неизвестная ошибка'),
                ' · ',
                1
            )
        ),
        ''
    ) AS error_type,
    d.error_summary,
    d.error_text,
    public.normalize_semd_code(d.semd_code) AS semd_code,
    st.name AS semd_name,
    CASE
        WHEN st.code IS NOT NULL AND st.name IS NOT NULL
            THEN st.code || ' · ' || st.name
        WHEN st.code IS NOT NULL
            THEN st.code || ' · Наименование СЭМД отсутствует в справочнике СЭМД'
        ELSE NULL
    END AS semd_code_name,
    public.clean_text_value(d.local_uid) AS semd_local_uid,
    d.document_created_at AS semd_created_at,
    d.emdr_id AS semd_emdr_id,
    d.jid AS clinic_jid,
    o.name AS clinic_name,
    COALESCE(NULLIF(BTRIM(d.jid::text), ''), '—')
        || ' · ' ||
    COALESCE(NULLIF(BTRIM(o.name), ''), '—') AS clinic_label,
    o.inn AS clinic_inn,
    COALESCE(
        NULLIF(btrim(a.clinic_oid_license), ''),
        NULLIF(btrim(d.org_oid), '')
    ) AS clinic_oid,
    a.clinic_host,
    a.clinic_jid_mismatch,
    public.clean_text_value(d.relates_to_id) AS relates_to_id,
    d.callback_log_id::text AS logid,
    d.message_id,
    CASE
        WHEN d.status = 'success'
         AND d.document_created_at IS NOT NULL
         AND COALESCE(d.last_callback_at, d.sent_at, d.document_created_at) >= d.document_created_at
        THEN ROUND(
            EXTRACT(
                EPOCH FROM (
                    COALESCE(d.last_callback_at, d.sent_at, d.document_created_at)
                    - d.document_created_at
                )
            )::numeric,
            0
        )
        ELSE NULL::numeric
    END AS delivery_seconds,
    a.patient_name_masked,
    a.snils_masked,
    a.doctor_name,
    a.patient_hash,
    a.doctor_hash,
    d.registered_at,
    (
        COALESCE(d.first_sent_at, d.document_created_at)
        AT TIME ZONE 'Europe/Moscow'
    )::date AS arrival_day
FROM public.documents d
LEFT JOIN public.document_attributes a ON a.dwh_id = d.dwh_id
LEFT JOIN public.dim_document_status ds ON ds.code = d.status
LEFT JOIN public.dim_organizations o ON o.jid = d.jid
LEFT JOIN LATERAL (
    SELECT dst.*
    FROM public.dim_semd_types dst
    WHERE dst.oid = public.normalize_semd_code(d.semd_code)
    ORDER BY dst.start_date DESC NULLS LAST, dst.code DESC
    LIMIT 1
) st ON TRUE
WHERE NULLIF(btrim(d.dwh_id), '') IS NOT NULL;

COMMENT ON VIEW public.rpt_documents IS
'Единая документная витрина: одна строка на dwh_id.';

CREATE OR REPLACE VIEW public.rpt_documents_waiting AS
SELECT
    d.sent_at,
    EXTRACT(EPOCH FROM (now() - d.sent_at)) / 3600.0 AS waiting_hours,
    ROUND(EXTRACT(EPOCH FROM (now() - d.sent_at)) / 86400.0, 1) AS waiting_days,
    CASE
        WHEN d.sent_at IS NULL THEN 'дата неизвестна'
        WHEN now() - d.sent_at > INTERVAL '30 days' THEN '>30 дней'
        WHEN now() - d.sent_at > INTERVAL '7 days' THEN '>7 дней'
        WHEN now() - d.sent_at > INTERVAL '3 days' THEN '>3 дней'
        ELSE 'до 3 дней'
    END AS wait_segment,
    r.semd_local_uid,
    r.semd_code,
    r.semd_name,
    r.semd_code_name,
    r.clinic_jid,
    r.clinic_name,
    r.clinic_label,
    r.relates_to_id,
    r.message_id,
    r.clinic_host
FROM public.documents d
INNER JOIN public.rpt_documents r ON r.dwh_id = d.dwh_id
WHERE d.status = 'waiting';

CREATE OR REPLACE VIEW public.rpt_network_errors AS
SELECT
    r.processed_at,
    r.logid,
    r.message_id,
    r.dwh_id,
    r.semd_local_uid,
    r.relates_to_id,
    r.clinic_host,
    r.clinic_jid,
    r.clinic_name,
    r.clinic_label,
    r.semd_code,
    r.semd_name,
    r.semd_code_name,
    public.network_error_type(r.error_text) AS network_error_type,
    r.error_text,
    r.error_type,
    r.semd_emdr_id
FROM public.rpt_documents r
WHERE r.status = 'network_error';

COMMENT ON VIEW public.rpt_network_errors IS
'Ошибки связи proxy_egisz: document-grain (status=network_error).';

CREATE OR REPLACE VIEW public.rpt_error_breakdown AS
WITH remd_errors AS (
    SELECT
        r.processed_at,
        r.processed_day,
        r.dwh_id,
        r.clinic_jid,
        r.clinic_name,
        r.clinic_label,
        r.semd_code,
        r.semd_code_name,
        trim(err_item) AS error_type
    FROM public.documents doc
    INNER JOIN public.rpt_documents r ON r.dwh_id = doc.dwh_id
    CROSS JOIN LATERAL regexp_split_to_table(
        COALESCE(NULLIF(btrim(doc.error_type), ''), 'Неизвестная ошибка'),
        ' [·-] '
    ) AS err_item
    WHERE r.status IN ('async_error', 'network_error')
      AND doc.error_type IS NOT NULL
      AND doc.dwh_id IS NOT NULL
      AND btrim(doc.error_type) <> ''
      AND trim(err_item) <> ''
)
SELECT
    processed_at,
    processed_day,
    dwh_id,
    clinic_jid,
    clinic_name,
    clinic_label,
    semd_code,
    semd_code_name,
    error_type,
    public.error_category(error_type) AS error_category
FROM remd_errors;

COMMENT ON VIEW public.rpt_error_breakdown IS
'Разбивка ошибок: один ряд = один атомарный вид ошибки на документ (split documents.error_type по '' · '' и '' - '').';

CREATE OR REPLACE VIEW public.rpt_document_lineage AS
SELECT
    d.dwh_id,
    d.jid AS clinic_jid,
    o.name AS clinic_name,
    a.clinic_oid_xml,
    a.clinic_oid_jpersons,
    a.clinic_oid_license,
    a.clinic_host,
    a.clinic_jid_resolve_method,
    a.message_endpoint,
    a.clinic_jid_mismatch,
    d.org_oid AS document_org_oid,
    d.jid_resolve_method AS document_jid_resolve_method
FROM public.documents d
LEFT JOIN public.document_attributes a ON a.dwh_id = d.dwh_id
LEFT JOIN public.dim_organizations o ON o.jid = d.jid
WHERE d.dwh_id IS NOT NULL;

COMMENT ON VIEW public.rpt_document_lineage IS
'Lineage документа: атомы идентификаторов клиники из XML, лицензий и журнала.';
